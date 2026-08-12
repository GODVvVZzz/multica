package execenv

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

// openclawConfigFile is the per-task synthesized OpenClaw config the daemon
// points the openclaw CLI at via OPENCLAW_CONFIG_PATH. It sits in the env
// root (alongside workdir/, output/, logs/) so the GC reaper sweeps it with
// the rest of the task env.
const openclawConfigFile = "openclaw-config.json"

// openclawUserSnapshotFile is the sanitized include bridge the wrapper uses
// when the agent has a managed mcp_config. It composes the user's active
// config with openclawMcpResetFile, then restores only the resolved non-server
// MCP settings. The wrapper can therefore add managed `mcp.servers` without
// deep-merge-by-name leaking user servers.
const openclawUserSnapshotFile = "openclaw-user-snapshot.json"

// openclawMcpResetFile contributes `{"mcp":null}` between the user's config
// and the sanitized MCP settings. OpenClaw's include merge treats a primitive
// source as replacement, so this resets the entire user MCP object before the
// next merge restores the allowed siblings.
const openclawMcpResetFile = "openclaw-mcp-reset.json"

// openclawCLITimeout is the context deadline for OpenClaw discovery commands
// during task setup (`config file`, `config get agents.list`, and the registry
// fallback). OpenClaw loads its plugin graph before answering these commands;
// on a cold Windows host that can take 13-20s, so 30s leaves realistic startup
// headroom without consuming the preparation step's five-minute hard bound.
//
// This deadline is enforceable as of MUL-5467, which is not a given: it was
// documented here as a known gap for several releases. exec.CommandContext
// kills only the direct child, and cmd.Output() blocks in Wait() until the
// output pipes os/exec manages reach EOF — so an openclaw that leaves a
// descendant holding stdout (its `openclaw-config` helper; on Windows, the
// cmd.exe → node shim) parked the call for the descendant's lifetime.
// Measured on linux/dash: a shim whose backgrounded child slept 6s took 6.01s
// against a 150ms deadline. A cmd.WaitDelay backstop bounded the call but left
// the descendant running, which is why it was reverted from #6084.
//
// execOpenclawCLI now goes through agent.RunCollectQuiet, which owns the pipes
// (so Wait returns on the direct child's exit) and the process tree (a Unix
// process group or Windows Job Object), so descendants are reaped rather than
// orphaned. Production Prepare/Reuse also has an outer isolation boundary
// around the complete execenv helper before directory ownership is returned.
const openclawCLITimeout = 30 * time.Second

// openclawResolvedMcpTimeout remains deliberately shorter. The MCP query runs
// only after discovery has warmed the CLI and asks for one bounded config
// subtree; a slow or stuck read must fail closed without spending another full
// discovery budget on the task's critical path.
const openclawResolvedMcpTimeout = 5 * time.Second

// OpenclawConfigPrep is the input to prepareOpenclawConfig. Only OpenclawBin
// is meaningful in production — Timeout is here for tests that need a tight
// deadline to assert error paths.
type OpenclawConfigPrep struct {
	// OpenclawBin is the openclaw CLI binary to invoke for config introspection.
	// Empty means resolve "openclaw" from PATH at exec time.
	OpenclawBin string
	// Timeout overrides the context deadline for every CLI invocation. It is
	// not an exact wall-clock cap because bounded collector cleanup follows the
	// deadline; see agent.RunCollectQuiet.
	// Zero selects the command-specific production defaults.
	Timeout time.Duration
	// McpConfig is the agent's saved `mcp_config` JSON (Claude-style
	// `{"mcpServers": {"<name>": {...}}}`). When non-null the wrapper pins
	// `mcp.servers` to the managed set so OpenClaw resolves MCP from the
	// daemon's authoritative list instead of the user's global `mcp.servers`.
	// Null / empty means inherit the user's global config — same three-state
	// semantics codex uses (`hasManagedCodexMcpConfig`).
	McpConfig json.RawMessage
	// Gateway pins a specific OpenClaw Gateway endpoint inside the per-task
	// wrapper. Only consulted when the agent is configured for gateway-mode
	// openclaw (see ExecOptions.OpenclawMode); zero means "inherit whatever
	// the user's global openclaw.json already configures under `gateway.*`"
	// — which is the right default when the user already has a working
	// gateway set up locally. See issue #3260.
	Gateway OpenclawGatewayPin
	// Logger records the config-discovery outcome. Optional; nil disables
	// logging. Discovery used to be entirely silent, which is why #6630 —
	// a wrapper written without `$include` — could only be diagnosed by
	// reading the generated file and reverse-engineering the daemon. Paths
	// and booleans are logged; config contents never are.
	Logger *slog.Logger
}

// OpenclawGatewayPin describes the Gateway endpoint a per-task openclaw
// wrapper should pin. Fields mirror OpenClaw's own `gateway.*` config shape
// (see ~/.openclaw/openclaw.json). All fields are optional; only non-zero
// fields are emitted into the wrapper so a partial pin (e.g. host+port
// only, token left to inherit from the user's config) does the right
// thing under OpenClaw's deep-merge $include semantics.
type OpenclawGatewayPin struct {
	Host  string `json:"host,omitempty"`
	Port  int    `json:"port,omitempty"`
	Token string `json:"token,omitempty"`
	TLS   bool   `json:"tls,omitempty"`
}

// IsZero reports whether every field is zero, i.e. there is nothing to pin.
func (p OpenclawGatewayPin) IsZero() bool {
	return p == OpenclawGatewayPin{}
}

// String masks the bearer token when the pin is rendered as a string —
// `%v` / `%+v` / direct `fmt.Stringer` use cases all go through here. The
// raw Token field still exists for the wrapper-config emitter that needs
// it; this is a belt against a future caller that logs a whole task-prep
// summary at a level a non-admin can see (issue #3260 CR).
func (p OpenclawGatewayPin) String() string {
	tok := ""
	if p.Token != "" {
		tok = "***"
	}
	return fmt.Sprintf("OpenclawGatewayPin{Host:%q Port:%d Token:%s TLS:%t}", p.Host, p.Port, tok, p.TLS)
}

// MarshalJSON masks the bearer token in any default JSON dump (debug
// endpoints, error envelopes, structured-log encoders). The wrapper config
// writer goes through buildGatewayOverride, and the private preparation-helper
// transport uses its own methodless wire view, so both retain the real token.
func (p OpenclawGatewayPin) MarshalJSON() ([]byte, error) {
	type alias struct {
		Host  string `json:"host,omitempty"`
		Port  int    `json:"port,omitempty"`
		Token string `json:"token,omitempty"`
		TLS   bool   `json:"tls,omitempty"`
	}
	masked := alias{Host: p.Host, Port: p.Port, TLS: p.TLS}
	if p.Token != "" {
		masked.Token = "***"
	}
	return json.Marshal(masked)
}

// OpenclawConfigResult is what prepareOpenclawConfig returns to its callers
// in execenv.go. ConfigPath is the wrapper file the daemon points
// OPENCLAW_CONFIG_PATH at. IncludeRoot is the directory the daemon must add
// to OPENCLAW_INCLUDE_ROOTS so OpenClaw will follow the $include link out
// of envRoot into the user's active config; it is empty when no $include
// is emitted (fresh install).
type OpenclawConfigResult struct {
	ConfigPath  string
	IncludeRoot string
}

// prepareOpenclawConfig writes a per-task OpenClaw config to envRoot and
// returns its absolute path along with the include root the daemon must
// grant. The daemon sets OPENCLAW_CONFIG_PATH to the path on the spawned
// openclaw subprocess so the CLI resolves its `agents.defaults.workspace`
// (and every `agents.list[].workspace`) to the task workdir — which is
// what makes OpenClaw's native skill scanner pick up the per-task skills
// we write under `<workDir>/skills/`.
//
// Strategy: delegate JSON5 / $include / env-substitution / state-dir
// resolution to the openclaw CLI itself rather than re-implementing the
// spec. We:
//
//  1. Run `openclaw config file` to find the user's active config path.
//     For OpenClaw releases whose `config` command rejects the `file`
//     subcommand shape, fall back to resolving OpenClaw's active-config
//     candidates, including legacy Clawdbot/Moltbot/Moldbot locations.
//  2. Run `openclaw config get agents.list --json` to enumerate every
//     registered agent ID with its resolved fields. The CLI parses JSON5,
//     follows $include, and substitutes ${VAR} for us.
//  3. For managed MCP, resolve only the `mcp` subtree and build an include
//     bridge that replaces user servers while preserving other MCP settings.
//  4. Write a wrapper config to envRoot/openclaw-config.json that
//     `$include`s the active path and overrides
//     `agents.defaults.workspace` plus every `agents.list[].workspace` to
//     workDir. The original config bytes are not mutated — they are loaded
//     by openclaw's own loader through the $include link, which preserves
//     comments, secrets, and nested $include chains verbatim.
//
// **Cross-directory $include confinement.** OpenClaw confines `$include`
// resolution to the directory containing the wrapper file unless the
// target's parent is listed in `OPENCLAW_INCLUDE_ROOTS`. Our wrapper lives
// in envRoot but $includes the user's active config (typically
// `~/.openclaw/openclaw.json`) — a cross-directory hop. We surface
// `filepath.Dir(activePath)` as IncludeRoot so the daemon can prepend it
// to whatever the user already has in OPENCLAW_INCLUDE_ROOTS; without
// this, OpenClaw refuses to follow the link and the wrapper boots with no
// user config. Fresh install emits no $include, so IncludeRoot is "".
//
// **Intentional task isolation.** The override of every per-agent workspace
// is deliberate. OpenClaw's resolution order is
// `agents.list[id].workspace → agents.defaults.workspace → ~/.openclaw/
// workspace`. Pinning only the default would let a per-agent workspace the
// user configured at host scope silently re-route the scanner back to the
// shared workspace, defeating the per-task skill discovery this whole flow
// exists for. The cost is that any per-agent SOUL.md / MEMORY.md / standing
// orders the user laid in `<host-agent-workspace>/` are NOT visible to the
// in-task openclaw run — task isolation wins over host carry-over. The
// user's on-disk config is untouched; this only affects the wrapper used
// for this single task.
//
// **Fail closed.** Missing openclaw binary, CLI errors, malformed CLI
// output, or any IO error during write surfaces as an error to the caller
// rather than degrading to a minimal config. An earlier version silently
// synthesized a minimal config on parse failure; that masked broken user
// configs by starting OpenClaw without the registered agents / model
// providers / API keys it expects, which led to tasks routing to the wrong
// agent or failing to authenticate. The only "synthesize minimal" case
// kept is a fresh install where the CLI reports a path but no file exists
// — there is no user data to lose in that case.
func prepareOpenclawConfig(envRoot, workDir string, opts OpenclawConfigPrep) (OpenclawConfigResult, error) {
	bin := opts.OpenclawBin
	if bin == "" {
		bin = "openclaw"
	}
	discoveryTimeout, resolvedMcpTimeout := openclawTimeouts(opts.Timeout)

	activePath, exists, err := openclawActiveConfigPath(bin, discoveryTimeout)
	if err != nil {
		return OpenclawConfigResult{}, fmt.Errorf("locate openclaw active config: %w", err)
	}
	if !exists && opts.Logger != nil {
		// Not an error — a genuine fresh install lands here legitimately.
		// But it is also where a failed discovery lands, and the two are
		// indistinguishable from the outside, so say so loudly: every task
		// prepared from this point runs without the user's model providers
		// and auth profiles.
		opts.Logger.Warn("execenv: openclaw active config not found; task wrapper will omit $include so the user's models and auth profiles will NOT be visible to this task",
			"reported_path", activePath)
	}

	var resolvedList []any
	var agentsFromRegistry bool
	if exists {
		resolvedList, agentsFromRegistry, err = openclawResolvedAgentsList(bin, discoveryTimeout)
		if err != nil {
			return OpenclawConfigResult{}, fmt.Errorf("read openclaw agents.list: %w", err)
		}
	}

	// Parse the agent's managed mcp_config (if any) before writing the wrapper
	// so a malformed value fails the prepare step rather than crashing the
	// openclaw subprocess later. Same fail-closed posture as Codex's
	// ensureCodexMcpConfig — silent fallback to the user's global mcp.servers
	// would be indistinguishable from "the managed set applied" and is exactly
	// the surprise the MCP Tab is supposed to remove.
	managedMcp, hasManagedMcp, err := openclawManagedMcpServers(opts.McpConfig)
	if err != nil {
		return OpenclawConfigResult{}, fmt.Errorf("render openclaw mcp_config: %w", err)
	}

	// **Strict replace for managed mcp_config.** When the agent has a managed
	// set, deep-merging the wrapper's `mcp.servers` against the user's active
	// config via `$include` would let user-only entries leak in (and an empty
	// managed set would not actually clear inherited servers). OpenClaw no
	// longer supports reading the resolved config root, but it does support
	// `config get mcp --json`. Build a three-stage include instead:
	//
	//  1. include the user's full config,
	//  2. merge `mcp: null` to replace its MCP object,
	//  3. restore the resolved non-server MCP settings.
	//
	// The final wrapper then adds the managed server set. OpenClaw's own loader
	// still resolves JSON5, nested includes, and env substitutions; user config
	// bytes and secrets are never copied into the task directory.
	snapshotPath := ""
	if hasManagedMcp && exists {
		resolvedMcp, ferr := openclawResolvedMcpConfig(bin, resolvedMcpTimeout)
		if ferr != nil {
			return OpenclawConfigResult{}, fmt.Errorf("read openclaw resolved mcp config: %w", ferr)
		}
		delete(resolvedMcp, "servers")

		resetPath := filepath.Join(envRoot, openclawMcpResetFile)
		if werr := os.WriteFile(resetPath, []byte("{\n  \"mcp\": null\n}\n"), 0o600); werr != nil {
			return OpenclawConfigResult{}, fmt.Errorf("write openclaw mcp reset: %w", werr)
		}
		snapshot := map[string]any{
			"$include": []any{activePath, resetPath},
		}
		if len(resolvedMcp) > 0 {
			snapshot["mcp"] = resolvedMcp
		}
		snapBytes, merr := json.MarshalIndent(snapshot, "", "  ")
		if merr != nil {
			return OpenclawConfigResult{}, fmt.Errorf("marshal openclaw user snapshot: %w", merr)
		}
		snapshotPath = filepath.Join(envRoot, openclawUserSnapshotFile)
		if werr := os.WriteFile(snapshotPath, snapBytes, 0o600); werr != nil {
			return OpenclawConfigResult{}, fmt.Errorf("write openclaw user snapshot: %w", werr)
		}
	}

	cfg := buildPerTaskOpenclawConfig(activePath, exists, snapshotPath, resolvedList, agentsFromRegistry, workDir, managedMcp, hasManagedMcp, opts.Gateway)

	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return OpenclawConfigResult{}, fmt.Errorf("marshal openclaw config: %w", err)
	}
	outPath := filepath.Join(envRoot, openclawConfigFile)
	// 0o600 — defense in depth. The wrapper itself carries no secrets (the
	// $include link is just a filesystem path), but the file lives next to
	// task scratch and we keep the same posture as ~/.openclaw/openclaw.json.
	if err := os.WriteFile(outPath, data, 0o600); err != nil {
		return OpenclawConfigResult{}, fmt.Errorf("write openclaw config: %w", err)
	}
	result := OpenclawConfigResult{ConfigPath: outPath}
	includeTarget := "none"
	if snapshotPath != "" {
		// The snapshot lives beside the wrapper but includes the live user
		// config, so grant that cross-directory hop to OpenClaw's nested
		// include resolver.
		result.IncludeRoot = filepath.Dir(activePath)
		includeTarget = "sanitized-snapshot"
	} else if exists {
		// Live user config is in its own directory; tell the daemon to grant
		// it so OpenClaw's include-confinement check passes.
		result.IncludeRoot = filepath.Dir(activePath)
		includeTarget = "user-config"
	}
	if opts.Logger != nil {
		opts.Logger.Info("execenv: prepared openclaw config",
			"active_config", activePath,
			"active_config_exists", exists,
			"include_target", includeTarget,
			"include_root", result.IncludeRoot,
			"agents_from_registry", agentsFromRegistry,
			"managed_mcp", hasManagedMcp)
	}
	return result, nil
}

func openclawTimeouts(override time.Duration) (discovery, resolvedMcp time.Duration) {
	if override > 0 {
		return override, override
	}
	return openclawCLITimeout, openclawResolvedMcpTimeout
}

// buildPerTaskOpenclawConfig assembles the wrapper map that goes on disk.
//
// Exists=true: emit a $include link to the user's active config plus the
// workspace overrides as siblings. OpenClaw deep-merges sibling object keys
// after includes, so agents.defaults.workspace lands correctly. The
// agents.list override is emitted as a full replacement carrying every
// field of every resolved entry (id, model, prompts, tools, …) verbatim
// with only `workspace` rewritten — this is robust regardless of whether
// the runtime merges the sibling array or replaces it, because either way
// the resulting list is shape-equivalent to the user's minus workspace.
//
// Exists=false: a fresh install with no on-disk config. Emit a minimal
// config containing only the workspace override. There is no user data to
// $include here, so this is not the silent-fallback case the reviewer
// flagged.
//
// snapshotPath, when non-empty, points at the sanitized include bridge in
// envRoot. It is the $include target whenever the agent has a managed
// mcp_config; the bridge still references the live user file for all non-MCP
// config but resets its MCP object before restoring allowed settings. When
// snapshotPath is empty the wrapper falls back to $include'ing the active path
// directly (no managed MCP means there is nothing to enforce strictness
// against).
//
// hasManagedMcp distinguishes "agent has a managed mcp_config (possibly an
// empty set)" from "agent inherits the user's global mcp.servers". When
// true we pin `mcp.servers` to managedMcp on the wrapper. Because the
// snapshot $include has already replaced the user's `mcp` block, the
// resulting view of `mcp.servers` is exactly the managed set — including
// `{}` for "admin saved no servers" (mirrors `hasManagedCodexMcpConfig`).
func buildPerTaskOpenclawConfig(activePath string, exists bool, snapshotPath string, resolvedList []any, agentsFromRegistry bool, workDir string, managedMcp map[string]any, hasManagedMcp bool, gateway OpenclawGatewayPin) map[string]any {
	agents := map[string]any{
		"defaults": map[string]any{"workspace": workDir},
	}
	// Only write per-agent overrides back to the wrapper when they came from
	// the config-schema `agents.list` path (pre-2026.6). A registry-sourced
	// list (OpenClaw 2026.6.x+) is NOT valid `agents.list[]` config — the
	// schema validator rejects it ("agents.list.0: Invalid input") and fails
	// closed before the agent runs. 2026.6.x has no in-config path for per-
	// agent workspace pinning, so `agents.defaults.workspace` (set above) is
	// the only knob, and it is sufficient: OpenClaw applies it to the agent it
	// selects from the registry (see upstream #3028, write-side half).
	if !agentsFromRegistry {
		if rewritten := rewriteAgentsListWorkspaces(resolvedList, workDir); rewritten != nil {
			agents["list"] = rewritten
		}
	}
	cfg := map[string]any{
		"agents": agents,
	}
	if hasManagedMcp {
		// Always emit `mcp.servers` (even when empty) so the wrapper's intent
		// — "admin manages this set" — is grep-able on disk and visible to
		// OpenClaw's loader. The snapshot $include has already dropped the
		// user's `mcp` block, so this becomes the only definition.
		servers := managedMcp
		if servers == nil {
			servers = map[string]any{}
		}
		cfg["mcp"] = map[string]any{"servers": servers}
	}
	// Gateway endpoint pin (issue #3260). Mirrors the user's openclaw.json
	// `gateway.*` shape so OpenClaw's deep-merge $include semantics produce
	// the right composed config: anything we set here wins over the user's
	// global, anything we omit inherits from the user's global. Only emit
	// fields the multica admin explicitly populated — zero strings/ints
	// would override the user's value with junk.
	if gw := buildGatewayOverride(gateway); gw != nil {
		cfg["gateway"] = gw
	}
	switch {
	case snapshotPath != "":
		// Sanitized snapshot path; strict-replace flow for managed mcp_config.
		// Array form so OpenClaw deep-merges the snapshot's content with our
		// sibling keys (agents overrides, mcp.servers) rather than letting the
		// include replace the whole wrapper.
		cfg["$include"] = []any{snapshotPath}
	case exists:
		cfg["$include"] = []any{activePath}
	}
	return cfg
}

// buildGatewayOverride renders the non-zero subset of a Gateway pin into the
// shape OpenClaw expects under `gateway.*` (see ~/.openclaw/openclaw.json:
// host, port, tls at the top level and an `auth: {mode, token}` sub-object).
// Returns nil when nothing is populated so the caller can skip emission.
func buildGatewayOverride(p OpenclawGatewayPin) map[string]any {
	if p.IsZero() {
		return nil
	}
	out := map[string]any{}
	if p.Host != "" {
		out["host"] = p.Host
	}
	if p.Port != 0 {
		out["port"] = p.Port
	}
	if p.TLS {
		out["tls"] = true
	}
	if p.Token != "" {
		out["auth"] = map[string]any{
			"mode":  "token",
			"token": p.Token,
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// rewriteAgentsListWorkspaces copies every entry of the resolved agents.list
// and pins its `workspace` field to workDir. Returns nil when the input is
// nil or empty so buildPerTaskOpenclawConfig can omit the key entirely
// (avoiding an empty `agents.list: []` that would replace whatever the
// include carries).
func rewriteAgentsListWorkspaces(list []any, workDir string) []any {
	if len(list) == 0 {
		return nil
	}
	out := make([]any, 0, len(list))
	for _, item := range list {
		entry, ok := item.(map[string]any)
		if !ok {
			// Shape we don't recognize — skip rather than guess. Worst case
			// the user loses native skill discovery on that one agent; we
			// still won't crash the wrapper.
			continue
		}
		copyEntry := make(map[string]any, len(entry)+1)
		for k, v := range entry {
			copyEntry[k] = v
		}
		copyEntry["workspace"] = workDir
		out = append(out, copyEntry)
	}
	if len(out) == 0 {
		return nil
	}
	return out
}

// openclawActiveConfigPath runs `openclaw config file` to discover the path
// the openclaw CLI considers active. Returns (absolutePath, exists, error).
//
// The CLI handles the full resolution chain — explicit config path, state
// directory, OPENCLAW_HOME / default home, legacy locations, migration, and `~`
// expansion — so we prefer it when the installed CLI supports the command.
//
// OpenClaw 2026.2.x briefly rejected `openclaw config file` with the generic
// "too many arguments for 'config'" error. For that command-shape failure only,
// fall back to the same active-config candidate shape so task prep can still
// continue without losing upgraded users' legacy config files.
//
// The reported path uses `~` shorthand for the user's home; we expand it
// so the $include reference we write is unambiguous absolute.
func openclawActiveConfigPath(bin string, timeout time.Duration) (string, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := openclawExec(ctx, bin, "config", "file")
	if err != nil {
		if isOpenclawConfigFileUnsupported(err) {
			path, exists, ferr := openclawFallbackActiveConfigPath()
			if ferr != nil {
				return "", false, fmt.Errorf("fallback after unsupported `openclaw config file` (%v): %w", err, ferr)
			}
			return path, exists, nil
		}
		return "", false, err
	}
	return openclawParseActiveConfigPath(out)
}

func openclawParseActiveConfigPath(out string) (string, bool, error) {
	// OpenClaw may print terminal UI borders (e.g., Doctor warnings) before
	// the actual path. The path is always the last non-empty line.
	path := openclawLastNonEmptyLine(out)
	if path == "" {
		return "", false, fmt.Errorf("`openclaw config file` returned empty output")
	}
	var err error
	path, err = expandOpenclawPath(path)
	if err != nil {
		return "", false, err
	}
	return openclawStatConfigPath(path)
}

// openclawLastNonEmptyLine returns the last non-empty, trimmed line of out.
// Shared by the parser and by openclawConfigPathComplete so the two cannot
// disagree about which line carries the answer.
func openclawLastNonEmptyLine(out string) string {
	lines := strings.Split(strings.TrimSpace(out), "\n")
	for i := len(lines) - 1; i >= 0; i-- {
		if trimmed := strings.TrimSpace(lines[i]); trimmed != "" {
			return trimmed
		}
	}
	return ""
}

// openclawOutputComplete returns the rule that decides whether the bytes
// captured so far are a finished answer for this openclaw subcommand, for
// agent.RunCollectQuiet's early return.
//
// A nil result means "no rule for this shape", which makes RunCollectQuiet wait
// for the process to exit — the conservative behaviour. Adding a subcommand
// without a rule therefore loses the hang tolerance rather than risking a
// truncated answer.
func openclawOutputComplete(args []string) agent.OutputComplete {
	for _, a := range args {
		if a == "--json" {
			return agent.JSONOutputComplete
		}
	}
	if len(args) >= 2 && args[0] == "config" && args[1] == "file" {
		return openclawConfigPathComplete
	}
	return nil
}

// openclawConfigPathComplete reports whether out already carries a usable
// `openclaw config file` answer, i.e. its last non-empty line looks like a path.
//
// Deliberately stricter than openclawParseActiveConfigPath, which resolves a
// relative line through filepath.Abs and would therefore accept a Doctor warning
// border as an answer. That leniency is fine once the command has finished, but
// as a completeness rule it would let the early return fire on the banner
// OpenClaw prints *before* the path (see MUL-3136) and return the banner as the
// config path.
func openclawConfigPathComplete(out []byte) bool {
	line := openclawLastNonEmptyLine(string(out))
	if line == "" {
		return false
	}
	if _, isTilde := openclawTildeRest(line); isTilde {
		return true
	}
	if _, isOpenclawHome := openclawHomeRest(line); isOpenclawHome {
		return filepath.IsAbs(strings.TrimSpace(os.Getenv("OPENCLAW_HOME")))
	}
	return filepath.IsAbs(line)
}

func openclawFallbackActiveConfigPath() (string, bool, error) {
	if explicitPath := strings.TrimSpace(os.Getenv("OPENCLAW_CONFIG_PATH")); explicitPath != "" {
		path, err := expandOpenclawPath(explicitPath)
		if err != nil {
			return "", false, err
		}
		return openclawStatConfigPath(path)
	}

	candidates, canonicalPath, err := openclawFallbackConfigCandidates()
	if err != nil {
		return "", false, err
	}
	for _, candidate := range candidates {
		path, err := expandOpenclawPath(candidate)
		if err != nil {
			return "", false, err
		}
		exists, err := openclawConfigPathExists(path)
		if err != nil {
			return "", false, err
		}
		if exists {
			return path, true, nil
		}
	}
	return openclawStatConfigPath(canonicalPath)
}

var openclawFallbackConfigFileNames = []string{
	"openclaw.json",
	"clawdbot.json",
	"moltbot.json",
	"moldbot.json",
}

var openclawFallbackConfigDirNames = []string{
	".openclaw",
	".clawdbot",
	".moltbot",
	".moldbot",
}

func openclawFallbackConfigCandidates() ([]string, string, error) {
	candidates := make([]string, 0, 1+2*len(openclawFallbackConfigFileNames)+len(openclawFallbackConfigDirNames)*len(openclawFallbackConfigFileNames))
	for _, env := range []string{"CLAWDBOT_CONFIG_PATH"} {
		if path := strings.TrimSpace(os.Getenv(env)); path != "" {
			candidates = append(candidates, path)
		}
	}

	for _, env := range []string{"OPENCLAW_STATE_DIR", "CLAWDBOT_STATE_DIR"} {
		if dir := strings.TrimSpace(os.Getenv(env)); dir != "" {
			candidates = appendOpenclawConfigFileCandidates(candidates, dir)
		}
	}

	home := strings.TrimSpace(os.Getenv("OPENCLAW_HOME"))
	var err error
	if home == "" {
		home, err = os.UserHomeDir()
		if err != nil {
			return nil, "", fmt.Errorf("resolve openclaw home: %w", err)
		}
	} else {
		home, err = expandOpenclawPath(home)
		if err != nil {
			return nil, "", fmt.Errorf("resolve OPENCLAW_HOME: %w", err)
		}
	}

	for _, dirName := range openclawFallbackConfigDirNames {
		candidates = appendOpenclawConfigFileCandidates(candidates, filepath.Join(home, dirName))
	}
	return candidates, filepath.Join(home, ".openclaw", "openclaw.json"), nil
}

func appendOpenclawConfigFileCandidates(candidates []string, dir string) []string {
	for _, name := range openclawFallbackConfigFileNames {
		candidates = append(candidates, filepath.Join(dir, name))
	}
	return candidates
}

// openclawTildeRest splits a `~`-shortened path into the part after the home
// prefix, reporting whether the path was tilde-shortened at all.
//
// The separator after `~` is whatever the CLI's host OS uses: OpenClaw
// prints `~/.openclaw/openclaw.json` on Unix and `~\.openclaw\openclaw.json`
// on Windows. Matching only the forward-slash form left the Windows tilde
// unexpanded, and since `~\...` is not absolute the path then got joined
// onto the daemon's working directory, producing a path that can never
// exist. The stat miss was indistinguishable from a fresh install, so the
// wrapper silently dropped the user's `$include` and every task booted
// without their model providers or auth profiles (issue #6630).
//
// Both separators are accepted regardless of runtime.GOOS, deliberately, and
// not via os.IsPathSeparator (which rejects `\` on Unix). The daemon and the
// CLI share a host, so only the host's own form arises in production — but
// keying on the character rather than the host OS lets the Windows shape be
// exercised from the normal Linux/macOS test job instead of only on a Windows
// runner, the same trade isOpenclawShimPath makes above.
func openclawTildeRest(path string) (string, bool) {
	if path == "~" {
		return "", true
	}
	if len(path) > 1 && path[0] == '~' && (path[1] == '/' || path[1] == '\\') {
		return path[2:], true
	}
	return "", false
}

// openclawHomeRest recognizes the symbolic path shape emitted by current
// OpenClaw releases when OPENCLAW_HOME is set. The CLI prints the variable name
// rather than its value (for example `$OPENCLAW_HOME\.openclaw\openclaw.json`),
// so treating that line as an ordinary relative path silently loses the user's
// config. Both shell-style forms and host separators are accepted.
func openclawHomeRest(path string) (string, bool) {
	for _, prefix := range []string{"$OPENCLAW_HOME", "${OPENCLAW_HOME}"} {
		if path == prefix {
			return "", true
		}
		if len(path) > len(prefix) && strings.HasPrefix(path, prefix) &&
			(path[len(prefix)] == '/' || path[len(prefix)] == '\\') {
			return path[len(prefix)+1:], true
		}
	}
	return "", false
}

func expandOpenclawPath(path string) (string, error) {
	if rest, isOpenclawHome := openclawHomeRest(path); isOpenclawHome {
		home := strings.TrimSpace(os.Getenv("OPENCLAW_HOME"))
		if home == "" {
			return "", fmt.Errorf("expand OPENCLAW_HOME in openclaw config path: environment variable is empty")
		}
		if rest == "" {
			path = home
		} else {
			path = filepath.Join(home, rest)
		}
	} else if rest, isTilde := openclawTildeRest(path); isTilde {
		home, herr := os.UserHomeDir()
		if herr != nil {
			return "", fmt.Errorf("expand `~` in openclaw config path: %w", herr)
		}
		if rest == "" {
			path = home
		} else {
			// The remainder still carries the CLI's separators. filepath.Join
			// normalizes them to the host's on the OS that matters here
			// (Windows accepts both), and the result is what we stat.
			path = filepath.Join(home, rest)
		}
	}
	if !filepath.IsAbs(path) {
		abs, err := filepath.Abs(path)
		if err != nil {
			return "", fmt.Errorf("resolve openclaw config path %q: %w", path, err)
		}
		path = abs
	}
	return path, nil
}

func openclawStatConfigPath(path string) (string, bool, error) {
	if !filepath.IsAbs(path) {
		return "", false, fmt.Errorf("openclaw reported non-absolute config path %q", path)
	}
	exists, err := openclawConfigPathExists(path)
	if err != nil {
		return "", false, err
	}
	return path, exists, nil
}

func openclawConfigPathExists(path string) (bool, error) {
	info, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("stat openclaw config %s: %w", path, err)
	}
	if info.IsDir() {
		return false, fmt.Errorf("openclaw config path %s is a directory, not a file", path)
	}
	return true, nil
}

func isOpenclawConfigFileUnsupported(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "too many arguments for 'config'") ||
		strings.Contains(msg, "expected 0 arguments but got 1") ||
		(strings.Contains(msg, "unknown") && strings.Contains(msg, "config") && strings.Contains(msg, "file"))
}

// openclawResolvedMcpConfig fetches the user's fully resolved `mcp` subtree.
// The CLI handles JSON5, nested includes, and env substitution. Reading only
// this path is intentional: OpenClaw 2026.7 requires a path for `config get`,
// so the former root `config get --json` invocation is no longer valid.
//
// Returns (nil, nil) when the key is absent or the CLI prints empty/null. Any
// other failure surfaces so managed MCP remains fail closed.
func openclawResolvedMcpConfig(bin string, timeout time.Duration) (map[string]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := openclawExec(ctx, bin, "config", "get", "mcp", "--json")
	if err != nil {
		if isOpenclawKeyMissing(err) {
			return nil, nil
		}
		return nil, err
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var mcp map[string]any
	if err := json.Unmarshal([]byte(trimmed), &mcp); err != nil {
		return nil, fmt.Errorf("parse `openclaw config get mcp --json` output: %w", err)
	}
	return mcp, nil
}

// openclawResolvedAgentsList fetches the user's resolved per-agent list and
// reports which schema produced it. The schema matters downstream: a config-
// sourced list is itself valid `agents.list[]` config and may be written back
// into the wrapper to pin per-agent workspaces, whereas a registry-sourced
// list MUST NOT be written back — see openclawRegistryAgentsList.
//
// Two schemas are supported:
//
//   - Pre-2026.6: agents live in the config under `agents.list`. We read them
//     via `openclaw config get agents.list --json`, which returns the post-
//     include, post-env-substitution array. fromRegistry=false.
//   - 2026.6.x and later: `agents.list` is no longer a config path — agents
//     live in a sqlite registry. `config get agents.list` exits non-zero with
//     "Config path not found: agents.list". We fall back to the
//     `openclaw agents list --json` *subcommand*. fromRegistry=true.
//
// Returns (nil, false, nil) when neither source yields any agents.
func openclawResolvedAgentsList(bin string, timeout time.Duration) ([]any, bool, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := openclawExec(ctx, bin, "config", "get", "agents.list", "--json")
	if err != nil {
		if isOpenclawKeyMissing(err) {
			// New schema: the config path is gone; the agents live in the
			// sqlite registry. Resolve them via the subcommand instead.
			list, rerr := openclawRegistryAgentsList(bin, timeout)
			return list, true, rerr
		}
		return nil, false, err
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" || trimmed == "null" {
		return nil, false, nil
	}
	var list []any
	if err := json.Unmarshal([]byte(trimmed), &list); err != nil {
		return nil, false, fmt.Errorf("parse `openclaw config get agents.list --json` output: %w", err)
	}
	return list, false, nil
}

// openclawRegistryAgentsList resolves agents from the sqlite-backed registry
// via `openclaw agents list --json` (OpenClaw 2026.6.x+).
//
// **The result is for read-side use only — it must never be written back into
// the wrapper as `agents.list`.** The registry entries carry CLI-only fields
// (identityName, identitySource, agentDir, bindings, isDefault) that are NOT
// part of the 2026.6.x config schema's `agents.list[]` shape; OpenClaw's
// validator rejects them ("agents.list.0: Invalid input") and fails closed
// before the agent runs. Worse, `agents.list` is no longer a valid config
// path at all in 2026.6.x — there is no in-config way to pin a per-agent
// workspace. The per-task workspace is instead pinned via
// `agents.defaults.workspace` alone, which the wrapper always sets and which
// OpenClaw applies to the agent it selects from the registry (verified on
// 2026.6.8). Callers gate the write-back on fromRegistry from
// openclawResolvedAgentsList.
//
// Returns nil (not an error) when the registry is empty or the subcommand
// reports no agents.
func openclawRegistryAgentsList(bin string, timeout time.Duration) ([]any, error) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	out, err := openclawExec(ctx, bin, "agents", "list", "--json")
	if err != nil {
		// Older OpenClaw builds may lack the subcommand entirely; treat an
		// unrecognized/missing subcommand the same as "no agents to pin"
		// rather than failing closed, since the defaults.workspace override
		// alone still gives correct per-task skill discovery for the common
		// single-agent case.
		if isOpenclawKeyMissing(err) || isOpenclawUnknownSubcommand(err) {
			return nil, nil
		}
		return nil, err
	}
	trimmed := strings.TrimSpace(out)
	if trimmed == "" || trimmed == "null" {
		return nil, nil
	}
	var list []any
	if err := json.Unmarshal([]byte(trimmed), &list); err != nil {
		return nil, fmt.Errorf("parse `openclaw agents list --json` output: %w", err)
	}
	return list, nil
}

// openclawExec is the runtime hook prepareOpenclawConfig uses to invoke the
// openclaw CLI. Production points at execOpenclawCLI; tests swap in a stub
// to avoid spawning a real binary. Production code never reassigns it.
var openclawExec = execOpenclawCLI

// execOpenclawCLI executes an openclaw subcommand and returns its stdout.
// The daemon's environment is inherited so OPENCLAW_CONFIG_PATH /
// OPENCLAW_STATE_DIR / OPENCLAW_HOME / OPENCLAW_INCLUDE_ROOTS pass through.
//
// stderr is captured separately and appended to error messages — failures
// here surface up to the daemon log, and a `openclaw doctor` hint there is
// more useful than just an exit code.
//
// When the CLI is a batch shim that exits non-zero and says nothing at all,
// openclawShimDiagnostic adds the interpreter-resolution detail that a bare
// `exit status 1` hides (MUL-5422 / #6061). Real stderr always wins — the
// diagnostic is a fallback for the silent case, not a replacement.
//
// Attribution order matters. openclawCLITimeout kills the child via
// CommandContext, and a killed process surfaces as *exec.ExitError
// ("signal: killed") — indistinguishable by type from a genuine exit 1. So the
// context is checked FIRST; otherwise a timeout gets reported as "node is not
// on PATH, install Node.js", sending the user to fix something that was never
// broken.
//
// In that branch the CONTEXT error is what gets %w-wrapped, not the process
// error, so errors.Is(err, context.DeadlineExceeded) holds for callers that
// check cancellation the standard way. The process error is still printed for
// diagnosis, just not as the wrapped cause.
//
// The invocation goes through agent.RunCollectQuiet rather than cmd.Output(),
// which closes the gap openclawCLITimeout documents (MUL-5467) and tolerates an
// openclaw that prints its answer and then declines to exit. Both are
// openclaw-side misbehaviour we cannot fix from here, and neither should stop a
// chat task from starting. See server/pkg/agent/run_collect.go.
//
// The completeness rule is per-subcommand (openclawOutputComplete): the runner
// must never treat "some output arrived" as "the answer arrived", or a response
// still streaming when the deadline hits would be reported as success.
func execOpenclawCLI(ctx context.Context, bin string, args ...string) (string, error) {
	raw, stderr, _, err := agent.RunCollectQuiet(ctx, os.Environ(), 0, openclawOutputComplete(args), bin, args...)
	if err != nil {
		stderrMsg := strings.TrimSpace(stderr)
		if ctxErr := ctx.Err(); ctxErr != nil {
			if stderrMsg != "" {
				return "", fmt.Errorf("openclaw %s: %w (process: %v; stderr: %s)", strings.Join(args, " "), ctxErr, err, stderrMsg)
			}
			return "", fmt.Errorf("openclaw %s: %w (process: %v)", strings.Join(args, " "), ctxErr, err)
		}
		if stderrMsg != "" {
			return "", fmt.Errorf("openclaw %s: %w (stderr: %s)", strings.Join(args, " "), err, stderrMsg)
		}
		if diag := openclawShimDiagnostic(bin, err); diag != "" {
			return "", fmt.Errorf("openclaw %s: %w (%s)", strings.Join(args, " "), err, diag)
		}
		return "", fmt.Errorf("openclaw %s: %w", strings.Join(args, " "), err)
	}
	return string(raw), nil
}

// openclawManagedMcpServers parses the agent's `mcp_config` JSON and returns
// the map of server name → server config that the wrapper should emit at
// `mcp.servers`. The second return is `true` when the agent has a managed
// mcp_config saved (non-null) — including the explicit empty set
// `{}` / `{"mcpServers":{}}` — and `false` when the field is null/absent so
// the user's global config flows through unmodified.
//
// Input shape mirrors the rest of Multica: Claude-style
// `{"mcpServers": {"<name>": {...}}}`. The server-entry fields pass through
// verbatim. OpenClaw's stdio schema uses the same camelCase keys (`command`,
// `args`, `env`) as Claude; HTTP/SSE entries should set OpenClaw's
// `transport` field directly (e.g. `"transport": "streamable-http"`) rather
// than Claude's `type` since OpenClaw does not recognise the latter.
//
// Each entry must declare either `command` (stdio) or `url` (http/sse); any
// other shape returns an error so the launch fails closed with an actionable
// message rather than running with a server OpenClaw will refuse to start.
func openclawManagedMcpServers(raw json.RawMessage) (map[string]any, bool, error) {
	trimmed := bytes.TrimSpace(raw)
	if len(trimmed) == 0 || bytes.Equal(trimmed, []byte("null")) {
		return nil, false, nil
	}
	var parsed struct {
		McpServers map[string]json.RawMessage `json:"mcpServers"`
	}
	if err := json.Unmarshal(trimmed, &parsed); err != nil {
		return nil, false, fmt.Errorf("parse mcp_config json: %w", err)
	}
	if len(parsed.McpServers) == 0 {
		return map[string]any{}, true, nil
	}
	names := make([]string, 0, len(parsed.McpServers))
	for name := range parsed.McpServers {
		names = append(names, name)
	}
	sort.Strings(names)
	out := make(map[string]any, len(names))
	for _, name := range names {
		var entry map[string]any
		if err := json.Unmarshal(parsed.McpServers[name], &entry); err != nil {
			return nil, false, fmt.Errorf("mcp_servers.%s: %w", name, err)
		}
		if entry == nil {
			return nil, false, fmt.Errorf("mcp_servers.%s must be a JSON object", name)
		}
		command, _ := entry["command"].(string)
		url, _ := entry["url"].(string)
		if strings.TrimSpace(command) == "" && strings.TrimSpace(url) == "" {
			return nil, false, fmt.Errorf("mcp_servers.%s must declare either `command` (stdio) or `url` (http/sse)", name)
		}
		out[name] = entry
	}
	return out, true, nil
}

// isOpenclawKeyMissing returns true when the CLI error indicates the asked-
// for path simply isn't set, as opposed to a real failure (bad config,
// CLI bug, missing binary). The CLI's "key not found" exit text has varied
// across versions, so we match on a handful of substrings rather than the
// exit code alone.
func isOpenclawKeyMissing(err error) bool {
	if err == nil {
		return false
	}
	// Match case-insensitively: the CLI's "key not found" wording has drifted
	// across versions and capitalization is not stable. Pre-2026.6 emitted
	// "Path not found"; OpenClaw 2026.6.x emits "Config path not found:
	// agents.list" (lowercase "path", "Config" prefix). A case-sensitive
	// strings.Contains on "Path not found" silently stopped matching the
	// 2026.6.x string, turning the intended graceful-skip into a fail-closed
	// error that broke every OpenClaw 2026.6.x runtime (see upstream #3028).
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "no value at ") ||
		strings.Contains(msg, "not set") ||
		strings.Contains(msg, "missing key") ||
		strings.Contains(msg, "path not found")
}

// isOpenclawUnknownSubcommand returns true when the CLI error indicates the
// invoked subcommand/option does not exist on this OpenClaw build (e.g. an
// older release predating `openclaw agents list --json`). Used so the
// registry fallback degrades to "no agents to pin" rather than failing
// closed on builds that never had the subcommand.
func isOpenclawUnknownSubcommand(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "unknown command") ||
		strings.Contains(msg, "unknown option") ||
		strings.Contains(msg, "does not recognize") ||
		strings.Contains(msg, "unknown argument")
}
