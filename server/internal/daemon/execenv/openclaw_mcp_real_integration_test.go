//go:build agentintegration

package execenv

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
)

// Real-CLI coverage for the managed-MCP include chain. Opt-in twice over: the
// agentintegration build tag, and MULTICA_RUN_REAL_AGENT_SMOKE, because this
// executes the openclaw binary installed on the host.
//
// The unit tests above assert the JSON this package writes. That is not enough
// for a design whose correctness lives in OpenClaw's include-merge semantics —
// ordered array includes, the nested wrapper-to-snapshot include, and
// `{"mcp": null}` resetting the user's block before the managed set is merged.
// Only the real loader can confirm that, which is why this test asks the CLI to
// resolve the chain we generated rather than inspecting our own files.
//
// All three loader behaviors and the exact managed-server isolation were
// measured on 2026-08-26 on Windows 10.0.19045.6466 (PowerShell 7.6.4, Go
// 1.26.6 windows/amd64) against the npm extended-stable (2026.6.34), latest
// (2026.7.1-2), and beta (2026.8.1-beta.3) channels: PASS in 36.9s, 40.6s and
// 34.2s respectively. Windows on purpose — that is the platform where the npm
// shim puts `cmd.exe → node → node` between the daemon and the CLI, so it is the
// least forgiving place to assert include resolution.
//
// The control that makes those passes mean something was run by hand rather than
// asserted here: dropping the reset stage from the chain leaks `user-only` back
// into the resolved server map on all three channels. Without that, a green test
// could equally mean "the wrapper's own block happened to win".
//
// The OpenClaw config compatibility smoke workflow keeps those three moving
// channels under scheduled and manually dispatched coverage.
func realOpenclawBin(t *testing.T) string {
	t.Helper()
	if os.Getenv("MULTICA_RUN_REAL_AGENT_SMOKE") == "" {
		t.Skip("set MULTICA_RUN_REAL_AGENT_SMOKE=1 to run the real openclaw smoke tests")
	}
	bin, err := exec.LookPath("openclaw")
	if err != nil {
		t.Skipf("openclaw not installed: %v", err)
	}
	return bin
}

// realOpenclawConfig points the CLI at an isolated HOME holding a user config
// with both a user-only MCP server (which must not survive the reset) and a
// same-name server whose definition the managed set must replace.
func realOpenclawConfig(t *testing.T) (bin, activeConfig string) {
	t.Helper()
	bin = realOpenclawBin(t)

	home := t.TempDir()
	stateDir := filepath.Join(home, ".openclaw")
	if err := os.MkdirAll(stateDir, 0o700); err != nil {
		t.Fatalf("create isolated OpenClaw state: %v", err)
	}
	activeConfig = filepath.Join(stateDir, "openclaw.json")
	config := `{
		"gateway": {"mode": "local"},
		"logging": {"level": "debug"},
		"mcp": {
			"servers": {
				"user-only": {"command": "user-only"},
				"shared": {"command": "user-shared"}
			}
		}
	}`
	if err := os.WriteFile(activeConfig, []byte(config), 0o600); err != nil {
		t.Fatalf("write isolated OpenClaw config: %v", err)
	}
	t.Setenv("HOME", home)
	t.Setenv("USERPROFILE", home)
	t.Setenv("OPENCLAW_HOME", home)
	t.Setenv("OPENCLAW_STATE_DIR", stateDir)
	t.Setenv("OPENCLAW_CONFIG_PATH", activeConfig)
	return bin, activeConfig
}

func realOpenclawConfigGetJSON(t *testing.T, bin, keyPath string) any {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), openclawCLITimeout)
	defer cancel()
	out, err := openclawExec(ctx, bin, "config", "get", keyPath, "--json")
	if err != nil {
		t.Fatalf("resolve %s through the real CLI: %v", keyPath, annotateOpenclawJSONError(err, out))
	}
	var value any
	if err := json.Unmarshal([]byte(out), &value); err != nil {
		t.Fatalf("parse resolved %s JSON %q: %v", keyPath, out, err)
	}
	return value
}

func TestPrepareOpenclawConfigRealCLI(t *testing.T) {
	bin, activeConfig := realOpenclawConfig(t)

	envRoot := t.TempDir()
	workDir := filepath.Join(envRoot, "workdir")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("mkdir workdir: %v", err)
	}

	result, err := prepareOpenclawConfig(envRoot, workDir, OpenclawConfigPrep{
		OpenclawBin: bin,
		McpConfig: json.RawMessage(`{"mcpServers":{
			"managed-only":{"command":"managed-only"},
			"shared":{"command":"managed-shared"}
		}}`),
	})
	if err != nil {
		t.Fatalf("prepareOpenclawConfig against the real CLI: %v", err)
	}

	wrapper := mustReadJSON(t, result.ConfigPath)
	snapshotPath := filepath.Join(envRoot, openclawUserSnapshotFile)
	include, ok := wrapper["$include"].([]any)
	if !ok || len(include) != 1 || include[0] != snapshotPath {
		t.Fatalf("wrapper $include = %#v, want [%q]", wrapper["$include"], snapshotPath)
	}
	if result.IncludeRoot != filepath.Dir(activeConfig) {
		t.Fatalf("include root = %q, want %q", result.IncludeRoot, filepath.Dir(activeConfig))
	}

	// Ask the real CLI to resolve the generated include chain. This is what
	// verifies the reset bridge rather than merely inspecting the JSON we wrote.
	t.Setenv("OPENCLAW_CONFIG_PATH", result.ConfigPath)
	t.Setenv("OPENCLAW_INCLUDE_ROOTS", result.IncludeRoot)
	// This field exists only in the live user config. Seeing it through the
	// generated wrapper proves the outer include followed the nested snapshot
	// include rather than merely leaving the wrapper's managed MCP block intact.
	if level := realOpenclawConfigGetJSON(t, bin, "logging.level"); level != "debug" {
		t.Fatalf("resolved logging.level = %#v, want debug through nested includes", level)
	}
	resolvedMcp, err := openclawResolvedMcpConfig(bin, openclawCLITimeout)
	if err != nil {
		t.Fatalf("resolve the generated config with the real CLI: %v", err)
	}
	servers, ok := resolvedMcp["servers"].(map[string]any)
	wantServers := map[string]any{
		"managed-only": map[string]any{"command": "managed-only"},
		"shared":       map[string]any{"command": "managed-shared"},
	}
	if !ok || !reflect.DeepEqual(servers, wantServers) {
		t.Fatalf("resolved managed servers = %#v, want exactly %#v", resolvedMcp["servers"], wantServers)
	}
}
