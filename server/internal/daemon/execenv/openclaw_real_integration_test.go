//go:build agentintegration

package execenv

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

func realOpenclawBin(t *testing.T) string {
	t.Helper()
	if os.Getenv("MULTICA_RUN_REAL_AGENT_SMOKE") != "1" {
		t.Skip("set MULTICA_RUN_REAL_AGENT_SMOKE=1 to allow real agent CLI access")
	}
	if testing.Short() {
		t.Skip("skipping real-binary smoke test in -short mode")
	}

	bin := os.Getenv("MULTICA_REAL_OPENCLAW_BIN")
	if bin == "" {
		var err error
		bin, err = exec.LookPath("openclaw")
		if err != nil {
			t.Skip("openclaw not on PATH; skipping real-binary smoke test")
		}
	}
	return bin
}

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
		"mcp": {
			"sessionIdleTtlMs": 300000,
			"servers": {"user-only": {"command": "echo"}}
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

func TestPrepareOpenclawConfigRealCLI(t *testing.T) {
	bin, activeConfig := realOpenclawConfig(t)

	envRoot := t.TempDir()
	workDir := filepath.Join(envRoot, "workdir")
	if err := os.MkdirAll(workDir, 0o755); err != nil {
		t.Fatalf("create workdir: %v", err)
	}

	started := time.Now()
	result, err := prepareOpenclawConfig(envRoot, workDir, OpenclawConfigPrep{
		OpenclawBin: bin,
		McpConfig:   json.RawMessage(`{"mcpServers":{}}`),
	})
	if err != nil {
		t.Fatalf("prepare with real OpenClaw CLI: %v", err)
	}
	t.Logf("real OpenClaw config preparation completed in %s", time.Since(started))

	data, err := os.ReadFile(result.ConfigPath)
	if err != nil {
		t.Fatalf("read generated wrapper: %v", err)
	}
	var wrapper map[string]any
	if err := json.Unmarshal(data, &wrapper); err != nil {
		t.Fatalf("parse generated wrapper: %v", err)
	}
	include, ok := wrapper["$include"].([]any)
	snapshotPath := filepath.Join(envRoot, openclawUserSnapshotFile)
	if !ok || len(include) != 1 || include[0] != snapshotPath {
		t.Fatalf("wrapper $include = %#v, want [%q]", wrapper["$include"], snapshotPath)
	}
	agents, ok := wrapper["agents"].(map[string]any)
	if !ok {
		t.Fatalf("wrapper agents = %#v, want object", wrapper["agents"])
	}
	defaults, ok := agents["defaults"].(map[string]any)
	if !ok || defaults["workspace"] != workDir {
		t.Fatalf("wrapper agents.defaults = %#v, want workspace %q", agents["defaults"], workDir)
	}
	if result.IncludeRoot != filepath.Dir(activeConfig) {
		t.Fatalf("include root = %q, want %q", result.IncludeRoot, filepath.Dir(activeConfig))
	}

	// Ask the real CLI to resolve the generated include chain. This verifies
	// the reset bridge rather than merely inspecting the JSON files we wrote.
	t.Setenv("OPENCLAW_CONFIG_PATH", result.ConfigPath)
	t.Setenv("OPENCLAW_INCLUDE_ROOTS", result.IncludeRoot)
	resolvedMcp, err := openclawResolvedMcpConfig(bin, openclawCLITimeout)
	if err != nil {
		t.Fatalf("resolve generated wrapper with real OpenClaw CLI: %v", err)
	}
	servers, ok := resolvedMcp["servers"].(map[string]any)
	if !ok || len(servers) != 0 {
		t.Fatalf("resolved managed servers = %#v, want empty and no user-only leak", resolvedMcp["servers"])
	}
	if resolvedMcp["sessionIdleTtlMs"] != float64(300000) {
		t.Fatalf("resolved non-server MCP settings = %#v, want sessionIdleTtlMs preserved", resolvedMcp)
	}
}

func TestOpenclawDaemonEquivalentRealTask(t *testing.T) {
	bin := realOpenclawBin(t)
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))

	prepareCtx, prepareCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer prepareCancel()
	started := time.Now()
	environment, err := PrepareIsolated(prepareCtx, preparationHelperTestCommand(), PrepareParams{
		WorkspacesRoot: t.TempDir(),
		WorkspaceID:    "00000000-0000-4000-8000-000000000001",
		TaskID:         "00000000-0000-4000-8000-000000000002",
		AgentName:      "openclaw-real-smoke",
		Provider:       "openclaw",
		OpenclawBin:    bin,
		Task: TaskContextForEnv{
			IssueID: "openclaw-real-smoke",
		},
	}, logger)
	if err != nil {
		t.Fatalf("prepare isolated daemon-equivalent environment: %v", err)
	}
	t.Logf("real isolated daemon-equivalent preparation completed in %s", time.Since(started))

	agentEnv := map[string]string{
		"OPENCLAW_CONFIG_PATH": environment.OpenclawConfigPath,
	}
	if environment.OpenclawIncludeRoot != "" {
		roots := []string{environment.OpenclawIncludeRoot}
		if existing := strings.TrimSpace(os.Getenv("OPENCLAW_INCLUDE_ROOTS")); existing != "" {
			roots = append(roots, existing)
		}
		agentEnv["OPENCLAW_INCLUDE_ROOTS"] = strings.Join(roots, string(os.PathListSeparator))
	}
	backend, err := agent.ResolveBackend("openclaw", agent.Config{
		ExecutablePath: bin,
		Env:            agentEnv,
		Logger:         logger,
		TaskID:         "00000000-0000-4000-8000-000000000002",
		BuiltinRuntime: true,
	})
	if err != nil {
		t.Fatalf("resolve real OpenClaw backend: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	session, err := backend.Execute(ctx, "Reply with exactly: MULTICA_DAEMON_REAL_OK", agent.ExecOptions{
		Cwd:     environment.WorkDir,
		Model:   "main",
		Timeout: 2 * time.Minute,
	})
	if err != nil {
		t.Fatalf("start real OpenClaw backend: %v", err)
	}
	messagesDone := make(chan struct{})
	go func() {
		defer close(messagesDone)
		for range session.Messages {
		}
	}()
	result, ok := <-session.Result
	<-messagesDone
	if !ok {
		t.Fatal("real OpenClaw backend closed without a result")
	}
	if result.Status != "completed" {
		t.Fatalf("real OpenClaw task status = %q, error = %q", result.Status, result.Error)
	}
	if strings.TrimSpace(result.Output) != "MULTICA_DAEMON_REAL_OK" {
		t.Fatalf("real OpenClaw task output = %q", result.Output)
	}
	t.Logf("real daemon-equivalent task completed in %dms", result.DurationMs)
}
