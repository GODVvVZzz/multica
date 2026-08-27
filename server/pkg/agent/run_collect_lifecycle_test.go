//go:build !windows

package agent

import (
	"context"
	"errors"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// Regressions for the second review round on #6275. All three are cases where an
// earlier revision of this branch was *worse* than the launch.go helpers it
// replaced, which is the bar any replacement has to clear.

// writeWrapperExitingBeforeChild writes a CLI whose direct child exits 0
// immediately while a backgrounded descendant, holding the inherited stdout,
// prints the answer `delay` later.
//
// This is not a contrived shape. An npm-installed CLI on Windows is reached
// through a shim, a PowerShell launcher spawns a native child, and either can
// return before the process that owes us the answer has written it.
func writeWrapperExitingBeforeChild(t *testing.T, delay, answer string) string {
	t.Helper()
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-cli")
	body := "#!/bin/sh\n" +
		"( sleep " + delay + "; printf '%s\\n' '" + answer + "' ) &\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}
	return bin
}

// TestDetectCLIVersionWaitsForAWrapperDescendant pins the baseline comparison the
// review asked for: whatever mechanism detectCLIVersion uses must not do worse
// than launch.go's outputOwned on the same stub.
//
// The failure it guards against was measured on an earlier revision: treating the
// direct child's exit as "the answer is in" returned an empty version with a *nil
// error* in 0.41s, while outputOwned returned the version in 0.68s. Empty-and-nil
// is the worst possible shape here — DetectVersion routes all 23 providers through
// this function, and a caller cannot tell that answer from a CLI that legitimately
// prints nothing.
func TestDetectCLIVersionWaitsForAWrapperDescendant(t *testing.T) {
	bin := writeWrapperExitingBeforeChild(t, "0.5", "fake-cli 1.2.3")

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got, err := detectCLIVersion(ctx, Command{Path: bin, logger: slog.Default()})
	if err != nil {
		t.Fatalf("detectCLIVersion: %v", err)
	}
	if !strings.Contains(got, "1.2.3") {
		t.Fatalf("version = %q, want the descendant's output — leader exit is not "+
			"the end of output, only pipe EOF is", got)
	}

	// The baseline, on the identical stub, so this test fails if the mechanism
	// ever regresses below it rather than merely changing.
	cmd := Command{Path: bin}.exec(ctx)
	hideAgentWindow(cmd)
	out, oerr := outputOwned(cmd, slog.Default())
	if oerr != nil {
		t.Fatalf("outputOwned baseline: %v", oerr)
	}
	baseline, _ := extractVersionLine(string(out))
	if !strings.Contains(baseline, "1.2.3") {
		t.Fatalf("baseline outputOwned = %q, want the version — the stub is wrong, "+
			"not the code under test", out)
	}
}

// TestDetectCLIVersionDoesNotSalvageABannerAsTheVersion pins the third-round
// review finding: the ErrWaitDelay salvage was gated on `version != ""`, and
// extractVersionLine's trimmed-raw fallback makes any non-empty text satisfy
// that — including a line the wrapper printed before the real version existed.
//
// Measured on the reviewed head with this stub: detectCLIVersion returned
// version="initializing plugins" with a nil error in 2.31s, and logged "CLI
// answered but left its output pipes open" — the opposite of what happened. The
// banner would then be persisted as the runtime's version for every one of the 23
// providers, since DetectVersion routes them all through here.
//
// The contract: when the bound expires and no *recognised* version arrived, the
// original error stands. There is no answer to salvage.
func TestDetectCLIVersionDoesNotSalvageABannerAsTheVersion(t *testing.T) {
	// Banner on stdout, leader exits 0, and the real version arrives from a
	// descendant holding the pipe well past the 2s WaitDelay this probe sets.
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-cli")
	body := "#!/bin/sh\n" +
		"printf 'initializing plugins\\n'\n" +
		"( sleep 5; printf 'fake-cli 1.2.3\\n' ) &\n" +
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	got, err := detectCLIVersion(ctx, Command{Path: bin, logger: slog.Default()})
	if err == nil {
		t.Fatalf("detectCLIVersion = %q with a nil error; a banner is not the "+
			"answer, so the ErrWaitDelay failure must stand", got)
	}
	if got != "" {
		t.Errorf("version = %q, want empty — a failed probe must not report a "+
			"version the CLI never gave", got)
	}
	if !errors.Is(err, exec.ErrWaitDelay) {
		t.Errorf("err = %v, want it to wrap exec.ErrWaitDelay (the bound that "+
			"actually expired)", err)
	}
}

// TestRunCollectQuietWaitsForAWrapperDescendant is the same contract on the
// collector, which is the one mechanism in this package that can return before
// pipe EOF.
//
// It may do so only when the caller's completeness rule says the answer is in.
// With a rule that is not yet satisfied at leader exit, it has to keep waiting —
// bounded by collectDrainGrace — or it kills the process that owes the answer.
func TestRunCollectQuietWaitsForAWrapperDescendant(t *testing.T) {
	bin := writeWrapperExitingBeforeChild(t, "0.5", `{"ok":true}`)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	out, _, _, err := RunCollectQuiet(ctx, nil, 0, JSONOutputComplete, bin)
	if err != nil {
		t.Fatalf("RunCollectQuiet: %v", err)
	}
	if strings.TrimSpace(string(out)) != `{"ok":true}` {
		t.Fatalf("stdout = %q, want the descendant's document", out)
	}
}

// TestRunCollectQuietDoesNotWaitWhenTheAnswerIsIn is the other half: the drain
// wait must not become a tax on every call.
//
// A CLI that prints a complete answer and exits, leaving a helper on the pipe —
// which is what OpenClaw does on every invocation — has nothing left to wait for,
// so the rule short-circuits the drain and the call returns promptly rather than
// paying collectDrainGrace.
func TestRunCollectQuietDoesNotWaitWhenTheAnswerIsIn(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-cli")
	body := "#!/bin/sh\n" +
		"printf '{\"ok\":true}\\n'\n" +
		"sleep 300 &\n" + // inherits stdout, so EOF never arrives
		"exit 0\n"
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	start := time.Now()
	out, _, _, err := RunCollectQuiet(ctx, nil, 0, JSONOutputComplete, bin)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("RunCollectQuiet: %v", err)
	}
	if strings.TrimSpace(string(out)) != `{"ok":true}` {
		t.Fatalf("stdout = %q", out)
	}
	if elapsed >= collectDrainGrace {
		t.Errorf("took %v, i.e. at least the full drain grace (%v) — a satisfied "+
			"completeness rule must short-circuit the wait for EOF", elapsed, collectDrainGrace)
	}
}

// TestCollectedStderrKeepsOnlyItsTail pins the memory bound the review found
// missing. An earlier revision retained 13,107,400 bytes of stderr from a CLI
// writing continuously, where launch.go's outputOwned keeps the last
// probeStderrSampleBytes; a broken local CLI in a log loop could exhaust daemon
// memory inside the probe window.
//
// The tail rather than the head, matching outputOwned, because a CLI's actual
// failure line is at the end.
func TestCollectedStderrKeepsOnlyItsTail(t *testing.T) {
	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-cli")
	// ~64 KiB per line, 200 lines: ~12.8 MB, i.e. 400x the bound.
	body := "#!/bin/sh\n" +
		"line=$(printf 'x%.0s' $(seq 1 65536))\n" +
		"i=0\n" +
		"while [ $i -lt 200 ]; do printf '%s\\n' \"$line\" >&2; i=$((i+1)); done\n" +
		"printf 'LAST-STDERR-LINE\\n' >&2\n" +
		"printf '{\"ok\":true}\\n'\n"
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	_, stderr, _, err := RunCollectQuiet(ctx, nil, 0, JSONOutputComplete, bin)
	if err != nil {
		t.Fatalf("RunCollectQuiet: %v", err)
	}
	if len(stderr) > collectStderrTail {
		t.Errorf("retained %d bytes of stderr, want at most %d — the bound "+
			"outputOwned already applied", len(stderr), collectStderrTail)
	}
	if !strings.Contains(stderr, "LAST-STDERR-LINE") {
		t.Error("the tail was dropped instead of the head; a failed probe's " +
			"diagnosis is its last line")
	}
}

// TestCollectedStdoutOverflowIsReportedNotTruncated is the stdout half of the
// same bound, and it is deliberately not symmetric with stderr.
//
// stderr is a sample, so dropping its front costs nothing. stdout is the answer,
// and handing a caller a head-truncated answer is how a partial catalog becomes a
// confident empty one — so overflow is an error instead.
func TestCollectedStdoutOverflowIsReportedNotTruncated(t *testing.T) {
	prev := collectStdoutLimit
	collectStdoutLimit = 64 << 10
	t.Cleanup(func() { collectStdoutLimit = prev })

	dir := t.TempDir()
	bin := filepath.Join(dir, "fake-cli")
	body := "#!/bin/sh\n" +
		"line=$(printf 'y%.0s' $(seq 1 65536))\n" +
		"i=0\n" +
		"while [ $i -lt 8 ]; do printf '%s\\n' \"$line\"; i=$((i+1)); done\n"
	if err := os.WriteFile(bin, []byte(body), 0o755); err != nil {
		t.Fatalf("write stub: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()

	out, _, _, err := RunCollectQuiet(ctx, nil, 0, nil, bin)
	if err == nil {
		t.Fatalf("an over-limit answer must be reported, not silently shortened "+
			"(got %d bytes and a nil error)", len(out))
	}
	if !errors.Is(err, errCollectStdoutTooLarge) {
		t.Errorf("err = %v, want errCollectStdoutTooLarge", err)
	}
}
