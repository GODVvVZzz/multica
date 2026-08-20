package agent

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"sync"
	"syscall"
	"time"
)

// collectDrainGrace bounds the total cleanup wait in finish(): the direct child
// must be reaped and the reader goroutines must see EOF within this one budget.
// If a descendant escaped the platform process-tree boundary, finish closes the
// read ends outright when the budget expires.
const collectDrainGrace = 5 * time.Second

// outputBuffer accumulates one stream and records when the last write landed.
//
// buf and lastWrite are updated inside the same critical section on purpose. If
// the timestamp were published after releasing the lock, a reader could observe
// new bytes together with a stale timestamp and conclude the stream had gone
// quiet at the very moment it was producing — which for RunCollectQuiet means
// truncating an answer mid-write.
type outputBuffer struct {
	mu        sync.Mutex
	buf       []byte
	lastWrite time.Time
}

// absorb drains r into the buffer until EOF or the file is closed. Read errors
// other than those are reported; a truncated stream is surfaced to the caller
// as whatever arrived, matching the previous cmd.Output() behaviour.
func (o *outputBuffer) absorb(r io.Reader) error {
	chunk := make([]byte, 32*1024)
	for {
		n, err := r.Read(chunk)
		if n > 0 {
			now := time.Now()
			o.mu.Lock()
			o.buf = append(o.buf, chunk[:n]...)
			o.lastWrite = now
			o.mu.Unlock()
		}
		if err != nil {
			if errors.Is(err, io.EOF) || errors.Is(err, os.ErrClosed) {
				return nil
			}
			return err
		}
	}
}

func (o *outputBuffer) snapshot() []byte {
	o.mu.Lock()
	defer o.mu.Unlock()
	return append([]byte(nil), o.buf...)
}

// idleFor reports how long since the last write and whether anything has been
// written at all.
func (o *outputBuffer) idleFor() (time.Duration, bool) {
	o.mu.Lock()
	defer o.mu.Unlock()
	if o.lastWrite.IsZero() {
		return 0, false
	}
	return time.Since(o.lastWrite), true
}

// collector runs a command with pipes this package owns, rather than handing
// os/exec an io.Writer and letting it do the draining.
//
// That distinction is the whole point. When os/exec owns the draining, Wait
// blocks until the output pipes reach EOF, and EOF requires every write end to
// be closed — so a CLI that forks a helper inheriting stdout (OpenClaw spawns
// `openclaw-config`) keeps Wait open for the helper's lifetime. Cancelling the
// context does not help: it kills the direct child without unblocking os/exec's
// io.Copy. Handing os/exec an *os.File instead means it starts no copy goroutine
// at all, so Wait returns the instant the direct child exits and this package
// decides when reading is over.
type collector struct {
	cmd        *exec.Cmd
	outR, errR *os.File
	stdout     outputBuffer
	stderr     outputBuffer

	readers   chan struct{} // closed once both absorb loops have returned
	waitDone  chan struct{} // closed once cmd.Wait has returned
	waitErr   error         // valid after waitDone is closed
	finishErr error         // valid after finishOnce has run

	finishOnce sync.Once
}

// startCollector takes a *exec.Cmd the caller has already built rather than an
// executable path, so a launch that carries an argv prefix (Command.exec, which
// applies a custom runtime's fixed_args) keeps it. Building the command here
// would silently drop that prefix.
//
// The caller must not have started it, and must leave Stdout/Stderr unset: this
// package installs its own *os.File pipes, which is the whole reason Wait cannot
// be held hostage by a descendant.
//
// No ctx parameter: the collector's lifetime is bounded by the callers below,
// which select on ctx themselves. Taking one here would suggest this function
// enforces it.
func startCollector(cmd *exec.Cmd, env []string) (*collector, error) {
	if env != nil {
		cmd.Env = env
	}
	hideAgentWindow(cmd)
	configureProcessGroup(cmd)

	outR, outW, err := os.Pipe()
	if err != nil {
		return nil, fmt.Errorf("collect stdout pipe: %w", err)
	}
	errR, errW, err := os.Pipe()
	if err != nil {
		outR.Close()
		outW.Close()
		return nil, fmt.Errorf("collect stderr pipe: %w", err)
	}

	cmd.Stdout = outW
	cmd.Stderr = errW

	// startOwnedProcessTree rather than cmd.Start: on Windows it creates the
	// child suspended and assigns it to a Job Object before it runs a single
	// instruction, so a .cmd shim cannot spawn the real CLI outside the
	// ownership boundary. On Unix configureProcessGroup above already did the
	// equivalent and this is a plain Start. Either way the tree — not just the
	// direct child — is what finish() gets to reap.
	if startErr := startOwnedProcessTree(cmd, slog.Default()); startErr != nil {
		releaseProcessGroup(cmd)
		outR.Close()
		outW.Close()
		errR.Close()
		errW.Close()
		return nil, startErr
	}

	// Drop the parent's write ends immediately: otherwise EOF can never arrive
	// no matter how thoroughly the child tree is reaped.
	outW.Close()
	errW.Close()

	c := &collector{
		cmd:      cmd,
		outR:     outR,
		errR:     errR,
		readers:  make(chan struct{}),
		waitDone: make(chan struct{}),
	}

	var wg sync.WaitGroup
	wg.Add(2)
	go func() { defer wg.Done(); _ = c.stdout.absorb(outR) }()
	go func() { defer wg.Done(); _ = c.stderr.absorb(errR) }()
	go func() { wg.Wait(); close(c.readers) }()
	go func() { c.waitErr = cmd.Wait(); close(c.waitDone) }()

	return c, nil
}

// finish reaps the process tree and guarantees that once it returns, no
// goroutine started by startCollector is still running and nothing can still
// append to the buffers — so the caller may snapshot them without racing.
//
// Safe to call more than once and from any of the caller's exit paths.
func (c *collector) finish() error {
	c.finishOnce.Do(func() {
		drainDeadline := time.Now().Add(collectDrainGrace)
		// Reap whatever the CLI forked, on the success path too: a successful
		// `openclaw --version` still leaves its helper behind, which is how
		// orphans accumulate on a host that probes on a timer. This also
		// releases the last write end so the readers below can see EOF.
		reapProcessTree(c.cmd)

		// Bounded belt-and-braces: because this package owns the pipes, Wait is
		// not being held open by a descendant, so it returns as soon as the
		// direct child is gone — which reapProcessTree has just ensured.
		waitPending := !waitUntil(c.waitDone, drainDeadline)

		drainForced := false
		if !waitUntil(c.readers, drainDeadline) {
			// A descendant we could not kill still holds a write end, so EOF
			// will not arrive on its own. Close the read ends and wait for the
			// absorb loops: returning without that wait would leave io.Copy
			// appending to a buffer the caller is about to read.
			//
			// On Unix the close evicts a blocked Read, because the pipe is
			// pollable. On Windows an anonymous pipe is not, so the close waits
			// for the in-flight Read to release its reference — the wait below
			// can then last until the descendant closes the write end. That is
			// a slow return rather than a corrupt buffer, and it is only
			// reachable when the tree kill above did not take.
			drainForced = true
			c.outR.Close()
			c.errR.Close()
			<-c.readers
		}

		c.outR.Close()
		c.errR.Close()
		// Only now: on Windows closing the job handle is what kills anything
		// still inside it, so it must not run while the tree could still be
		// serving output.
		releaseProcessGroup(c.cmd)

		// Report, rather than swallow, a cleanup that did not converge. Both
		// cases mean the OS refused to let us kill something, and both cost the
		// caller something it would otherwise have no way to know about: an
		// outstanding Wait, or output that may be truncated. Callers surface
		// this through collectorError, so a command that failed on its own
		// merits keeps its error and gains this one alongside it.
		switch {
		case waitPending:
			c.finishErr = fmt.Errorf("collect cleanup: %s did not exit within %v of the tree kill, so its Wait is still outstanding",
				c.cmd.Path, collectDrainGrace)
		case drainForced:
			c.finishErr = fmt.Errorf("collect cleanup: a descendant of %s still held the output pipe after %v, so the read ends were force-closed and trailing output may be missing",
				c.cmd.Path, collectDrainGrace)
		}
	})
	return c.finishErr
}

func waitUntil(done <-chan struct{}, deadline time.Time) bool {
	select {
	case <-done:
		return true
	default:
	}
	remaining := time.Until(deadline)
	if remaining <= 0 {
		return false
	}
	timer := time.NewTimer(remaining)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

func collectorError(commandErr, cleanupErr error) error {
	if cleanupErr == nil {
		return commandErr
	}
	if commandErr == nil {
		return cleanupErr
	}
	return errors.Join(commandErr, cleanupErr)
}

// exitErr reports the command's exit error without blocking, and whether the
// command has been reaped at all yet.
func (c *collector) exitErr() (error, bool) {
	select {
	case <-c.waitDone:
		return c.waitErr, true
	default:
		return nil, false
	}
}

// RunCollect runs a short-lived CLI command to completion and returns its
// stdout and stderr. It is the safe replacement for cmd.Output() on any path
// that shells out to an agent CLI — see collector for why cmd.Output() cannot
// be bounded when the CLI leaves a descendant holding stdout.
//
// #6084 measured that shape (a shim whose backgrounded child slept 6s took
// 6.01s against a 150ms deadline) and reverted a cmd.WaitDelay backstop on
// review, because WaitDelay bounds the call only by leaving the descendant
// running and reports exec.ErrWaitDelay — turning a probe whose output arrived
// perfectly fine into a failure. The gap was tracked as MUL-5467; this closes
// it by owning the pipes and the process group instead.
//
// Guarantees callers depend on:
//
//  1. Returns within roughly the caller's context deadline plus
//     collectDrainGrace, whatever the CLI leaves behind.
//  2. Descendants the CLI forked are killed before returning, so
//     invoking a CLI on a timer cannot accumulate orphans.
//  3. Whenever the tree is reaped successfully — which is every case the OS
//     lets us have — the reader goroutines and cmd.Wait have all returned
//     before this call does. If the kill does not take, finish() reports it
//     (see there) and one goroutine can stay parked in cmd.Wait on a process
//     nothing is able to terminate; that residue is not removable from here,
//     which is why it is surfaced as an error instead of being asserted away.
//     On Windows the same case can extend the call rather than leak, because an
//     anonymous pipe is not pollable there and closing the read end waits for
//     an in-flight Read to release it.
//  4. The command's real exit status is reported, which openclawShimDiagnostic
//     depends on (it type-switches on *exec.ExitError).
//
// env, when non-nil, replaces the child's environment (os/exec semantics).
//
// Not for agent execution: those paths stream stdout incrementally and manage
// their own lifecycle. Use this for one-shot, read-only invocations
// (`--version`, `agents list`, `config get`).
func RunCollect(ctx context.Context, env []string, execPath string, args ...string) (stdout []byte, stderr string, err error) {
	// Command.exec, not exec.CommandContext: launch.go owns process construction
	// so a custom runtime's fixed_args prefix cannot be dropped (GH #7046). A zero
	// Command has no prefix, which is what a bare path argument means here.
	return RunCollectCmd(ctx, Command{Path: execPath}.exec(ctx, args...), env)
}

// RunCollectCmd is RunCollect for a caller that already holds a *exec.Cmd, which
// is what Command.exec returns.
//
// ctx bounds the call on its own: it is not assumed that cmd was built with
// exec.CommandContext. RunCollect's own cmd is, but a caller passing a plain
// exec.Command would otherwise block here for as long as the CLI chose to run,
// with the ctx argument doing nothing.
func RunCollectCmd(ctx context.Context, cmd *exec.Cmd, env []string) (stdout []byte, stderr string, err error) {
	c, startErr := startCollector(cmd, env)
	if startErr != nil {
		return nil, "", startErr
	}
	select {
	case <-c.waitDone:
		finishErr := c.finish()
		return c.stdout.snapshot(), string(c.stderr.snapshot()), collectorError(c.waitErr, finishErr)
	case <-ctx.Done():
		// Same ordering as RunCollectQuietCmd's deadline branch, so the two
		// entry points cannot hand callers different error shapes for the same
		// situation: prefer the process error once the tree has been reaped —
		// "signal: killed" is worth keeping — and fall back to ctx.Err().
		finishErr := c.finish()
		out := c.stdout.snapshot()
		if werr, reaped := c.exitErr(); reaped && werr != nil {
			return out, string(c.stderr.snapshot()), collectorError(werr, finishErr)
		}
		return out, string(c.stderr.snapshot()), collectorError(ctx.Err(), finishErr)
	}
}

// reapProcessTree SIGKILLs the process group led by cmd's child, so helpers the
// child forked die with it. Safe to call after the child has already exited:
// configureProcessGroup makes the child the group leader, so its pid doubles as
// the group id and the group outlives the leader for as long as any member runs.
// An empty group yields ESRCH, which signalProcessGroup absorbs.
//
// The group kill is issued just after Wait has reaped the leader, so in
// principle the leader's pid is already free for reuse. Sequential pid
// allocation makes reuse inside that window (microseconds, and only after
// wrapping the whole pid space) not a practical concern, and it is the same
// window the other backends' cancellation paths already live with.
//
// On Windows signalProcessGroup terminates the Job Object that
// startOwnedProcessTree assigned the child to, which reaches the same
// descendants; see proc_windows.go.
func reapProcessTree(cmd *exec.Cmd) {
	if cmd == nil || cmd.Process == nil {
		return
	}
	signalProcessGroup(cmd, syscall.SIGKILL)
}
