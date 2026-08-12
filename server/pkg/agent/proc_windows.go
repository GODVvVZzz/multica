//go:build windows

package agent

import (
	"errors"
	"fmt"
	"os"
	"os/exec"
	"syscall"
	"time"
	"unsafe"

	"golang.org/x/sys/windows"
)

// createNewConsole allocates a fresh console for the child process. Combined
// with HideWindow=true (STARTF_USESHOWWINDOW + SW_HIDE) the console window
// stays off-screen, and — critically — any grandchildren the agent spawns
// (tool subprocesses like bash, cmd, netstat, findstr) inherit this hidden
// console instead of each allocating their own visible one.
//
// Using CREATE_NO_WINDOW here instead would strip the console entirely,
// which forces Windows to allocate a new visible console per grandchild
// when the grandchild is a console-subsystem program that doesn't itself
// pass CREATE_NO_WINDOW — the exact popup storm reported in #1521.
const createNewConsole = 0x00000010

// hideAgentWindow configures cmd to suppress the console window on Windows
// while still giving descendant processes a hidden console to inherit.
// Stdio pipes set via cmd.StdoutPipe/StdinPipe keep working because
// STARTF_USESTDHANDLES takes precedence over the new console's stdio.
func hideAgentWindow(cmd *exec.Cmd) {
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.HideWindow = true
	cmd.SysProcAttr.CreationFlags |= createNewConsole
}

// configureProcessGroup is a no-op on Windows: there is no Setpgid/process-group
// signalling. Long-lived agent launches retain their existing direct-child
// cancellation contract; the one-shot collector below adds a Job Object.
func configureProcessGroup(cmd *exec.Cmd) {}

type collectorProcessTree struct {
	job windows.Handle
}

func newCollectorProcessTree(cmd *exec.Cmd) (*collectorProcessTree, error) {
	job, err := windows.CreateJobObject(nil, nil)
	if err != nil {
		return nil, err
	}
	info := windows.JOBOBJECT_EXTENDED_LIMIT_INFORMATION{}
	info.BasicLimitInformation.LimitFlags = windows.JOB_OBJECT_LIMIT_KILL_ON_JOB_CLOSE
	if _, err := windows.SetInformationJobObject(
		job,
		windows.JobObjectExtendedLimitInformation,
		uintptr(unsafe.Pointer(&info)),
		uint32(unsafe.Sizeof(info)),
	); err != nil {
		windows.CloseHandle(job)
		return nil, fmt.Errorf("set KILL_ON_JOB_CLOSE: %w", err)
	}
	if cmd.SysProcAttr == nil {
		cmd.SysProcAttr = &syscall.SysProcAttr{}
	}
	cmd.SysProcAttr.CreationFlags |= windows.CREATE_SUSPENDED
	return &collectorProcessTree{job: job}, nil
}

func (c *collectorProcessTree) attach(cmd *exec.Cmd) error {
	process, err := windows.OpenProcess(
		windows.PROCESS_SET_QUOTA|windows.PROCESS_TERMINATE,
		false,
		uint32(cmd.Process.Pid),
	)
	if err != nil {
		return fmt.Errorf("open process: %w", err)
	}
	defer windows.CloseHandle(process)
	if err := windows.AssignProcessToJobObject(c.job, process); err != nil {
		return fmt.Errorf("assign process to job: %w", err)
	}

	thread, err := suspendedProcessThread(uint32(cmd.Process.Pid))
	if err != nil {
		return err
	}
	defer windows.CloseHandle(thread)
	if _, err := windows.ResumeThread(thread); err != nil {
		return fmt.Errorf("resume process thread: %w", err)
	}
	return nil
}

func suspendedProcessThread(processID uint32) (windows.Handle, error) {
	snapshot, err := windows.CreateToolhelp32Snapshot(windows.TH32CS_SNAPTHREAD, 0)
	if err != nil {
		return 0, fmt.Errorf("snapshot process threads: %w", err)
	}
	defer windows.CloseHandle(snapshot)

	entry := windows.ThreadEntry32{Size: uint32(unsafe.Sizeof(windows.ThreadEntry32{}))}
	for err = windows.Thread32First(snapshot, &entry); err == nil; err = windows.Thread32Next(snapshot, &entry) {
		if entry.OwnerProcessID != processID {
			continue
		}
		thread, openErr := windows.OpenThread(windows.THREAD_SUSPEND_RESUME, false, entry.ThreadID)
		if openErr != nil {
			return 0, fmt.Errorf("open suspended process thread: %w", openErr)
		}
		return thread, nil
	}
	if !errors.Is(err, windows.ERROR_NO_MORE_FILES) {
		return 0, fmt.Errorf("enumerate process threads: %w", err)
	}
	return 0, fmt.Errorf("suspended process %d has no thread", processID)
}

func (c *collectorProcessTree) terminate(_ *exec.Cmd) error {
	if c == nil || c.job == 0 {
		return nil
	}
	return windows.TerminateJobObject(c.job, 1)
}

func (c *collectorProcessTree) close() {
	if c == nil || c.job == 0 {
		return
	}
	_ = windows.CloseHandle(c.job)
	c.job = 0
}

// codexInitializeRetrySupported remains false until Codex children are owned
// by a Job Object and descendant termination can be positively confirmed.
func codexInitializeRetrySupported() bool { return false }

// signalProcessGroup terminates the process on Windows. Windows has no
// SIGTERM/SIGKILL distinction or process-group signalling, so the signal is
// ignored and the process is killed directly (TerminateProcess via Kill). The
// caller's grace window still applies before this is invoked with SIGKILL.
func signalProcessGroup(p *os.Process, _ syscall.Signal) {
	if p == nil {
		return
	}
	_ = p.Kill()
}

func waitProcessGroupGone(_ *os.Process, _ time.Duration) bool { return false }
