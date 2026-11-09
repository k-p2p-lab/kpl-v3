//go:build linux

package workerproc

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	"golang.org/x/sys/unix"
)

func currentExecutable() (string, error) { return "/proc/self/exe", nil }
func configureProcess(cmd *exec.Cmd) {
	cmd.SysProcAttr = &syscall.SysProcAttr{Setpgid: true, Pdeathsig: syscall.SIGKILL}
}
func terminateProcess(cmd *exec.Cmd) error { return signalRunningProcess(cmd, syscall.SIGTERM) }
func killProcess(cmd *exec.Cmd) error      { return signalRunningProcess(cmd, syscall.SIGKILL) }
func cleanupProcessGroup(cmd *exec.Cmd)    { _ = syscall.Kill(-cmd.Process.Pid, syscall.SIGKILL) }
func preparePrivatePipes() error {
	for _, fd := range []int{3, 4} {
		var stat unix.Stat_t
		if err := unix.Fstat(fd, &stat); err != nil {
			return err
		}
		if stat.Mode&unix.S_IFMT != unix.S_IFIFO {
			return ErrPrivateRole
		}
		// ExtraFiles are blocking after exec. NewFile must see O_NONBLOCK to
		// register these pipes with netpoll, allowing Close to cancel reads.
		if err := unix.SetNonblock(fd, true); err != nil {
			return err
		}
	}
	return nil
}
func privateFilesCloseOnExec(files ...*os.File) error {
	for _, file := range files {
		raw, err := file.SyscallConn()
		if err != nil {
			return err
		}
		var markErr error
		if err := raw.Control(func(fd uintptr) { _, markErr = unix.FcntlInt(fd, unix.F_SETFD, unix.FD_CLOEXEC) }); err != nil {
			return err
		}
		if markErr != nil {
			return markErr
		}
	}
	return nil
}

func signalRunningProcess(cmd *exec.Cmd, signal syscall.Signal) error {
	// kill(2) can succeed for a zombie whose Cmd.Wait goroutine has not run yet.
	// Such success must not turn an earlier external SIGKILL into our own kill.
	var info unix.Siginfo
	for {
		err := unix.Waitid(unix.P_PID, cmd.Process.Pid, &info, unix.WEXITED|unix.WNOHANG|unix.WNOWAIT, nil)
		if errors.Is(err, unix.EINTR) {
			continue
		}
		if errors.Is(err, unix.ECHILD) || err == nil && info.Signo != 0 {
			return os.ErrProcessDone
		}
		if err != nil {
			return err
		}
		return syscall.Kill(-cmd.Process.Pid, signal)
	}
}

func expectedTermination(err error, term, killed bool) bool {
	var exited *exec.ExitError
	if !errors.As(err, &exited) {
		return false
	}
	status, ok := exited.Sys().(syscall.WaitStatus)
	return ok && status.Signaled() && (term && status.Signal() == syscall.SIGTERM || killed && status.Signal() == syscall.SIGKILL)
}
