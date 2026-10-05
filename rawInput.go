package main

import (
	"os"
	"syscall"
	"unsafe"
)

// enableRawInput puts the terminal on stdin into single-key mode: key presses
// arrive immediately and aren't echoed. It returns a function that restores the
// original settings and true, or false if stdin isn't a terminal.
//
// Only line buffering (ICANON) and echo are switched off. Signal generation
// (ISIG) is left on, so Ctrl-C still raises SIGINT, and output processing
// (OPOST) is left on, so the "\n" in the progress display still returns the
// cursor to column 0.
func enableRawInput() (restore func(), ok bool) {
	fd := int(os.Stdin.Fd())

	var old syscall.Termios
	if err := ioctlTermios(fd, syscall.TCGETS, &old); err != nil {
		return func() {}, false
	}

	raw := old
	raw.Lflag &^= syscall.ICANON | syscall.ECHO
	raw.Cc[syscall.VMIN] = 1  // a read returns after one byte
	raw.Cc[syscall.VTIME] = 0 // with no timeout
	if err := ioctlTermios(fd, syscall.TCSETS, &raw); err != nil {
		return func() {}, false
	}

	return func() { _ = ioctlTermios(fd, syscall.TCSETS, &old) }, true
}

func ioctlTermios(fd int, req uintptr, t *syscall.Termios) error {
	_, _, errno := syscall.Syscall(syscall.SYS_IOCTL, uintptr(fd), req, uintptr(unsafe.Pointer(t)))
	if errno != 0 {
		return errno
	}
	return nil
}
