// Package failpoint injects crashes and pauses at named points in the code so
// tests can prove recovery from failures at every boundary (design §14.2).
//
// Building with -tags nofailpoint compiles Hit to an empty function.
package failpoint

import (
	"errors"
	"io"
	"os"
	"sync"
)

// Action runs when an enabled failpoint is hit.
type Action func()

// ErrCrash is the panic value used to simulate a crash in-process.
var ErrCrash = errors.New("failpoint: simulated crash")

// stderr receives the line an exiting failpoint writes ("failpoint <name>
// fired: exit"), so a chaos run can tell which failpoints fired; swapped in tests.
var stderr io.Writer = os.Stderr

// exitFunc terminates the process for the "exit" action; swapped in tests.
var exitFunc = os.Exit

// IsCrash reports whether a recovered panic value is a simulated crash.
// Code that recovers panics must re-panic when IsCrash is true, so a simulated
// crash unwinds like a real one.
func IsCrash(r any) bool {
	err, ok := r.(error)
	return ok && errors.Is(err, ErrCrash)
}

// CrashOnce returns an action that panics with ErrCrash the first time it runs.
func CrashOnce() Action {
	var once sync.Once
	return func() {
		crash := false
		once.Do(func() { crash = true })
		if crash {
			panic(ErrCrash)
		}
	}
}

// BlockOnce returns an action that blocks the first caller until release is
// called, simulating a paused process (GC pause, SIGSTOP). entered is closed
// when the first caller starts blocking. release is idempotent.
func BlockOnce() (a Action, entered <-chan struct{}, release func()) {
	enteredCh := make(chan struct{})
	releaseCh := make(chan struct{})
	var first, rel sync.Once

	a = func() {
		block := false
		first.Do(func() { block = true })
		if !block {
			return
		}
		close(enteredCh)
		<-releaseCh
	}
	return a, enteredCh, func() { rel.Do(func() { close(releaseCh) }) }
}
