//go:build !nofailpoint

package failpoint

import (
	"sync"
	"sync/atomic"
)

var (
	mu      sync.RWMutex
	actions = map[string]Action{}
	active  atomic.Bool
)

// Hit runs the action enabled for name, if any. When no failpoint is enabled
// it costs one atomic load.
func Hit(name string) {
	if !active.Load() {
		return
	}
	mu.RLock()
	a := actions[name]
	mu.RUnlock()
	if a != nil {
		a()
	}
}

// Enable installs a for name, replacing any previous action.
func Enable(name string, a Action) {
	mu.Lock()
	defer mu.Unlock()
	actions[name] = a
	active.Store(true)
}

// Disable removes the action for name.
func Disable(name string) {
	mu.Lock()
	defer mu.Unlock()
	delete(actions, name)
	active.Store(len(actions) > 0)
}

// Reset removes every action.
func Reset() {
	mu.Lock()
	defer mu.Unlock()
	actions = map[string]Action{}
	active.Store(false)
}
