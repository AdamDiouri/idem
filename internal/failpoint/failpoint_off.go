//go:build nofailpoint

package failpoint

// Hit does nothing in release builds.
func Hit(string) {}

// Enable does nothing in release builds.
func Enable(string, Action) {}

// Disable does nothing in release builds.
func Disable(string) {}

// Reset does nothing in release builds.
func Reset() {}
