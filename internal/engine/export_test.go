package engine

// SetInboxAfterRecheck installs a hook that runs between an inbox event's
// provider re-check and its apply transaction (tests only).
func SetInboxAfterRecheck(p *InboxProcessor, f func()) { p.opts.afterRecheck = f }
