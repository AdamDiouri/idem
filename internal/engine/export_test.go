package engine

// SetInboxAfterRead installs a hook that runs after an inbox event's read
// phase (snapshot, and provider re-check if any) and before its apply
// transaction (tests only).
func SetInboxAfterRead(p *InboxProcessor, f func()) { p.opts.afterRead = f }
