package engine

// SetInboxAfterRead installs a hook that runs after an inbox event's read
// phase (snapshot, and provider re-check if any) and before its apply
// transaction (tests only).
func SetInboxAfterRead(p *InboxProcessor, f func()) { p.opts.afterRead = f }

// SetBeforeRecordHookFailure installs a hook that runs after a completion's
// hook failed and before the failure is recorded (tests only).
func SetBeforeRecordHookFailure(o *WorkerOptions, f func()) { o.beforeRecordHookFailure = f }
