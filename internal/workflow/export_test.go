package workflow

// SetBeforeCommit installs a hook that runs just before COMMIT in Submit.
// Returning an error from it simulates a crash at the last possible moment.
func SetBeforeCommit(s *Submitter, f func() error) {
	s.beforeCommit = f
}
