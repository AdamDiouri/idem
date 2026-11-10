package fakeprovider

import "time"

// SetFetchTimeout shortens the oracle clients' timeout and returns a function
// that restores it (tests only).
func SetFetchTimeout(d time.Duration) (restore func()) {
	old := oracleClient.Timeout
	oracleClient.Timeout = d
	return func() { oracleClient.Timeout = old }
}
