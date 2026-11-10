package main

import (
	"testing"

	"github.com/stretchr/testify/require"
)

// Slice 2.3b: `idem migrate` creates the application's login user only when
// both variables are set; one without the other is a mistake, caught before
// touching the database.
func TestMigrate_AppUserNeedsBothVariables(t *testing.T) {
	t.Setenv("IDEM_DATABASE_URL", "postgres://idem:idem@127.0.0.1:1/idem?sslmode=disable&connect_timeout=1")
	t.Setenv("IDEM_APP_DB_USER", "idem_app_user")
	t.Setenv("IDEM_APP_DB_PASSWORD", "")
	code, out := run(t, "migrate")
	require.Equal(t, 2, code, out)
	require.Contains(t, out, "IDEM_APP_DB_USER and IDEM_APP_DB_PASSWORD must be set together")
}
