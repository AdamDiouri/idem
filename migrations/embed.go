// Package migrations embeds the SQL schema migrations so the binary can apply them.
package migrations

import "embed"

// FS holds every *.sql migration, applied in filename order by goose.
//
//go:embed *.sql
var FS embed.FS
