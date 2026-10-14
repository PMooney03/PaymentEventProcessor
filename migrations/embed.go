// Package migrations embeds the SQL migration files so that the migrate
// command (and integration tests) can apply them without needing the source
// tree on disk at runtime.
package migrations

import "embed"

// FS contains every *.sql migration file in this directory.
//
//go:embed *.sql
var FS embed.FS
