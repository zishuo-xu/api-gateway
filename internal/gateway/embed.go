package gateway

import (
	_ "embed"
)

//go:embed admin_ui.html
var adminUIHTML []byte

// mustEmbedAdminUI returns the embedded dashboard HTML. The file is compiled
// into the binary so the console works with zero external static-file mounts.
func mustEmbedAdminUI() []byte {
	return adminUIHTML
}
