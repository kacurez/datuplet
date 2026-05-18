// notokenlog is the command-line entry point for the notokenlog static
// analyzer (RFC 019 §4.10). Invoke as:
//
//	go run ./tools/lint/notokenlog/cmd/notokenlog ./...
//
// Wired into the Makefile's `lint-notokenlog` target.
package main

import (
	"golang.org/x/tools/go/analysis/singlechecker"

	"github.com/datuplet/datuplet/tools/lint/notokenlog"
)

func main() {
	singlechecker.Main(notokenlog.Analyzer)
}
