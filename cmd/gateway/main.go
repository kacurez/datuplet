// Package main provides the Data Gateway sidecar binary.
package main

import (
	"os"

	// Blank-import the centralised iceberg-go IO scheme registration
	// package so this binary's `gs://` factory is the Datuplet override
	// regardless of which transitive package registers first. See
	// pkg/datupleticeio/doc.go and RFC 019 §4.5.
	_ "github.com/datuplet/datuplet/pkg/datupleticeio"
)

func main() {
	os.Exit(gatewayMain(os.Args[1:], os.Getenv, os.Stderr))
}
