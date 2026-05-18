// Package a is the primary testdata for the notokenlog analyzer.
// Each "want" trailing-comment asserts the analyzer reports a diagnostic
// at that line. The analyzer uses Option A semantics: ANY seed-type
// argument to a formatter/logger is flagged, regardless of verb.
package a

import (
	"fmt"
	"log"

	"github.com/datuplet/datuplet/pkg/catalogwriter"
	"golang.org/x/oauth2"
)

// --- GCSCreds (value receiver) ----------------------------------------------

func gcsCredsValueBadPrintf(c catalogwriter.GCSCreds) {
	fmt.Printf("creds: %v", c) // want `bearer-credential type`
}

func gcsCredsValueBadSprintf(c catalogwriter.GCSCreds) string {
	return fmt.Sprintf("creds: %+v", c) // want `bearer-credential type`
}

func gcsCredsValueBadErrorf(c catalogwriter.GCSCreds) error {
	return fmt.Errorf("creds: %#v", c) // want `bearer-credential type`
}

func gcsCredsValueBadLogPrintf(c catalogwriter.GCSCreds) {
	log.Printf("creds: %v", c) // want `bearer-credential type`
}

// gcsCredsValueBadEvenWithPercentT — Option A flags %T too. The fix is to
// format c.Type() (a CredsType string) or write a literal type name.
func gcsCredsValueBadEvenWithPercentT(c catalogwriter.GCSCreds) string {
	return fmt.Sprintf("got %T", c) // want `bearer-credential type`
}

// --- *GCSCreds (pointer receiver) ------------------------------------------

func gcsCredsPointerBad(c *catalogwriter.GCSCreds) {
	fmt.Printf("creds: %v", c) // want `bearer-credential type`
}

// --- oauth2.Token (value + pointer) ----------------------------------------

func oauth2TokenValueBad(t oauth2.Token) {
	fmt.Printf("tok: %v", t) // want `bearer-credential type`
}

func oauth2TokenPointerBad(t *oauth2.Token) {
	log.Println("tok:", t) // want `bearer-credential type`
}

// --- Good cases (no diagnostic expected) -----------------------------------

// goodFormatTypeString uses c.Type() — a string-alias CredsType — not the
// credential value. Safe; should NOT trigger.
func goodFormatTypeString(c catalogwriter.GCSCreds) string {
	return fmt.Sprintf("creds family: %s", c.Type())
}

// goodFormatLiteralType passes a literal string of the type name. Safe.
func goodFormatLiteralType() string {
	return fmt.Sprintf("got %s", "catalogwriter.GCSCreds")
}

// goodFormatNonCredsType — formatter receives a plain string. Safe.
func goodFormatNonCredsType(s string) {
	fmt.Printf("hello %s", s)
}

// goodNonFormatterUse — passing a Creds value to a non-formatter is fine.
// The analyzer's scope is formatters/loggers only.
func goodNonFormatterUse(c catalogwriter.GCSCreds) catalogwriter.CredsType {
	return c.Type()
}
