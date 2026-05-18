// Package oauth2 is a fake stub used only by the notokenlog analyzer's
// testdata. The real package lives at golang.org/x/oauth2.
package oauth2

import "time"

// Token mirrors the shape of the real oauth2.Token. The notokenlog analyzer
// flags any formatter/logger call that takes a Token (value or pointer).
type Token struct {
	AccessToken string
	TokenType   string
	Expiry      time.Time
}
