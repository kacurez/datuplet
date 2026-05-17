package http_test

import (
	"math"
	"os"
	"path/filepath"
	"regexp"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// TestUIContrastRatios parses ui/product/style.css, extracts --bg-{0,1,2},
// --status-{ok,fail,running,pending}-bg, and --status-{ok,fail,running,
// pending}-fg hex values, computes the contrast between each status-fg
// and the ACTUAL mixed pill background — color-mix(in srgb,
// --status-*-bg 15%, surface) — and fails if any ratio < 4.5.
//
// Note: raw-surface comparisons are NOT a conservative underestimate.
// For dark-mode where --status-*-bg is lighter than the surface, the
// 15% mix produces a LIGHTER resulting background, which REDUCES
// contrast against a light --status-*-fg. We therefore compute the
// mixed background explicitly (see mixSRGB) and run contrast against
// that.
func TestUIContrastRatios(t *testing.T) {
	cssPath := findStyleCSS(t)
	data, err := os.ReadFile(cssPath)
	if err != nil {
		t.Fatal(err)
	}
	css := string(data)

	bg0 := mustColor(t, css, "--bg-0")
	bg1 := mustColor(t, css, "--bg-1")
	bg2 := mustColor(t, css, "--bg-2")
	statuses := []string{"ok", "fail", "running", "pending"}
	const minRatio = 4.5

	bgs := []struct {
		name string
		rgb  [3]float64
	}{
		{"bg-0", bg0}, {"bg-1", bg1}, {"bg-2", bg2},
	}

	for _, s := range statuses {
		fg := mustColor(t, css, "--status-"+s+"-fg")
		statusBg := mustColor(t, css, "--status-"+s+"-bg")
		for _, bg := range bgs {
			mixed := mixSRGB(statusBg, bg.rgb, 0.15)
			r := contrastRatio(fg, mixed)
			if r < minRatio {
				t.Errorf("status %s fg on pill-over-%s (mixed): %.2f < %.2f",
					s, bg.name, r, minRatio)
			} else {
				t.Logf("status %-7s fg on pill-over-%s (mixed): %.2f", s, bg.name, r)
			}
		}
	}
}

// mixSRGB computes a linear sRGB channel mix equivalent to CSS
// color-mix(in srgb, topColor alpha%, bottomColor). alpha is 0..1.
// Values are still in sRGB space (no gamma conversion needed —
// color-mix operates on sRGB components directly).
func mixSRGB(top, bottom [3]float64, alpha float64) [3]float64 {
	return [3]float64{
		alpha*top[0] + (1-alpha)*bottom[0],
		alpha*top[1] + (1-alpha)*bottom[1],
		alpha*top[2] + (1-alpha)*bottom[2],
	}
}

// findStyleCSS returns the absolute path to ui/product/style.css by
// walking up from this file's directory.
func findStyleCSS(t *testing.T) string {
	t.Helper()
	_, self, _, _ := runtime.Caller(0)
	dir := filepath.Dir(self)
	for i := 0; i < 10; i++ {
		cand := filepath.Join(dir, "ui", "product", "style.css")
		if _, err := os.Stat(cand); err == nil {
			return cand
		}
		dir = filepath.Dir(dir)
	}
	t.Fatalf("could not locate ui/product/style.css from %s", self)
	return ""
}

func mustColor(t *testing.T, css, tokenName string) [3]float64 {
	t.Helper()
	// Find the line "--tokenName: #xxxxxx;" (or similar) inside :root{}.
	// Keep parsing simple and pragmatic — the file is hand-written.
	rx := regexp.MustCompile(`(?m)` + regexp.QuoteMeta(tokenName) + `\s*:\s*(#[0-9a-fA-F]{6})`)
	m := rx.FindStringSubmatch(css)
	if m == nil {
		t.Fatalf("token %s not found", tokenName)
	}
	return parseHex(m[1])
}

func parseHex(s string) [3]float64 {
	s = strings.TrimPrefix(s, "#")
	r, _ := strconv.ParseInt(s[0:2], 16, 32)
	g, _ := strconv.ParseInt(s[2:4], 16, 32)
	b, _ := strconv.ParseInt(s[4:6], 16, 32)
	return [3]float64{float64(r) / 255, float64(g) / 255, float64(b) / 255}
}

// luminance returns WCAG 2.1 relative luminance for an sRGB colour.
func luminance(c [3]float64) float64 {
	toLin := func(v float64) float64 {
		if v <= 0.03928 {
			return v / 12.92
		}
		return math.Pow((v+0.055)/1.055, 2.4)
	}
	return 0.2126*toLin(c[0]) + 0.7152*toLin(c[1]) + 0.0722*toLin(c[2])
}

func contrastRatio(fg, bg [3]float64) float64 {
	lf, lb := luminance(fg), luminance(bg)
	if lf < lb {
		lf, lb = lb, lf
	}
	return (lf + 0.05) / (lb + 0.05)
}
