//go:build duckdb_arrow

// DuckDB 2.0 Quack surface: the deny proof (spec D5 in
// docs/superpowers/specs/2026-10-28-duckdb-2-upgrade-design.md).
//
// DuckDB 2.0 adds a remote protocol (Quack) whose extension autoloads on first
// use. The lockdown posture keeps it out on two layers: quack and
// postgres_scanner are not statically linked (TestStaticallyLinkedExtensions
// pins that), and with autoinstall/autoload off and LocalFileSystem disabled
// nothing can bring them in. Every row must error AND the error must name the
// layer that refused it (denyClass); a loopback listener proves no statement
// opened a socket.
//
// RE-RUN ON EVERY duckdb-go VERSION BUMP, like the matrix in
// lockdown_deny_test.go.
package queryengine

import (
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strconv"
	"strings"
	"testing"
	"time"
)

// denyClass names the layer a Quack-surface statement must be refused at
// (spec D5). A row passes only if its error satisfies the class predicate;
// matching the protocol name alone is not a proof.
type denyClass int

const (
	classFSLayer              denyClass = iota // prefix-routed extension-file load refused by disabled_filesystems
	classExtensionUnavailable                  // extension not loaded and cannot be autoloaded/installed
	classSecretTypeAbsent                      // CREATE SECRET with an unregistered TYPE
	classConfigLocked                          // lock_configuration refused a SET
)

// rejectRe: an error matching any of these is a transport/credential/parse
// failure, never a posture deny, whatever else it says. No bare "connect":
// a diagnostic may echo the CONNECT statement without opening a socket; the
// listener is the network proof.
var rejectRe = regexp.MustCompile(`(?i)(connection refused|could not connect|failed to connect|connection reset|timed out|unauthori[sz]ed|invalid token|parser error|syntax error)`)

// setNameRe extracts the setting name from a classConfigLocked row's stmt.
var setNameRe = regexp.MustCompile(`(?i)^SET\s+([a-z_][a-z0-9_]*)\s*=`)

// classRe: verbatim phrases from the spike report (## Quack probe).
// classExtensionUnavailable is deliberately absent: its class phrase is bound
// to the row's subject (subjectExtensionRe), so another extension's name
// cannot satisfy it.
var classRe = map[denyClass]*regexp.Regexp{
	classFSLayer:          regexp.MustCompile(`(?i)LocalFileSystem has been disabled`),
	classSecretTypeAbsent: regexp.MustCompile(`(?i)secret type 'quack' does not exist`),
	classConfigLocked:     regexp.MustCompile(`(?i)the configuration has been locked`),
}

// subjectExtensionRe matches DuckDB's not-loaded text for an object that lives
// in the subject extension. The subject is quoted into the pattern rather than
// searched for on its own, because the error may echo the statement (which
// names quack_query) and so contain "quack" whatever extension it blames.
func subjectExtensionRe(subject string) *regexp.Regexp {
	return regexp.MustCompile(`(?i)but it exists in the ` + regexp.QuoteMeta(subject) + ` extension`)
}

type quackProbe struct {
	name, stmt string
	class      denyClass
	subject    string // extension the error must say holds the missing object ("" = none; fsLayer text never names it)
	names      string // verbatim phrase the error must also contain, naming the refused object ("" = none)
	warmOnly   bool   // only on lockedEngineViaAttach (setting exists only with httpfs loaded)
}

var quackProbes = []quackProbe{
	{"attach-token", "ATTACH 'quack:127.0.0.1:<port>' AS q (TOKEN 'x')", classFSLayer, "", "", false},
	{"attach-type-quack", "ATTACH 'quack:127.0.0.1:<port>' AS q (TYPE quack)", classFSLayer, "", "", false},
	{"connect-quack", "CONNECT 'quack:127.0.0.1:<port>'", classFSLayer, "", "", false},
	{"connect-postgres", "CONNECT 'postgres:host=127.0.0.1 port=<port>'", classFSLayer, "", "", false},
	{"quack-query", "SELECT * FROM quack_query('quack:127.0.0.1:<port>', 'SELECT 1', token = 'x')", classExtensionUnavailable, "quack", `Table Function with name "quack_query" is not in the catalog`, false},
	{"secret-type-quack", "CREATE SECRET qs (TYPE quack, TOKEN 'x', SCOPE 'quack:127.0.0.1')", classSecretTypeAbsent, "quack", "", false},
	{"set-s3-deprecated-url-params", "SET s3_enable_deprecated_url_parameters = true", classConfigLocked, "", "", true},
}

// denied reports whether err proves the row's class: err != nil,
// !rejectRe.Match, the class marker matches (for classExtensionUnavailable,
// the subject-bound phrase), subjectExtensionRe(subject) matches when subject
// is set, and the error contains names when set. why says which condition
// failed.
func denied(err error, p quackProbe) (ok bool, why string) {
	if err == nil {
		return false, "statement SUCCEEDED"
	}
	msg := err.Error()
	if m := rejectRe.FindString(msg); m != "" {
		return false, fmt.Sprintf("transport/credential/parse failure (%q), not a posture deny", m)
	}
	switch re, found := classRe[p.class]; {
	case found:
		if !re.MatchString(msg) {
			return false, fmt.Sprintf("error lacks the class marker %s", re)
		}
	case p.class == classExtensionUnavailable:
		if p.subject == "" {
			return false, "extensionUnavailable row has no subject to bind its class phrase to"
		}
	default:
		return false, fmt.Sprintf("no class marker for class %d", p.class)
	}
	if p.subject != "" {
		if re := subjectExtensionRe(p.subject); !re.MatchString(msg) {
			return false, fmt.Sprintf("error lacks %s", re)
		}
	}
	if p.names != "" && !strings.Contains(msg, p.names) {
		return false, fmt.Sprintf("error does not contain %q", p.names)
	}
	return true, ""
}

// quackListener owns one loopback *net.TCPListener per subtest. No accept
// goroutine: drain is the barrier. A completed TCP handshake waits in the
// kernel accept queue until Accept is called, so draining AFTER the statement
// returned sees every connection the statement opened; closing first could
// discard the backlog.
type quackListener struct{ ln *net.TCPListener }

func newQuackListener(t *testing.T) *quackListener {
	t.Helper()
	ln, err := net.ListenTCP("tcp", &net.TCPAddr{IP: net.IPv4(127, 0, 0, 1)})
	if err != nil {
		t.Fatalf("listen on loopback: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	return &quackListener{ln: ln}
}

func (l *quackListener) Port() string {
	return strconv.Itoa(l.ln.Addr().(*net.TCPAddr).Port)
}

// drain: SetDeadline(now+d), then Accept in a loop (close each conn, count)
// until the deadline error; returns the count. Any other Accept error means
// the barrier never completed, so no count would be a proof: it panics.
func (l *quackListener) drain(d time.Duration) int {
	if err := l.ln.SetDeadline(time.Now().Add(d)); err != nil {
		panic(fmt.Sprintf("quackListener.drain: SetDeadline: %v", err))
	}
	n := 0
	for {
		c, err := l.ln.Accept()
		if err != nil {
			var ne net.Error
			if errors.As(err, &ne) && ne.Timeout() {
				return n
			}
			panic(fmt.Sprintf("quackListener.drain: Accept: %v", err))
		}
		_ = c.Close()
		n++
	}
}

// execQuackProbe runs p's statement, aimed at l, under the 10 s probe timeout.
func execQuackProbe(e *engine, p quackProbe, l *quackListener) error {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	_, err := e.conn.ExecContext(ctx, strings.ReplaceAll(p.stmt, "<port>", l.Port()))
	return err
}

// layerNamingEngine: openEngine + the extension posture only
// (autoinstall_known_extensions=false, autoload_known_extensions=false,
// allow_community_extensions=false), NO disabled_filesystems, NO lock();
// SET extension_directory = t.TempDir() (empty, so a developer's locally
// installed quack cannot change the outcome); t.Chdir(t.TempDir()) so a stray
// local DB file would be visible.
func layerNamingEngine(t *testing.T) *engine {
	t.Helper()
	ctx := context.Background()
	t.Chdir(t.TempDir())
	e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
	if err != nil {
		t.Fatalf("openEngine: %v", err)
	}
	t.Cleanup(func() { _ = e.Close() })
	for _, s := range []string{
		fmt.Sprintf("SET extension_directory = '%s'", escapeSQL(t.TempDir())),
		`SET autoinstall_known_extensions=false`,
		`SET autoload_known_extensions=false`,
		`SET allow_community_extensions=false`,
	} {
		if _, err := e.conn.ExecContext(ctx, s); err != nil {
			t.Fatalf("extension posture %q: %v", s, err)
		}
	}
	return e
}

// TestLockdownDenyQuack proves every Quack entry point is refused post-lock,
// at a named layer, without a socket ever being opened. A row that SUCCEEDS,
// fails for a transport/credential/parse reason, or reaches the listener is a
// security finding to escalate, never a regex to loosen.
func TestLockdownDenyQuack(t *testing.T) {
	// Negative control for the barrier: one dial must surface as exactly one
	// drained connection, or a drain of zero proves nothing.
	t.Run("listener-detects-connection", func(t *testing.T) {
		l := newQuackListener(t)
		c, err := net.Dial("tcp", "127.0.0.1:"+l.Port())
		if err != nil {
			t.Fatalf("dial own listener: %v", err)
		}
		defer c.Close()
		if n := l.drain(200 * time.Millisecond); n != 1 {
			t.Fatalf("drain counted %d connection(s) after one dial; want 1", n)
		}
	})

	// The fsLayer rows deny with a LocalFileSystem text that never names the
	// extension. Here the FS is enabled and only the extension posture is on,
	// so the same statements surface the layer underneath: DuckDB maps the
	// quack:/postgres: prefix to its extension and tries an explicit load of
	// the extension file, which is absent (not linked, not installed). Line 1 of
	// the error names the extension-directory path, so only line 2 is matched.
	t.Run("layer-naming", func(t *testing.T) {
		wantLine2 := map[string]string{
			"attach-token":      `Extension "quack" is an existing extension`,
			"attach-type-quack": `Extension "quack" is an existing extension`,
			"connect-quack":     `Extension "quack" is an existing extension`,
			"connect-postgres":  `Extension "postgres_scanner" is an existing extension`,
		}
		e := layerNamingEngine(t)
		cwd, err := os.Getwd()
		if err != nil {
			t.Fatalf("getwd: %v", err)
		}
		l := newQuackListener(t)
		for _, p := range quackProbes {
			if p.class != classFSLayer {
				continue
			}
			t.Run(p.name, func(t *testing.T) {
				want, ok := wantLine2[p.name]
				if !ok {
					t.Fatalf("fsLayer row %q has no layer-naming text pinned", p.name)
				}
				err := execQuackProbe(e, p, l)
				n := l.drain(200 * time.Millisecond)
				t.Logf("%s → %v", p.name, err)
				if err == nil {
					t.Errorf("SECURITY: %q SUCCEEDED on the extension-posture engine; want the extension-load error", p.name)
				} else if lines := strings.Split(err.Error(), "\n"); len(lines) < 2 || !strings.Contains(lines[1], want) {
					t.Errorf("%q: line 2 of the error must contain %q; got: %v", p.name, want, err)
				}
				if n != 0 {
					t.Errorf("SECURITY: %q opened %d connection(s) to the loopback listener", p.name, n)
				}
				entries, rerr := os.ReadDir(cwd)
				if rerr != nil {
					t.Fatalf("ReadDir(cwd): %v", rerr)
				}
				if len(entries) != 0 {
					t.Errorf("SECURITY: %q left %d file(s) in the cwd (stray local DB?)", p.name, len(entries))
				}
			})
		}
	})

	runRows := func(t *testing.T, e *engine, withWarmOnly bool) {
		l := newQuackListener(t)
		for _, p := range quackProbes {
			if p.warmOnly && !withWarmOnly {
				continue
			}
			t.Run(p.name, func(t *testing.T) {
				err := execQuackProbe(e, p, l)
				n := l.drain(200 * time.Millisecond)
				t.Logf("%s → %v", p.name, err)
				if ok, why := denied(err, p); !ok {
					t.Errorf("SECURITY: %q is not a proven deny: %s\n  stmt: %s\n  err: %v", p.name, why, p.stmt, err)
				}
				if n != 0 {
					t.Errorf("SECURITY: %q opened %d connection(s) to the loopback listener", p.name, n)
				}
			})
		}
	}

	// lock_configuration refuses a SET before resolving the setting name, so a
	// classConfigLocked row stays green on a typo or a renamed setting. Each
	// such row must therefore name a setting that exists in the warmed state
	// (httpfs loaded, which is where the S3 settings come from), checked
	// pre-lock where duckdb_settings() still answers.
	t.Run("config-locked-settings-exist", func(t *testing.T) {
		ctx := context.Background()
		e, err := openEngine(ctx, Request{TempDir: t.TempDir()})
		if err != nil {
			t.Fatalf("openEngine: %v", err)
		}
		t.Cleanup(func() { _ = e.Close() })
		requireIcebergExtensions(t, e)
		checked := 0
		for _, p := range quackProbes {
			if p.class != classConfigLocked {
				continue
			}
			m := setNameRe.FindStringSubmatch(p.stmt)
			if m == nil {
				t.Errorf("configLocked row %q: stmt %q is not SET <name> = ...; the existence check cannot cover it", p.name, p.stmt)
				continue
			}
			var n int
			if err := e.conn.QueryRowContext(ctx,
				`SELECT count(*) FROM duckdb_settings() WHERE name = ?`, m[1]).Scan(&n); err != nil {
				t.Fatalf("duckdb_settings() lookup for %q: %v", m[1], err)
			}
			if n != 1 {
				t.Errorf("configLocked row %q names setting %q, which duckdb_settings() lists %d time(s) with httpfs loaded; "+
					"the lock error would not prove the setting is frozen", p.name, m[1], n)
			}
			checked++
		}
		if checked == 0 {
			t.Fatal("no classConfigLocked row was checked")
		}
	})

	// Cold: lock() with nothing loaded. warmOnly rows do not run here: without
	// httpfs their setting does not exist, so a lock error would not prove it.
	t.Run("cold", func(t *testing.T) {
		e, _ := lockedEngine(t)
		runRows(t, e, false)
	})

	// Warmed: the production pre-lock state (httpfs + iceberg loaded, secret
	// manager initialised). Mandatory at the gate via QE_REQUIRE_WARMED=1.
	t.Run("warmed", func(t *testing.T) {
		e, _ := lockedEngineViaAttach(t)
		runRows(t, e, true)
	})
}

// TestDeniedPredicate pins the predicate itself on synthetic errors: it must
// accept each row's recorded 2.0.0 deny (first line, verbatim from the spike
// report's ## Quack probe) and reject a success, a transport failure (also one
// that carries the row's class marker, which only rejectRe can catch), and a
// diagnostic that blames the wrong extension or names the wrong object.
func TestDeniedPredicate(t *testing.T) {
	const fsLayerText = `Permission Error: File system LocalFileSystem has been disabled by configuration`
	spikeFirstLine := map[string]string{
		"attach-token":                 fsLayerText,
		"attach-type-quack":            fsLayerText,
		"connect-quack":                fsLayerText,
		"connect-postgres":             fsLayerText,
		"quack-query":                  `Catalog Error: Table Function with name "quack_query" is not in the catalog, but it exists in the quack extension.`,
		"secret-type-quack":            `Invalid Input Error: Secret type 'quack' does not exist, but it exists in the quack extension.`,
		"set-s3-deprecated-url-params": `Invalid Input Error: Cannot change configuration option ""s3_enable_deprecated_url_parameters"" - the configuration has been locked`,
	}
	const transportText = `IO Error: Could not connect to server error for HTTP HEAD to 'http://127.0.0.1:1/x.parquet'`
	// transportText carries no class marker, so reject-transport passes on the
	// class check alone. reject-marker-plus-transport keeps the class marker and
	// adds a transport phrase, so only the rejectRe check can reject it.
	const transportPhrase = `Could not connect to server`

	type predicateCase struct {
		name, row string
		err       error
		accept    bool
	}
	var cases []predicateCase
	for _, p := range quackProbes {
		line, ok := spikeFirstLine[p.name]
		if !ok {
			t.Fatalf("row %q has no recorded spike text", p.name)
		}
		cases = append(cases,
			predicateCase{"accept-spike-text/" + p.name, p.name, errors.New(line), true},
			predicateCase{"reject-nil/" + p.name, p.name, nil, false},
			predicateCase{"reject-transport/" + p.name, p.name, errors.New(transportText), false},
			predicateCase{"reject-marker-plus-transport/" + p.name, p.name, errors.New(line + " " + transportPhrase), false},
		)
	}
	cases = append(cases,
		predicateCase{"reject-wrong-extension/quack-query", "quack-query",
			errors.New(`Catalog Error: Table Function with name "quack_query" is not in the catalog, but it exists in the httpfs extension.`), false},
		predicateCase{"reject-secret-type-text/quack-query", "quack-query",
			errors.New(spikeFirstLine["secret-type-quack"]), false},
		predicateCase{"reject-fs-layer-text/quack-query", "quack-query",
			errors.New(fsLayerText), false},
	)

	rows := make(map[string]quackProbe, len(quackProbes))
	for _, p := range quackProbes {
		rows[p.name] = p
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			ok, why := denied(c.err, rows[c.row])
			if ok != c.accept {
				t.Errorf("denied = %v (%s), want %v\n  err: %v", ok, why, c.accept, c.err)
			}
		})
	}
}
