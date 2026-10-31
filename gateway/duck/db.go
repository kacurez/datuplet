//go:build duckdb_arrow

package duck

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"

	bindings "github.com/duckdb/duckdb-go-bindings"
)

// Settings are the engine settings applied when the database is opened. A
// zero field leaves DuckDB's own default in place; resolving the gateway
// defaults (threads 1, memory 50 % of the cgroup limit, ...) is the caller's job.
type Settings struct {
	Threads        int
	MemoryLimit    string
	TempDir        string
	MaxTempDirSize string
	ExtensionDir   string
	SecretDir      string
	HomeDir        string
}

// fixedOptions are set on every database regardless of Settings.
var fixedOptions = [][2]string{
	{"preserve_insertion_order", "false"},
	{"enable_external_file_cache", "false"},
	{"read_ahead_depth", "1"},
}

// lockdown is applied by Lock, in order; lock_configuration must stay last.
var lockdown = []string{
	"SET autoinstall_known_extensions=false",
	"SET autoload_known_extensions=false",
	"SET allow_community_extensions=false",
	"SET allow_unredacted_secrets=false",
	"SET lock_configuration=true",
}

// ErrUnusable is returned by every operation after a failed Lock or Close.
var ErrUnusable = errors.New("duck: database is unusable")

// DB is one in-memory DuckDB instance.
type DB struct {
	mu       sync.Mutex
	db       bindings.Database
	closed   bool
	unusable atomic.Bool
}

func (s Settings) options() [][2]string {
	var opts [][2]string
	add := func(name, value string) {
		if value != "" {
			opts = append(opts, [2]string{name, value})
		}
	}
	if s.Threads > 0 {
		add("threads", strconv.Itoa(s.Threads))
	}
	add("memory_limit", s.MemoryLimit)
	add("temp_directory", s.TempDir)
	add("max_temp_directory_size", s.MaxTempDirSize)
	add("extension_directory", s.ExtensionDir)
	add("secret_directory", s.SecretDir)
	add("home_directory", s.HomeDir)
	return append(opts, fixedOptions...)
}

// Open opens an in-memory database with s and the fixed engine options.
func Open(s Settings) (*DB, error) {
	var cfg bindings.Config
	if bindings.CreateConfig(&cfg) == bindings.StateError {
		bindings.DestroyConfig(&cfg)
		return nil, errors.New("duck: create config failed")
	}
	defer bindings.DestroyConfig(&cfg)
	for _, o := range s.options() {
		if bindings.SetConfig(cfg, o[0], o[1]) == bindings.StateError {
			return nil, fmt.Errorf("duck: invalid setting %s=%q", o[0], o[1])
		}
	}
	var db bindings.Database
	var msg string
	if bindings.OpenExt("", &db, cfg, &msg) == bindings.StateError {
		bindings.Close(&db)
		return nil, fmt.Errorf("duck: open: %s", msg)
	}
	return &DB{db: db}, nil
}

// Connect opens a new connection to the database.
func (d *DB) Connect() (*Conn, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed || d.unusable.Load() {
		return nil, ErrUnusable
	}
	var conn bindings.Connection
	if bindings.Connect(d.db, &conn) == bindings.StateError {
		bindings.Disconnect(&conn)
		return nil, errors.New("duck: connect failed")
	}
	return &Conn{db: d, conn: conn}, nil
}

// Lock applies the extension and secret lockdown on c, ending with
// lock_configuration=true, after which no setting can change. Lockdown
// settings are database-global. On error the database is marked unusable:
// Connect and every statement on its connections fail from then on.
func (d *DB) Lock(c *Conn) error {
	if c == nil || c.db != d {
		return errors.New("duck: Lock needs a connection of this database")
	}
	for _, stmt := range lockdown {
		if err := c.Exec(context.Background(), stmt); err != nil {
			d.unusable.Store(true)
			return fmt.Errorf("duck: lockdown %q: %w", stmt, err)
		}
	}
	return nil
}

// Close closes the database. Close every Conn first; DuckDB keeps the
// instance alive while connections remain. Close is idempotent.
func (d *DB) Close() error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.closed {
		return nil
	}
	d.closed = true
	d.unusable.Store(true)
	bindings.Close(&d.db)
	return nil
}
