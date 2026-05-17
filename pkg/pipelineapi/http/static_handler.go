package http

import (
	"net/http"
	"os"
	"path/filepath"
	"strings"
)

// WithStaticDir mounts the UI's static directory at /ui/*. When unset,
// /ui/ returns 404. The directory layout is ui/product/ in the repo;
// the Dockerfile.pipeline-api COPYs it under /app/ui/product and the
// deployment sets PIPELINE_API_UI_DIR accordingly (see docs/pipeline-api.md).
func (s *Server) WithStaticDir(dir string) *Server {
	s.staticDir = dir
	return s
}

// staticHandler serves requests under /ui/*.
//
// Behavior:
//   - Existing file under staticDir: served verbatim via FileServer so
//     MIME types and byte ranges work correctly.
//   - Missing file or any directory path: falls back to index.html so
//     the vanilla-ES-modules SPA can handle /ui/pipelines/foo-style deep
//     links via the History API.
//
// Traversal guard: the path rooted at staticDir after filepath.Clean
// must stay inside staticDir. http.FileServer has its own guard but we
// re-check because we also touch the filesystem via os.Stat before
// deciding which branch to take.
func (s *Server) staticHandler() http.HandlerFunc {
	fs := http.FileServer(http.Dir(s.staticDir))
	// Resolve staticDir once so the membership check below compares
	// absolute paths and isn't fooled by symlinks mid-string.
	root, err := filepath.Abs(s.staticDir)
	if err != nil {
		root = s.staticDir
	}
	return func(w http.ResponseWriter, r *http.Request) {
		rel := strings.TrimPrefix(r.URL.Path, "/ui")
		rel = strings.TrimPrefix(rel, "/")

		// Empty path → serve index.html (root of the SPA).
		if rel == "" {
			http.ServeFile(w, r, filepath.Join(s.staticDir, "index.html"))
			return
		}

		// Traversal check: candidate must live under root. Use a
		// directory-boundary test (root == candidate OR candidate
		// starts with root + separator) — a plain HasPrefix would
		// accept sibling directories whose names happen to share the
		// same string prefix (e.g. root=/app/ui/product and
		// candidate=/app/ui/product2/x).
		candidate, err := filepath.Abs(filepath.Join(root, filepath.Clean(rel)))
		if err != nil {
			http.NotFound(w, r)
			return
		}
		rootWithSep := root
		if !strings.HasSuffix(rootWithSep, string(filepath.Separator)) {
			rootWithSep += string(filepath.Separator)
		}
		if candidate != root && !strings.HasPrefix(candidate, rootWithSep) {
			http.NotFound(w, r)
			return
		}

		info, statErr := os.Stat(candidate)
		if statErr == nil && !info.IsDir() {
			// File exists — let FileServer handle Content-Type, range
			// headers, caching. StripPrefix /ui so the FileServer sees
			// paths relative to root.
			http.StripPrefix("/ui/", fs).ServeHTTP(w, r)
			return
		}
		// Missing or a directory: SPA fallback. The client-side router
		// will interpret the pathname and render the right page.
		http.ServeFile(w, r, filepath.Join(s.staticDir, "index.html"))
	}
}
