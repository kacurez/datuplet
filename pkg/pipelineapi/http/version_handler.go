package http

import (
	"net/http"

	"github.com/datuplet/datuplet/pkg/buildinfo"
)

func (s *Server) handleVersion(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]string{
		"version": buildinfo.Version,
		"commit":  buildinfo.Commit,
	})
}
