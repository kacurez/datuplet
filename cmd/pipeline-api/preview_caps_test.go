package main

import (
	"testing"

	"github.com/datuplet/datuplet/pkg/pipelineapi/store"
)

func TestPreviewCapsFromEnv(t *testing.T) {
	for _, tc := range []struct {
		name               string
		workspace, project string
		want               store.PreviewCaps
	}{
		{"defaults", "", "", store.PreviewCaps{RunningPerWorkspace: 2, RunningPerProject: 8, MaxPerProject: 50}},
		{"override", "3", "0", store.PreviewCaps{RunningPerWorkspace: 3, RunningPerProject: 0, MaxPerProject: 50}},
		{"invalid falls back", "two", "-1", store.PreviewCaps{RunningPerWorkspace: 2, RunningPerProject: 8, MaxPerProject: 50}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("DATUPLET_PREVIEW_MAX_RUNNING_PER_WORKSPACE", tc.workspace)
			t.Setenv("DATUPLET_PREVIEW_MAX_RUNNING_PER_PROJECT", tc.project)
			t.Setenv("DATUPLET_PREVIEW_MAX_PER_PROJECT", "")
			if got := previewCapsFromEnv(); got != tc.want {
				t.Errorf("caps = %+v, want %+v", got, tc.want)
			}
		})
	}
}
