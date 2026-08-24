package main

import (
	"testing"

	"github.com/kylin1993/codex-remote/internal/capability"
)

func TestWorkspaceServiceUsesExecutableFrameBoundLimits(t *testing.T) {
	tests := []struct {
		name                       string
		maxFrame                   int
		wantText, wantUp, wantDown uint64
	}{
		{name: "default", maxFrame: 4 << 20, wantText: capability.DefaultMaxTextFileBytes, wantUp: capability.DefaultMaxInlineUploadBytes, wantDown: capability.DefaultMaxInlineDownloadBytes},
		{name: "small blackbox frame", maxFrame: 64 << 10, wantText: 8 << 10, wantUp: 32 << 10, wantDown: 32 << 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			service, err := workspaceServiceForFrame(test.maxFrame)
			if err != nil {
				t.Fatal(err)
			}
			got := service.Capabilities()
			if got.GetMaxTextFileBytes() != test.wantText || got.GetMaxInlineUploadBytes() != test.wantUp || got.GetMaxInlineDownloadBytes() != test.wantDown || got.GetMaxArchiveExpandedBytes() != capability.DefaultMaxArchiveExpandedBytes || got.GetMaxArchiveEntryCount() != capability.DefaultMaxArchiveEntryCount {
				t.Fatalf("workspace limits = %+v", got)
			}
		})
	}
}
