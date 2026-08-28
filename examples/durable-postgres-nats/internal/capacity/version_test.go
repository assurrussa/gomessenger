//nolint:testpackage // Tests exercise package-local build metadata normalization.
package capacity

import (
	"runtime/debug"
	"testing"
)

func TestDependencyVersionReportsPublishedAndLocalReplacementBuilds(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name string
		deps []*debug.Module
		want string
	}{
		{
			name: "published",
			deps: []*debug.Module{{Path: outboxModulePath, Version: "v0.12.0"}},
			want: "v0.12.0",
		},
		{
			name: "versioned replacement",
			deps: []*debug.Module{{
				Path: outboxModulePath, Version: "v0.11.0",
				Replace: &debug.Module{Path: "github.com/example/outbox", Version: "v0.12.0"},
			}},
			want: "v0.12.0",
		},
		{
			name: "local replacement",
			deps: []*debug.Module{{
				Path: outboxModulePath, Version: "v0.11.0",
				Replace: &debug.Module{Path: "../outbox"},
			}},
			want: "devel (local replace)",
		},
		{name: "missing", want: "unknown"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := dependencyVersion(test.deps, outboxModulePath); got != test.want {
				t.Fatalf("dependency version = %q, want %q", got, test.want)
			}
		})
	}
}
