//go:build !windows

package messenger_test

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

const (
	releaseTestVersion = "v0.99.99"
	releaseE2EModule   = "testdata/e2e"
)

func TestReleasePreparationRejectsUnavailableDependenciesWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		layer   string
		missing string
	}{
		{layer: "root", missing: "github.com/assurrussa/outbox/backends/sqlite@v0.15.0"},
		{layer: "modules", missing: "github.com/assurrussa/gomessenger@" + releaseTestVersion},
		{layer: "transports", missing: "github.com/assurrussa/gomessenger/adapters/inbox@" + releaseTestVersion},
		{layer: "final", missing: "github.com/assurrussa/gomessenger/observability@" + releaseTestVersion},
		{layer: "invalid"},
	} {
		t.Run(test.layer, func(t *testing.T) {
			dir, env := releaseScriptFixture(t)
			before := releaseModuleSnapshot(t, dir)
			env = append(env, "RELEASE_MISSING_MODULE="+test.missing)
			if err := runReleaseScript(t, dir, env, "prepare-release-modules.sh", test.layer); err == nil {
				t.Fatal("preparation succeeded without a valid layer and every prerequisite")
			}
			for name, content := range before {
				if actual := readReleaseFile(t, filepath.Join(dir, name)); actual != content {
					t.Fatalf("failed preflight changed %s", name)
				}
			}
		})
	}
}

func TestReleasePreparationKeepsLaterLayersAndProducesAnIsolatedFinalGraph(t *testing.T) {
	dir, env := releaseScriptFixture(t)
	for _, stage := range []struct {
		layer   string
		changed string
	}{
		{layer: "root", changed: "."},
		{layer: "modules", changed: "adapters/inbox adapters/outbox observability"},
		{layer: "transports", changed: "adapters/kafka adapters/nats"},
		{layer: "final", changed: strings.Join(releaseModuleDirectories(), " ")},
	} {
		t.Run(stage.layer, func(t *testing.T) {
			before := releaseModuleSnapshot(t, dir)
			if err := runReleaseScript(t, dir, env, "prepare-release-modules.sh", stage.layer); err != nil {
				t.Fatalf("prepare %s: %v", stage.layer, err)
			}
			if err := runReleaseScript(t, dir, env, "check-release-modules.sh", stage.layer); err != nil {
				t.Fatalf("check %s: %v", stage.layer, err)
			}
			for _, module := range releaseModuleDirectories() {
				name := filepath.Join(module, "go.mod")
				if !strings.Contains(" "+stage.changed+" ", " "+module+" ") &&
					readReleaseFile(t, filepath.Join(dir, name)) != before[name] {
					t.Fatalf("%s preparation changed later module %s", stage.layer, module)
				}
			}
		})
	}
	for _, module := range releaseModuleDirectories() {
		content := readReleaseFile(t, filepath.Join(dir, module, "go.mod"))
		if module != "." && !strings.Contains(content, "github.com/assurrussa/gomessenger "+releaseTestVersion) {
			t.Errorf("%s did not pin the exact root version", module)
		}
		if module != releaseE2EModule && strings.Contains(content, "replace") {
			t.Errorf("%s still depends on a replacement", module)
		}
		if module == releaseE2EModule && !strings.Contains(content, "replace github.com/assurrussa/gomessenger => ../..") {
			t.Error("E2E fixture lost its checkout replacement")
		}
	}
}

func TestReleaseReadinessRejectsConsumerAndExampleReplacements(t *testing.T) {
	for _, module := range []string{"testdata/consumer", "examples/durable-postgres-nats"} {
		t.Run(module, func(t *testing.T) {
			dir, env := releaseScriptFixture(t)
			if err := runReleaseScript(t, dir, env, "prepare-release-modules.sh", "final"); err != nil {
				t.Fatalf("prepare final graph: %v", err)
			}
			path := filepath.Join(dir, module, "go.mod")
			content := readReleaseFile(t, path) + "\nreplace github.com/assurrussa/gomessenger => ../..\n"
			writeReleaseFile(t, path, content)
			if err := runReleaseScript(t, dir, env, "check-release-modules.sh", "final"); err == nil {
				t.Fatal("readiness accepted a local replacement")
			}
		})
	}
}

func releaseScriptFixture(t *testing.T) (string, []string) {
	t.Helper()
	dir := t.TempDir()
	for _, module := range releaseModuleDirectories() {
		name := filepath.Join(module, "go.mod")
		writeReleaseFile(t, filepath.Join(dir, name), readReleaseFile(t, name))
	}
	for _, name := range []string{"go.work", "scripts/prepare-release-modules.sh", "scripts/check-release-modules.sh"} {
		writeReleaseFile(t, filepath.Join(dir, name), readReleaseFile(t, name))
	}
	realGo, err := exec.LookPath("go")
	if err != nil {
		t.Fatalf("locate Go: %v", err)
	}
	fakeGo := filepath.Join(t.TempDir(), "go")
	writeReleaseFile(t, fakeGo, `#!/bin/sh
set -eu
case "$1 $2" in
  'list -m')
    test "${GOWORK-}" = off
    test "$3" != "${RELEASE_MISSING_MODULE-}"
    ;;
  'mod tidy') test "${GOWORK-}" = off ;;
  *) exec "$RELEASE_REAL_GO" "$@" ;;
esac
`)
	if err := os.Chmod(fakeGo, 0o700); err != nil {
		t.Fatalf("make fake Go executable: %v", err)
	}
	env := append(os.Environ(), "PATH="+filepath.Dir(fakeGo)+string(os.PathListSeparator)+os.Getenv("PATH"),
		"RELEASE_REAL_GO="+realGo, "GOWORK=off")
	return dir, env
}

func releaseModuleDirectories() []string {
	return []string{
		".", "adapters/inbox", "adapters/outbox", "observability", "adapters/kafka", "adapters/nats",
		"tools/gomessengerctl", "testdata/consumer", releaseE2EModule, "examples/durable-postgres-nats",
	}
}

func releaseModuleSnapshot(t *testing.T, dir string) map[string]string {
	t.Helper()
	contents := map[string]string{"go.work": readReleaseFile(t, filepath.Join(dir, "go.work"))}
	for _, module := range releaseModuleDirectories() {
		name := filepath.Join(module, "go.mod")
		contents[name] = readReleaseFile(t, filepath.Join(dir, name))
	}
	return contents
}

func readReleaseFile(t *testing.T, path string) string {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read release fixture: %v", err)
	}
	return string(data)
}

func writeReleaseFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
		t.Fatalf("create release fixture directory: %v", err)
	}
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatalf("write release fixture: %v", err)
	}
}

func runReleaseScript(t *testing.T, dir string, env []string, script, layer string) error {
	t.Helper()
	//nolint:gosec // Test cases select only the two repository release scripts in an isolated fixture.
	command := exec.CommandContext(t.Context(), "sh", filepath.Join("scripts", script), releaseTestVersion, "v0.15.0", layer)
	command.Dir = dir
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		t.Logf("%s %s: %s", script, layer, output)
	}
	return err
}
