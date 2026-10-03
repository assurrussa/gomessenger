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
	releaseTestVersion        = "v0.99.99"
	releaseTestOutboxVersion  = "v0.98.98"
	releaseE2EModule          = "testdata/e2e"
	releaseModulesLayer       = "modules"
	releaseTransportsLayer    = "transports"
	releaseOutboxModule       = "github.com/assurrussa/outbox"
	releaseOutboxSQLiteModule = "github.com/assurrussa/outbox/backends/sqlite"
)

func TestReleasePreparationRejectsUnavailableDependenciesWithoutMutation(t *testing.T) {
	for _, test := range []struct {
		layer   string
		missing string
	}{
		{layer: "root", missing: "github.com/assurrussa/outbox/backends/sqlite@v0.15.0"},
		{layer: releaseModulesLayer, missing: "github.com/assurrussa/gomessenger@" + releaseTestVersion},
		{layer: releaseTransportsLayer, missing: "github.com/assurrussa/gomessenger/adapters/inbox@" + releaseTestVersion},
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
		{layer: releaseModulesLayer, changed: "adapters/inbox adapters/outbox observability testdata/e2e"},
		{layer: releaseTransportsLayer, changed: "adapters/kafka adapters/nats testdata/e2e"},
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

func TestReleasePartialLayersAlignOnlyPublishedE2ERequirements(t *testing.T) {
	for _, layer := range []string{releaseModulesLayer, releaseTransportsLayer} {
		t.Run(layer, func(t *testing.T) {
			dir, env := releaseScriptFixture(t)
			path := filepath.Join(dir, releaseE2EModule, "go.mod")
			before := readReleaseFile(t, path)
			if err := runReleaseScriptWithOutbox(t, dir, env, "prepare-release-modules.sh", layer, releaseTestOutboxVersion); err != nil {
				t.Fatalf("prepare %s: %v", layer, err)
			}
			after := readReleaseFile(t, path)
			published := []string{
				"github.com/assurrussa/gomessenger",
				releaseOutboxModule,
				releaseOutboxSQLiteModule,
			}
			if layer == releaseTransportsLayer {
				published = append(published, "github.com/assurrussa/gomessenger/adapters/inbox")
			}
			for _, dependency := range published {
				wanted := releaseTestVersion
				if strings.HasPrefix(dependency, releaseOutboxModule) {
					wanted = releaseTestOutboxVersion
				}
				if !strings.Contains(after, dependency+" "+wanted) {
					t.Fatalf("%s fixture did not align published prerequisite %s", layer, dependency)
				}
				stale := strings.ReplaceAll(after, dependency+" "+wanted, dependency+" v0.0.1")
				writeReleaseFile(t, path, stale)
				if err := runReleaseScriptWithOutbox(t, dir, env, "check-release-modules.sh", layer, releaseTestOutboxVersion); err == nil {
					t.Fatalf("%s readiness accepted stale fixture prerequisite %s", layer, dependency)
				}
				writeReleaseFile(t, path, after)
			}
			assertReleaseFixturePreservedDependencies(t, before, after, published)
		})
	}
}

func assertReleaseFixturePreservedDependencies(t *testing.T, before, after string, published []string) {
	t.Helper()
	for _, line := range strings.Split(before, "\n") {
		trimmed := strings.TrimSpace(line)
		if !strings.HasPrefix(trimmed, "github.com/assurrussa/") && !strings.HasPrefix(trimmed, "replace ") {
			continue
		}
		updated := false
		for _, dependency := range published {
			if strings.HasPrefix(trimmed, dependency+" ") {
				updated = true
			}
		}
		if !updated && !strings.Contains(after, line) {
			t.Fatalf("changed an unready dependency or checkout replacement: %s", trimmed)
		}
	}
}

func TestReleasePartialLayersRemoveAndRejectExternalE2EReplacements(t *testing.T) {
	for _, test := range []struct{ layer, dependency string }{
		{releaseModulesLayer, releaseOutboxModule},
		{releaseModulesLayer, releaseOutboxSQLiteModule},
		{releaseTransportsLayer, releaseOutboxModule},
		{releaseTransportsLayer, releaseOutboxSQLiteModule},
	} {
		for _, version := range []string{"", releaseTestOutboxVersion, "v0.97.97"} {
			t.Run(test.layer+"/"+test.dependency+"@"+version, func(t *testing.T) {
				assertReleaseExternalReplacement(t, test.layer, test.dependency, version)
			})
		}
	}
}

func assertReleaseExternalReplacement(t *testing.T, layer, dependency, version string) {
	t.Helper()
	dir, env := releaseScriptFixture(t)
	path := filepath.Join(dir, releaseE2EModule, "go.mod")
	before := readReleaseFile(t, path)
	qualifier := ""
	if version != "" {
		qualifier = " " + version
	}
	replacement := "\nreplace " + dependency + qualifier + " => ./external-checkout\n"
	writeReleaseFile(t, path, before+replacement)
	err := runReleaseScriptWithOutbox(t, dir, env, "prepare-release-modules.sh", layer, releaseTestOutboxVersion)
	if err != nil {
		t.Fatalf("prepare %s: %v", layer, err)
	}
	after := readReleaseFile(t, path)
	if strings.Contains(after, "replace "+dependency+" ") {
		t.Fatal("preparation retained an external replacement")
	}
	assertReleaseFixturePreservedDependencies(t, before, after, []string{
		"github.com/assurrussa/gomessenger",
		"github.com/assurrussa/gomessenger/adapters/inbox",
		releaseOutboxModule,
		releaseOutboxSQLiteModule,
	})
	writeReleaseFile(t, path, after+replacement)
	err = runReleaseScriptWithOutbox(t, dir, env, "check-release-modules.sh", layer, releaseTestOutboxVersion)
	if err == nil {
		t.Fatal("readiness accepted an external replacement")
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
	return runReleaseScriptWithOutbox(t, dir, env, script, layer, "v0.15.0")
}

func runReleaseScriptWithOutbox(t *testing.T, dir string, env []string, script, layer, outboxVersion string) error {
	t.Helper()
	//nolint:gosec // Test cases select only the two repository release scripts in an isolated fixture.
	command := exec.CommandContext(t.Context(), "sh", filepath.Join("scripts", script), releaseTestVersion, outboxVersion, layer)
	command.Dir = dir
	command.Env = env
	output, err := command.CombinedOutput()
	if err != nil {
		t.Logf("%s %s: %s", script, layer, output)
	}
	return err
}
