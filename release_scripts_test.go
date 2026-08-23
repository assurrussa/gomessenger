//go:build !windows

package messenger_test

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestReleaseConsumerProbeRequiresExactTagAndInstallsCLI(t *testing.T) {
	fakeBin := t.TempDir()
	goLog := filepath.Join(t.TempDir(), "go.log")
	fakeGo := filepath.Join(fakeBin, "go")
	if err := os.WriteFile(fakeGo, []byte("#!/bin/sh\nprintf '%s\\n' \"$*\" >> \"$GO_LOG\"\n"), 0o600); err != nil {
		t.Fatalf("write fake go: %v", err)
	}
	if err := os.Chmod(fakeGo, 0o700); err != nil {
		t.Fatalf("make fake go executable: %v", err)
	}
	runProbe := func(version string) error {
		t.Helper()
		//nolint:gosec // Fixed test cases intentionally exercise the local shell script's argument validation.
		command := exec.CommandContext(t.Context(), "sh", filepath.Join("scripts", "test-release-consumer.sh"), version)
		command.Env = append(os.Environ(), "PATH="+fakeBin+string(os.PathListSeparator)+os.Getenv("PATH"), "GO_LOG="+goLog)
		return command.Run()
	}

	if err := runProbe("v1.2.3"); err != nil {
		t.Fatalf("valid release probe: %v", err)
	}
	commands, err := os.ReadFile(goLog)
	if err != nil {
		t.Fatalf("read fake go log: %v", err)
	}
	if !strings.Contains(string(commands),
		"install github.com/assurrussa/gomessenger/tools/gomessengerctl@v1.2.3") {
		t.Fatalf("CLI module was not installed:\n%s", commands)
	}

	for _, version := range []string{"v1.2.3-rc1", "v1foo.2bar.3baz", "v1.2.3.4"} {
		t.Run(version, func(t *testing.T) {
			err := runProbe(version)
			var exitErr *exec.ExitError
			if !errors.As(err, &exitErr) || exitErr.ExitCode() != 2 {
				t.Fatalf("malformed tag error = %v", err)
			}
		})
	}
}
