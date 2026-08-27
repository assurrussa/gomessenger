package main

import (
	"errors"
	"testing"
)

func TestServiceExitCodeIncludesShutdownFailure(t *testing.T) {
	t.Parallel()

	failure := errors.New("failure")
	tests := []struct {
		name        string
		serveErr    error
		shutdownErr error
		want        int
	}{
		{name: "clean", want: 0},
		{name: "serve failure", serveErr: failure, want: 1},
		{name: "shutdown failure", shutdownErr: failure, want: 1},
		{name: "combined failure", serveErr: failure, shutdownErr: failure, want: 1},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()
			if got := serviceExitCode(test.serveErr, test.shutdownErr); got != test.want {
				t.Fatalf("serviceExitCode() = %d, want %d", got, test.want)
			}
		})
	}
}
