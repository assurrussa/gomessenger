package main

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestReadStrictJSONPathContract(t *testing.T) {
	directory := t.TempDir()
	workingDirectory := filepath.Join(directory, "work")
	if err := os.Mkdir(workingDirectory, 0o700); err != nil {
		t.Fatal(err)
	}
	t.Chdir(workingDirectory)

	inputs := []struct {
		name    string
		data    string
		wantErr string
	}{
		{name: "valid", data: `{"value":"selected"}`},
		{name: "unknown", data: `{"value":"selected","unknown":true}`, wantErr: "unknown field"},
		{name: "trailing", data: `{"value":"selected"} {"value":"second"}`, wantErr: "trailing JSON value"},
	}
	for _, input := range inputs {
		name := input.name + ".json"
		for _, root := range []string{directory, workingDirectory} {
			if err := os.WriteFile(filepath.Join(root, name), []byte(input.data), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		paths := []struct {
			name string
			path string
		}{
			{name: "absolute", path: filepath.Join(workingDirectory, name)},
			{name: "relative", path: name},
			{name: "parent", path: filepath.Join("..", name)},
		}
		for _, path := range paths {
			t.Run(input.name+"/"+path.name, func(t *testing.T) {
				var result struct {
					Value string `json:"value"`
				}
				err := readStrictJSON(path.path, &result)
				if input.wantErr != "" {
					if err == nil || !strings.Contains(err.Error(), input.wantErr) {
						t.Fatalf("error = %v, want %q", err, input.wantErr)
					}
					return
				}
				if err != nil || result.Value != "selected" {
					t.Fatalf("value = %q, error = %v", result.Value, err)
				}
			})
		}
	}
}
