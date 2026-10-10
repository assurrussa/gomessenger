package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	messenger "github.com/assurrussa/gomessenger"
)

const (
	cmdSchema                  = "schema"
	cmdSchemaCheck             = "check"
	flagSchemaManifest         = "--manifest"
	flagSchemaBaseline         = "--baseline"
	flagSchemaBaselineSHA256   = "--baseline-sha256"
	schemaImmutableError       = "immutable schema document changed"
	schemaFixturePath          = "testdata/schema/baseline.json"
	schemaGoldenBaselineSHA256 = "ed6467e74ea1fd496721d1d71e0f5a82592302d10afec2b5b4c035af36c5bb56"
	schemaGoldenFingerprint    = "05a8b3cd9ea49d734240e3acd6daea223fe78615424c77be55265127872cb57e"
)

func TestSchemaCheckGolden(t *testing.T) {
	baseline := readSchemaFixture(t)
	pin := fmt.Sprintf("%x", sha256.Sum256(baseline))
	var stdout, stderr bytes.Buffer
	code := run([]string{
		cmdSchema, cmdSchemaCheck, flagSchemaManifest, "testdata/schema/manifest.json",
		flagFile, schemaFixturePath, flagSchemaBaseline, schemaFixturePath, flagSchemaBaselineSHA256, pin,
	}, &stdout, &stderr)
	if code != exitOK {
		t.Fatalf("code=%d stderr=%s", code, stderr.String())
	}
	var result schemaCheckReport
	if err := json.Unmarshal(stdout.Bytes(), &result); err != nil {
		t.Fatal(err)
	}
	if result.Policy != schemaPolicy || result.BaselineSHA256 != pin || result.Preserved != 1 ||
		result.Added != 0 || result.SamplesValidated != 2 || result.SkippedNonJSON != 0 || len(result.Fingerprints) != 1 {
		t.Fatalf("unexpected report: %+v", result)
	}
	if got := result.Fingerprints[0]; got.Descriptor != "event/media.processed/v1" || got.SHA256 != schemaGoldenFingerprint {
		t.Fatalf("fingerprint: %+v", got)
	}
	if pin != schemaGoldenBaselineSHA256 {
		t.Fatalf("baseline bytes changed: %s", pin)
	}
}

func TestSchemaCatalogImmutability(t *testing.T) {
	tests := []struct {
		name   string
		change func(*schemaCatalog)
		want   string
	}{
		{"metadata", func(c *schemaCatalog) { c.Descriptors[0].Schema = "urn:changed" }, "metadata changed"},
		{"type", func(c *schemaCatalog) {
			c.Descriptors[0].Document = bytes.Replace(c.Descriptors[0].Document, []byte(`"type": "string"`),
				[]byte(`"type": "number"`), 1)
		}, schemaImmutableError},
		{"required", func(c *schemaCatalog) {
			c.Descriptors[0].Document = bytes.Replace(c.Descriptors[0].Document, []byte(`"mediaId", "status"`),
				[]byte(`"mediaId", "status", "sizes"`), 1)
		}, schemaImmutableError},
		{"annotations", func(c *schemaCatalog) {
			c.Descriptors[0].Document = bytes.Replace(c.Descriptors[0].Document, []byte(`"type": "object"`),
				[]byte(`"title": "Documentation edit", "type": "object"`), 1)
		}, schemaImmutableError},
		{"removed old version", func(c *schemaCatalog) { c.Descriptors[0].SchemaVersion = 2 }, "baseline version removed"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			baseline, err := decodeSchemaCatalog(readSchemaFixture(t))
			if err != nil {
				t.Fatal(err)
			}
			catalog := schemaFixtureCatalog(t)
			catalog.Descriptors[0].Samples = nil
			test.change(&catalog)
			candidate, err := decodeSchemaCatalog(marshalSchemaTest(t, catalog))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := compareSchemaCatalogs(candidate, baseline); err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("want %q, got %v", test.want, err)
			}
		})
	}
}

func TestSchemaCatalogAllowsRetainedAndNewVersions(t *testing.T) {
	baseline, err := decodeSchemaCatalog(readSchemaFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	catalog := schemaFixtureCatalog(t)
	newVersion := catalog.Descriptors[0]
	newVersion.SchemaVersion = 2
	newVersion.Schema = "urn:example:media.processed:2"
	newVersion.Document = bytes.Replace(newVersion.Document, []byte(`"done"`), []byte(`"done", "pending"`), 1)
	catalog.Descriptors = append([]schemaBinding{newVersion}, catalog.Descriptors...)
	candidate, err := decodeSchemaCatalog(marshalSchemaTest(t, catalog))
	if err != nil {
		t.Fatal(err)
	}
	result, err := compareSchemaCatalogs(candidate, baseline)
	if err != nil || result.Preserved != 1 || result.Added != 1 || result.SamplesValidated != 4 {
		t.Fatalf("result=%+v err=%v", result, err)
	}
	if result.Fingerprints[0].Descriptor != "event/media.processed/v1" ||
		result.Fingerprints[1].Descriptor != "event/media.processed/v2" {
		t.Fatalf("unstable ordering: %+v", result.Fingerprints)
	}
}

func TestSchemaFingerprintNormalizesObjects(t *testing.T) {
	binding := schemaFixtureCatalog(t).Descriptors[0]
	original, err := checkSchemaDocument(binding)
	if err != nil {
		t.Fatal(err)
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, binding.Document); err != nil {
		t.Fatal(err)
	}
	binding.Document = compact.Bytes()
	fingerprint, err := checkSchemaDocument(binding)
	if err != nil || fingerprint != original {
		t.Fatalf("whitespace changed fingerprint: %s %v", fingerprint, err)
	}
	binding.Document = bytes.Replace(binding.Document, []byte(`"type":"integer","minimum":0`),
		[]byte(`"minimum":0,"type":"integer"`), 1)
	fingerprint, err = checkSchemaDocument(binding)
	if err != nil || fingerprint != original {
		t.Fatalf("object key order changed fingerprint: %s %v", fingerprint, err)
	}
	binding.Document = bytes.Replace(binding.Document, []byte(`"mediaId","status"`), []byte(`"status","mediaId"`), 1)
	fingerprint, err = checkSchemaDocument(binding)
	if err != nil || fingerprint == original {
		t.Fatalf("array order must remain significant: %s %v", fingerprint, err)
	}
}

func TestSchemaSamplesUseStandardValidation(t *testing.T) {
	for _, sample := range []string{
		`{"mediaId":"media-1"}`,
		`{"mediaId":1,"status":"done"}`,
		`{"mediaId":"","status":"done"}`,
		`{"mediaId":"media-1","status":"pending"}`,
		`{"mediaId":"media-1","status":"done","extra":true}`,
		`{"mediaId":"media-1","status":"done","sizes":[-1]}`,
	} {
		binding := schemaFixtureCatalog(t).Descriptors[0]
		binding.Samples = []json.RawMessage{json.RawMessage(sample)}
		if _, err := checkSchemaDocument(binding); err == nil || !strings.Contains(err.Error(), "sample 0") {
			t.Fatalf("sample %s: %v", sample, err)
		}
	}
}

func TestSchemaCatalogRejectsInvalidInputs(t *testing.T) {
	for _, input := range []string{
		`null`, `{}`, `{"specVersion":"1.0","specVersion":"1.0"}`, string(readSchemaFixture(t)) + `{}`,
		strings.Replace(string(readSchemaFixture(t)), `"policy":`, `"unknown":true,"policy":`, 1),
		strings.Replace(string(readSchemaFixture(t)), `"policy": "immutable-version-v1"`, `"policy": "backward"`, 1),
		strings.Replace(string(readSchemaFixture(t)), `"schemaVersion": 1`, `"schemaVersion": 0`, 1),
		strings.Replace(string(readSchemaFixture(t)), `"application/json"`, `"text/plain"`, 1),
		strings.Replace(string(readSchemaFixture(t)), `"dataEncoding": "json"`, `"dataEncoding": "text"`, 1),
		strings.Replace(string(readSchemaFixture(t)), `"type": "object"`, `"type": "invalid-type"`, 1),
		strings.Replace(string(readSchemaFixture(t)), schemaDraft, "http://json-schema.org/draft-07/schema#", 1),
		strings.Replace(string(readSchemaFixture(t)), `"type": "object"`, `"type":"object","type":"object"`, 1),
	} {
		if _, err := decodeSchemaCatalog([]byte(input)); err == nil {
			t.Fatalf("accepted invalid input: %s", input)
		}
	}
	catalog := schemaFixtureCatalog(t)
	catalog.Descriptors = append(catalog.Descriptors, catalog.Descriptors[0])
	if _, err := decodeSchemaCatalog(marshalSchemaTest(t, catalog)); err == nil || !strings.Contains(err.Error(), "duplicate") {
		t.Fatalf("duplicate binding: %v", err)
	}
}

func TestSchemaExternalReferencesCannotLoad(t *testing.T) {
	var hits atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(writer http.ResponseWriter, _ *http.Request) {
		hits.Add(1)
		_, _ = io.WriteString(writer, `{"type":"string"}`)
	}))
	defer server.Close()
	localPath := filepath.Join(t.TempDir(), "external.json")
	if err := os.WriteFile(localPath, []byte(`{"type":"string"}`), 0o600); err != nil {
		t.Fatal(err)
	}
	for _, reference := range []string{server.URL + "/external.json", "file://" + filepath.ToSlash(localPath), "other.json"} {
		binding := schemaFixtureCatalog(t).Descriptors[0]
		binding.Document = marshalSchemaTest(t, map[string]any{"$schema": schemaDraft, "$ref": reference})
		binding.Samples = nil
		if _, err := checkSchemaDocument(binding); err == nil {
			t.Fatalf("external reference accepted: %s", reference)
		}
	}
	if hits.Load() != 0 {
		t.Fatal("schema compiler used network")
	}
}

func TestSchemaNumericBounds(t *testing.T) {
	for _, document := range []string{
		`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"string","minLength":18446744073709551616}`,
		`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"array","maxItems":2147483648}`,
		`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"number","minimum":1e1000001}`,
	} {
		binding := schemaFixtureCatalog(t).Descriptors[0]
		binding.Document = json.RawMessage(document)
		binding.Samples = nil
		if _, err := checkSchemaDocument(binding); err == nil {
			t.Fatal("out-of-domain schema number accepted")
		}
	}
	binding := schemaFixtureCatalog(t).Descriptors[0]
	binding.Document = json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"number","minimum":0}`)
	binding.Samples = []json.RawMessage{json.RawMessage(`1e1000001`)}
	if _, err := checkSchemaDocument(binding); err == nil {
		t.Fatal("out-of-domain sample exponent accepted")
	}
	binding.Document = json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","const":9223372036854775807}`)
	binding.Samples = []json.RawMessage{json.RawMessage(`9223372036854775807`)}
	if _, err := checkSchemaDocument(binding); err != nil {
		t.Fatalf("int64 sample lost precision: %v", err)
	}
	binding.Samples = []json.RawMessage{json.RawMessage(`9223372036854775806`)}
	if _, err := checkSchemaDocument(binding); err == nil {
		t.Fatal("different int64 sample accepted")
	}
	binding.Document = json.RawMessage(`{"$schema":"https://json-schema.org/draft/2020-12/schema","type":"object"}`)
	binding.Samples = []json.RawMessage{json.RawMessage(`{"minLength":9223372036854775807}`)}
	if _, err := checkSchemaDocument(binding); err != nil {
		t.Fatalf("schema keyword bound leaked into sample data: %v", err)
	}
}

func TestSchemaNumericSpellingsRemainDistinct(t *testing.T) {
	fingerprints := make(map[string]struct{})
	for _, number := range []string{"1000", "1000.0", "1e3"} {
		binding := schemaFixtureCatalog(t).Descriptors[0]
		binding.Document = json.RawMessage(`{"$schema":"` + schemaDraft + `","type":"number","minimum":` + number + `}`)
		binding.Samples = []json.RawMessage{json.RawMessage(`1000`)}
		fingerprint, err := checkSchemaDocument(binding)
		if err != nil {
			t.Fatal(err)
		}
		fingerprints[fingerprint] = struct{}{}
	}
	if len(fingerprints) != 3 {
		t.Fatal("numeric spellings were silently collapsed")
	}
}

func TestSchemaJSONDepthBoundary(t *testing.T) {
	var value any
	valid := strings.Repeat("[", schemaMaxDepth) + "0" + strings.Repeat("]", schemaMaxDepth)
	if err := decodeSchemaJSON([]byte(valid), &value); err != nil {
		t.Fatalf("depth boundary rejected: %v", err)
	}
	if err := decodeSchemaJSON([]byte("["+valid+"]"), &value); err == nil {
		t.Fatal("depth boundary exceeded")
	}
	empty := strings.Repeat("[", schemaMaxDepth) + strings.Repeat("]", schemaMaxDepth)
	if err := decodeSchemaJSON([]byte(empty), &value); err != nil {
		t.Fatalf("empty-container depth boundary rejected: %v", err)
	}
	if err := decodeSchemaJSON([]byte("["+empty+"]"), &value); err == nil {
		t.Fatal("empty-container depth boundary exceeded")
	}
}

func TestSchemaManifestCoverage(t *testing.T) {
	catalog, err := decodeSchemaCatalog(readSchemaFixture(t))
	if err != nil {
		t.Fatal(err)
	}
	binding := schemaFixtureCatalog(t).Descriptors[0]
	manifest := messenger.Manifest{
		SpecVersion: messenger.ManifestSpecVersion, Source: "urn:test:schemas",
		Descriptors: []messenger.ManifestDescriptor{{DescriptorInfo: binding.DescriptorInfo}},
	}
	if _, err := bindSchemaManifest(catalog, manifest); err != nil {
		t.Fatal(err)
	}
	manifest.Descriptors[0].Schema = "urn:changed"
	if _, err := bindSchemaManifest(catalog, manifest); err == nil {
		t.Fatal("metadata mismatch accepted")
	}
	manifest.Descriptors[0].DescriptorInfo = binding.DescriptorInfo
	manifest.Descriptors = append(manifest.Descriptors, messenger.ManifestDescriptor{
		DescriptorInfo: messenger.MustEvent[string]("other.event", 1, messenger.JSON[string]()).Info(),
	})
	if _, err := bindSchemaManifest(catalog, manifest); err == nil {
		t.Fatal("missing JSON binding accepted")
	}
	manifest.Descriptors[1].DescriptorInfo = messenger.MustEvent[string]("other.event", 1, messenger.Text()).Info()
	if skipped, err := bindSchemaManifest(catalog, manifest); err != nil || skipped != 1 {
		t.Fatalf("non-JSON coverage: skipped=%d err=%v", skipped, err)
	}
	manifest.Descriptors = manifest.Descriptors[1:]
	if _, err := bindSchemaManifest(catalog, manifest); err == nil {
		t.Fatal("extra catalog binding accepted")
	}
}

func TestSchemaCheckUsageAndPin(t *testing.T) {
	for _, args := range [][]string{
		{cmdSchema, "unknown"},
		{cmdSchema, cmdSchemaCheck},
		{cmdSchema, cmdSchemaCheck, flagSchemaManifest, "x", flagFile, "x", flagSchemaBaseline, "x", flagSchemaBaselineSHA256, "bad"},
	} {
		if code := run(args, io.Discard, io.Discard); code != exitUsage {
			t.Fatalf("args=%v code=%d", args, code)
		}
	}
	var stderr bytes.Buffer
	code := run([]string{
		cmdSchema, cmdSchemaCheck, flagSchemaManifest, "missing-manifest.json", flagFile, "missing-candidate.json",
		flagSchemaBaseline, schemaFixturePath, flagSchemaBaselineSHA256, strings.Repeat("0", 64),
	}, io.Discard, &stderr)
	if code != exitFailure || !strings.Contains(stderr.String(), "trusted pin") {
		t.Fatalf("pin must fail before other reads: code=%d stderr=%s", code, stderr.String())
	}
}

func TestSchemaJSONBoundsAndStrictManifest(t *testing.T) {
	var value any
	for _, input := range [][]byte{
		bytes.Repeat([]byte(" "), schemaMaxBytes+1),
		{0xff},
		[]byte(strings.Repeat("[", schemaMaxDepth+2) + "0" + strings.Repeat("]", schemaMaxDepth+2)),
		[]byte(`{"a":1,"\u0061":2}`), []byte(`{"a":1} {"b":2}`),
		[]byte(`{"\ud800":1}`), []byte(`{"\udc00":1}`),
	} {
		if err := decodeSchemaJSON(input, &value); err == nil {
			t.Fatal("invalid or unbounded JSON accepted")
		}
	}
	path := filepath.Join(t.TempDir(), "oversize.json")
	if err := os.WriteFile(path, bytes.Repeat([]byte(" "), schemaMaxBytes+1), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := readSchemaFile(path); err == nil {
		t.Fatal("oversize file accepted")
	}
	var manifest messenger.Manifest
	if err := decodeSchemaJSON([]byte(`{"specVersion":"1.0","source":"urn:test","schemas":[]}`), &manifest); err == nil {
		t.Fatal("manifest v1 accepted schema extension")
	}
}

func TestSchemaJSONUnicode(t *testing.T) {
	for _, input := range []string{`{"\ud83d\ude00":"ok"}`, `{"literal":"\\ud800"}`, `{"😀":"ok"}`} {
		var value any
		if err := decodeSchemaJSON([]byte(input), &value); err != nil {
			t.Fatalf("valid Unicode rejected: %s: %v", input, err)
		}
	}
}

func FuzzSchemaCatalog(f *testing.F) {
	f.Add([]byte(`{"specVersion":"1.0","policy":"immutable-version-v1","descriptors":[]}`))
	f.Add([]byte(`{"a":{"a":1,"a":2}}`))
	f.Fuzz(func(_ *testing.T, data []byte) {
		// Exercise the bounded parser only; general schema compilation is for trusted repository inputs.
		var value any
		_ = decodeSchemaJSON(data, &value)
	})
}

func readSchemaFixture(t *testing.T) []byte {
	t.Helper()
	data, err := os.ReadFile(schemaFixturePath)
	if err != nil {
		t.Fatal(err)
	}
	return data
}

func schemaFixtureCatalog(t *testing.T) schemaCatalog {
	t.Helper()
	var catalog schemaCatalog
	if err := json.Unmarshal(readSchemaFixture(t), &catalog); err != nil {
		t.Fatal(err)
	}
	return catalog
}

func marshalSchemaTest(t *testing.T, value any) []byte {
	t.Helper()
	data, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return data
}
