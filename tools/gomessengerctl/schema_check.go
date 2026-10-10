package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"unicode/utf8"

	messenger "github.com/assurrussa/gomessenger"
	"github.com/santhosh-tekuri/jsonschema/v6"
)

const (
	schemaPolicy   = "immutable-version-v1"
	schemaDraft    = "https://json-schema.org/draft/2020-12/schema"
	schemaResource = "https://gomessenger.invalid/payload.json"
	schemaMaxBytes = 1 << 20
	schemaMaxDepth = 64
)

type schemaCatalog struct {
	SpecVersion string          `json:"specVersion"`
	Policy      string          `json:"policy"`
	Descriptors []schemaBinding `json:"descriptors"`
}

type schemaBinding struct {
	messenger.DescriptorInfo
	Document json.RawMessage   `json:"document"`
	Samples  []json.RawMessage `json:"samples,omitempty"`
}

type checkedSchema struct {
	info        messenger.DescriptorInfo
	fingerprint string
	samples     int
}

type schemaFingerprint struct {
	Descriptor string `json:"descriptor"`
	SHA256     string `json:"sha256"`
}

type schemaCheckReport struct {
	Policy           string              `json:"policy"`
	BaselineSHA256   string              `json:"baselineSha256"`
	Preserved        int                 `json:"preserved"`
	Added            int                 `json:"added"`
	SamplesValidated int                 `json:"samplesValidated"`
	SkippedNonJSON   int                 `json:"skippedNonJson"`
	Fingerprints     []schemaFingerprint `json:"fingerprints"`
}

func checkSchemas(args []string, stdout, stderr io.Writer) int {
	set := flag.NewFlagSet("schema check", flag.ContinueOnError)
	set.SetOutput(stderr)
	manifestPath := set.String("manifest", "", "existing v1 manifest JSON file")
	file := set.String("file", "", "candidate schema catalog JSON file")
	baselinePath := set.String("baseline", "", "released schema catalog JSON file")
	pin := set.String("baseline-sha256", "", "trusted lowercase SHA-256 of baseline file bytes")
	if err := set.Parse(args); err != nil || *file == "" || *manifestPath == "" ||
		*baselinePath == "" || set.NArg() != 0 {
		return exitUsage
	}
	decoded, err := hex.DecodeString(*pin)
	if err != nil || len(decoded) != sha256.Size || hex.EncodeToString(decoded) != *pin {
		return exitUsage
	}
	baselineBytes, err := readSchemaFile(*baselinePath)
	if err != nil {
		return report(stderr, err)
	}
	if actual := fmt.Sprintf("%x", sha256.Sum256(baselineBytes)); actual != *pin {
		return report(stderr, errors.New("baseline SHA-256 does not match trusted pin"))
	}
	baseline, err := decodeSchemaCatalog(baselineBytes)
	if err != nil {
		return report(stderr, fmt.Errorf("baseline: %w", err))
	}
	candidateBytes, err := readSchemaFile(*file)
	if err != nil {
		return report(stderr, err)
	}
	candidate, err := decodeSchemaCatalog(candidateBytes)
	if err != nil {
		return report(stderr, fmt.Errorf("candidate: %w", err))
	}
	manifestBytes, err := readSchemaFile(*manifestPath)
	if err != nil {
		return report(stderr, err)
	}
	var manifest messenger.Manifest
	if err := decodeSchemaJSON(manifestBytes, &manifest); err != nil {
		return report(stderr, fmt.Errorf("manifest: %w", err))
	}
	if err := manifest.Validate(); err != nil {
		return report(stderr, err)
	}
	skipped, err := bindSchemaManifest(candidate, manifest)
	if err != nil {
		return report(stderr, err)
	}
	result, err := compareSchemaCatalogs(candidate, baseline)
	if err != nil {
		return report(stderr, err)
	}
	result.BaselineSHA256 = *pin
	result.SkippedNonJSON = skipped
	if err := writeJSON(stdout, result); err != nil {
		return report(stderr, err)
	}
	return exitOK
}

func readSchemaFile(path string) (data []byte, err error) {
	file, err := os.Open(filepath.Clean(path))
	if err != nil {
		return nil, fmt.Errorf("open %s: %w", path, err)
	}
	defer func() {
		if closeErr := file.Close(); err == nil && closeErr != nil {
			err = fmt.Errorf("close %s: %w", path, closeErr)
		}
	}()
	data, err = io.ReadAll(io.LimitReader(file, schemaMaxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	if len(data) > schemaMaxBytes {
		return nil, fmt.Errorf("%s exceeds %d bytes", path, schemaMaxBytes)
	}
	return data, nil
}

func decodeSchemaJSON(data []byte, target any) error {
	if len(data) > schemaMaxBytes || !utf8.Valid(data) || !validSchemaSurrogates(data) {
		return errors.New("schema input exceeds size limit or is not UTF-8")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.UseNumber()
	if err := scanSchemaJSON(decoder, 0); err != nil {
		return err
	}
	if _, err := decoder.Token(); !errors.Is(err, io.EOF) {
		return errors.New("trailing JSON value")
	}
	decoder = json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("decode schema input: %w", err)
	}
	return nil
}

// encoding/json replaces unpaired UTF-16 escapes; reject them before fingerprint normalization.
func validSchemaSurrogates(data []byte) bool {
	for index := 0; index < len(data); index++ {
		if data[index] != '\\' || index+1 >= len(data) {
			continue
		}
		index++
		if data[index] != 'u' || index+4 >= len(data) {
			continue
		}
		value, err := strconv.ParseUint(string(data[index+1:index+5]), 16, 16)
		if err != nil {
			return false
		}
		index += 4
		if value < 0xd800 || value > 0xdfff {
			continue
		}
		if value > 0xdbff || index+6 >= len(data) || data[index+1] != '\\' || data[index+2] != 'u' {
			return false
		}
		low, err := strconv.ParseUint(string(data[index+3:index+7]), 16, 16)
		if err != nil || low < 0xdc00 || low > 0xdfff {
			return false
		}
		index += 6
	}
	return true
}

func scanSchemaJSON(decoder *json.Decoder, depth int) error {
	if depth > schemaMaxDepth {
		return errors.New("JSON nesting exceeds 64 levels")
	}
	token, err := decoder.Token()
	if err != nil {
		return fmt.Errorf("read JSON token: %w", err)
	}
	if number, ok := token.(json.Number); ok {
		if len(number) > 128 {
			return errors.New("JSON number exceeds 128 bytes")
		}
		if _, exponent, exists := strings.Cut(strings.ToLower(string(number)), "e"); exists {
			power, err := strconv.Atoi(exponent)
			if err != nil || power < -1000 || power > 1000 {
				return errors.New("JSON number exponent must be between -1000 and 1000")
			}
		}
	}
	delimiter, container := token.(json.Delim)
	if !container {
		return nil
	}
	if depth >= schemaMaxDepth {
		return errors.New("JSON nesting exceeds 64 levels")
	}
	keys := make(map[string]struct{})
	for decoder.More() {
		if delimiter == '{' {
			keyToken, err := decoder.Token()
			if err != nil {
				return fmt.Errorf("read JSON key: %w", err)
			}
			key, ok := keyToken.(string)
			if !ok {
				return errors.New("JSON object key is not a string")
			}
			if _, exists := keys[key]; exists {
				return fmt.Errorf("duplicate JSON object key %q", key)
			}
			keys[key] = struct{}{}
		}
		if err := scanSchemaJSON(decoder, depth+1); err != nil {
			return err
		}
	}
	if _, err := decoder.Token(); err != nil {
		return fmt.Errorf("close JSON container: %w", err)
	}
	return nil
}

func decodeSchemaCatalog(data []byte) (map[string]checkedSchema, error) {
	var catalog schemaCatalog
	if err := decodeSchemaJSON(data, &catalog); err != nil {
		return nil, err
	}
	if catalog.SpecVersion != "1.0" || catalog.Policy != schemaPolicy || len(catalog.Descriptors) == 0 {
		return nil, errors.New("catalog requires specVersion 1.0, immutable-version-v1 policy, and descriptors")
	}
	result := make(map[string]checkedSchema, len(catalog.Descriptors))
	for _, binding := range catalog.Descriptors {
		key := schemaKey(binding.DescriptorInfo)
		if _, exists := result[key]; exists {
			return nil, fmt.Errorf("%s: duplicate schema binding", key)
		}
		if err := validateSchemaIdentity(binding.DescriptorInfo); err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		fingerprint, err := checkSchemaDocument(binding)
		if err != nil {
			return nil, fmt.Errorf("%s: %w", key, err)
		}
		result[key] = checkedSchema{info: binding.DescriptorInfo, fingerprint: fingerprint, samples: len(binding.Samples)}
	}
	return result, nil
}

func validateSchemaIdentity(info messenger.DescriptorInfo) error {
	if info.ContentType != "application/json" || info.DataEncoding != messenger.DataJSON {
		return errors.New("schema bindings require application/json and json dataEncoding")
	}
	var err error
	switch info.Kind {
	case messenger.KindCommand:
		_, err = messenger.NewCommand[any](info.Name, info.SchemaVersion, messenger.JSON[any]())
	case messenger.KindEvent:
		_, err = messenger.NewEvent[any](info.Name, info.SchemaVersion, messenger.JSON[any]())
	case messenger.KindQuery:
		_, err = messenger.NewQuery[any, any](info.Name, info.SchemaVersion, messenger.JSON[any]())
	default:
		return errors.New("schema binding kind must be command, event, or query")
	}
	return err
}

func checkSchemaDocument(binding schemaBinding) (string, error) {
	var document map[string]any
	if err := decodeSchemaJSON(binding.Document, &document); err != nil {
		return "", err
	}
	if document["$schema"] != schemaDraft {
		return "", errors.New("document requires explicit JSON Schema draft 2020-12 $schema")
	}
	if err := boundSchemaCardinalities(document); err != nil {
		return "", err
	}
	canonical, err := json.Marshal(document)
	if err != nil {
		return "", fmt.Errorf("normalize schema: %w", err)
	}
	compiler := jsonschema.NewCompiler()
	compiler.DefaultDraft(jsonschema.Draft2020)
	compiler.UseLoader(nil) // No file or network retrieval, including indirect references.
	if err := compiler.AddResource(schemaResource, document); err != nil {
		return "", fmt.Errorf("add local schema: %w", err)
	}
	compiled, err := compiler.Compile(schemaResource)
	if err != nil {
		return "", fmt.Errorf("compile local schema: %s", schemaValidationDetails(err))
	}
	for index, sample := range binding.Samples {
		var value any
		if err := decodeSchemaJSON(sample, &value); err != nil {
			return "", fmt.Errorf("sample %d: %w", index, err)
		}
		if err := compiled.Validate(value); err != nil {
			return "", fmt.Errorf("sample %d: %s", index, schemaValidationDetails(err))
		}
	}
	return fmt.Sprintf("%x", sha256.Sum256(canonical)), nil
}

// The pinned validator stores cardinalities in int. Bound numeric-valued size keys throughout the
// document (conservatively including annotation/const data), without interpreting schema applicators.
func boundSchemaCardinalities(value any) error {
	switch value := value.(type) {
	case map[string]any:
		keys := make([]string, 0, len(value))
		for key := range value {
			keys = append(keys, key)
		}
		sort.Strings(keys)
		for _, key := range keys {
			switch key {
			case "minLength", "maxLength", "minItems", "maxItems", "minContains", "maxContains",
				"minProperties", "maxProperties":
				if number, ok := value[key].(json.Number); ok {
					rational, parsed := new(big.Rat).SetString(string(number))
					if !parsed || !rational.IsInt() || rational.Sign() < 0 || rational.Num().BitLen() > 31 {
						return fmt.Errorf("%s must be an integer between 0 and 2147483647", key)
					}
				}
			}
			if err := boundSchemaCardinalities(value[key]); err != nil {
				return err
			}
		}
	case []any:
		for _, item := range value {
			if err := boundSchemaCardinalities(item); err != nil {
				return err
			}
		}
	}
	return nil
}

// Report sorted locations without dependency map ordering or sample values in diagnostics.
func schemaValidationDetails(err error) string {
	var schemaError *jsonschema.SchemaValidationError
	if errors.As(err, &schemaError) {
		err = schemaError.Err
	}
	var validation *jsonschema.ValidationError
	if !errors.As(err, &validation) {
		return "invalid schema or unresolved reference; only embedded resources are available"
	}
	output := validation.BasicOutput()
	units := output.Errors
	if len(units) == 0 {
		units = []jsonschema.OutputUnit{*output}
	}
	locations := make([]string, 0, len(units))
	for _, unit := range units {
		locations = append(locations, fmt.Sprintf("instance %q keyword %q", unit.InstanceLocation, unit.KeywordLocation))
	}
	sort.Strings(locations)
	return "validation failed: " + strings.Join(locations, "; ")
}

func bindSchemaManifest(candidate map[string]checkedSchema, manifest messenger.Manifest) (int, error) {
	matched := make(map[string]struct{}, len(candidate))
	skipped := 0
	for _, descriptor := range manifest.Descriptors {
		info := descriptor.DescriptorInfo
		key := schemaKey(info)
		if info.DataEncoding != messenger.DataJSON {
			skipped++
			continue
		}
		binding, exists := candidate[key]
		if !exists {
			return 0, fmt.Errorf("%s: missing JSON schema binding", key)
		}
		if binding.info != info {
			return 0, fmt.Errorf("%s: schema binding metadata does not match manifest", key)
		}
		matched[key] = struct{}{}
	}
	for _, key := range sortedSchemaKeys(candidate) {
		if _, exists := matched[key]; !exists {
			return 0, fmt.Errorf("%s: schema binding has no matching JSON manifest descriptor", key)
		}
	}
	return skipped, nil
}

func compareSchemaCatalogs(candidate, baseline map[string]checkedSchema) (schemaCheckReport, error) {
	result := schemaCheckReport{Policy: schemaPolicy, Fingerprints: make([]schemaFingerprint, 0, len(candidate))}
	for _, key := range sortedSchemaKeys(baseline) {
		old := baseline[key]
		current, exists := candidate[key]
		if !exists {
			return result, fmt.Errorf("%s: baseline version removed; retain it alongside new versions", key)
		}
		if current.info != old.info {
			return result, fmt.Errorf("%s: published descriptor metadata changed", key)
		}
		if current.fingerprint != old.fingerprint {
			return result, fmt.Errorf("%s: immutable schema document changed (%s -> %s); add a new schemaVersion", key,
				old.fingerprint, current.fingerprint)
		}
		result.Preserved++
	}
	for _, key := range sortedSchemaKeys(candidate) {
		current := candidate[key]
		if _, exists := baseline[key]; !exists {
			result.Added++
		}
		result.SamplesValidated += current.samples
		result.Fingerprints = append(result.Fingerprints, schemaFingerprint{Descriptor: key, SHA256: current.fingerprint})
	}
	return result, nil
}

func schemaKey(info messenger.DescriptorInfo) string {
	return fmt.Sprintf("%s/%s/v%d", info.Kind, info.Name, info.SchemaVersion)
}

func sortedSchemaKeys(catalog map[string]checkedSchema) []string {
	keys := make([]string, 0, len(catalog))
	for key := range catalog {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}
