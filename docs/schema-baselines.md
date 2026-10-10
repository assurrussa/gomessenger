# Local JSON Schema baselines

This unreleased CLI feature checks a deliberately narrow contract: published descriptor versions and their schema
documents stay immutable, and any supplied example payloads satisfy their own JSON Schema. It does not establish
universal backward, forward, or full compatibility between different schemas. It does not inspect Go payload types,
run their codecs, or install runtime validation.

## Inputs and manual command

Keep the existing manifest v1 unchanged. Supply a separate local schema catalog alongside it:

```sh
gomessengerctl schema check \
  --manifest manifest.json \
  --file schemas.json \
  --baseline released-schemas.json \
  --baseline-sha256 TRUSTED_SHA256
```

The command is read-only and offline. It has no registry, broker connection, network fetch, or automatic CI trigger.
It returns 0 on success, 1 for invalid inputs, pin mismatch, sample failures, or changed/removed baseline versions,
and 2 for invalid arguments. Success is a deterministic JSON report with preserved/added counts, candidate sample
count, non-JSON descriptors skipped, and sorted descriptor fingerprints. It does not print payload values.

`--baseline-sha256` is required: exactly 64 lowercase hexadecimal characters. The hash covers the exact baseline
file bytes, including whitespace and samples, and is checked before parsing the baseline or reading other inputs.
Keep that digest in a separately reviewed release record or an existing manual gate. Do not calculate it from the
candidate during the comparison: replacing both the baseline and trusted pin defeats the protection. This first
scope does not discover release tags, enforce repository permissions, or manage intentional retirement of versions.

There is no previously published GoMessenger payload-schema baseline. The checked-in baseline below is an example
fixture, not a claim about payload schemas published in v0.3.1. An adopting application must review and pin its own
initial baseline before using this gate for releases.

## Catalog format

```json
{
  "specVersion": "1.0",
  "policy": "immutable-version-v1",
  "descriptors": [{
    "kind": "event",
    "name": "media.processed",
    "schemaVersion": 1,
    "contentType": "application/json",
    "dataEncoding": "json",
    "schema": "urn:example:media.processed:1",
    "document": {
      "$schema": "https://json-schema.org/draft/2020-12/schema",
      "type": "object",
      "properties": {"mediaId": {"type": "string"}},
      "required": ["mediaId"],
      "additionalProperties": false
    },
    "samples": [{"mediaId": "media-1"}]
  }]
}
```

- Each binding identifies `(kind, name, schemaVersion)` and repeats its descriptor's content type, encoding, and
  optional schema URI. All descriptor metadata must match the candidate manifest exactly. The `schema` string is
  still URI metadata; `WithSchema` neither fetches nor validates a document.
- Every `dataEncoding: json` manifest descriptor needs exactly one binding, with `contentType: application/json`.
  A custom JSON content type fails this first scope. Extra/duplicate bindings fail. Non-JSON manifest descriptors
  are explicitly counted as skipped; this gate makes no claim about their payloads or schema evolution.
- Commands, events, and local query request descriptors are supported. Query result types remain absent; the
  manifest still validates the query's existing route/handler requirements.
- `document` contains the actual repository-owned JSON Schema, inline for a self-contained baseline. Its root must
  be an object with the exact `$schema` URL shown above. `samples` is optional and contains literal JSON values.
  Both baseline and candidate documents compile, and their supplied samples validate; the report counts candidate
  samples only. Zero samples is allowed and provides no payload-example evidence.
- Catalog envelope fields use Go's strict JSON struct decoder, including its existing case-insensitive field-name
  matching. Unknown fields fail. All inputs reject duplicate decoded JSON keys, trailing JSON, invalid UTF-8, and
  unpaired UTF-16 escapes. This does not change the separate `manifest validate` command.

## Schema validation and local references

Compilation and sample validation use pinned
[`github.com/santhosh-tekuri/jsonschema/v6` v6.0.3](https://github.com/santhosh-tekuri/jsonschema/tree/v6.0.3)
inside the CLI module. The root GoMessenger module gains no dependency or public API.

Each document has its own compiler with `DefaultDraft(Draft2020)` and `UseLoader(nil)`. Local fragments and embedded
`$defs`/resources work. Referencing a different file, another catalog entry, or a remote URL fails if it is not an
embedded resource or the validator's bundled standard metaschema. A URI-looking `$id` or `$schema` is never a request
to access the network. No default file loader is retained.

Standard validator defaults apply: `format` and `content*` annotations do not become assertions, unknown keywords
are not treated as custom validators, and regular expressions use the library's default Go regexp engine. Compilation
can therefore reject regex constructs unsupported by that engine. Do not interpret a successful sample as coverage
of annotations, custom keywords, Go decoding behavior, business invariants, or all future producer payloads. See the
[JSON Schema 2020-12 validation specification](https://json-schema.org/draft/2020-12/json-schema-validation).

These are trusted repository inputs, not a service for executing hostile schemas. Each input file is limited to
1 MiB and at most 64 JSON nesting levels. Numeric tokens are limited to 128 bytes, with decimal exponents between
-1000 and 1000. Numeric-valued `minLength`, `maxLength`, `minItems`, `maxItems`, `minContains`, `maxContains`,
`minProperties`, and `maxProperties` keys must be integers in `[0, 2147483647]`. This conservative guard applies
throughout the schema document, including annotation/constant data containing those same keys. It prevents unchecked
integer conversion in the pinned validator without implementing a second schema interpreter. Other schema numbers
and samples retain exact JSON-number precision, including int64 values within these token limits. These limits do
not promise a CPU deadline for arbitrary recursive or combinatorial schemas.

## Immutability and fingerprint definition

For every baseline key, the candidate must retain exactly equal descriptor metadata and an equal document fingerprint.
Removing a baseline version fails. Adding a descriptor/version passes after its manifest binding, schema, and samples
validate. Changing even an optional field in an existing version fails: add a new version and retain the old version.
Topology routes, handler IDs, manifest source, and service IDs remain independently validated manifest metadata; they
are not part of the payload-document fingerprint. `EnvelopeFingerprint` is unchanged and remains envelope identity.

The document fingerprint is lowercase SHA-256 over Go `encoding/json.Marshal` output from a `UseNumber`-decoded schema
object. Map keys sort deterministically; insignificant whitespace, object member order, and equivalent string escape
spelling normalize. JSON number spellings and all array order remain significant. For example `1` versus `1.0`, or
reordering `required`, can produce different fingerprints even when the schemas describe the same instances.
Annotations and unknown-keyword contents also participate. This is a conservative document-identity rule, not JSON
Schema semantic equivalence or RFC 8785 canonicalization. Samples are validated but are not part of the document
fingerprint; baseline-file pinning independently protects their exact released bytes.

## Fixed example and verification

From `tools/gomessengerctl`, the checked-in manual example is:

```sh
go run . schema check \
  --manifest testdata/schema/manifest.json \
  --file testdata/schema/baseline.json \
  --baseline testdata/schema/baseline.json \
  --baseline-sha256 ed6467e74ea1fd496721d1d71e0f5a82592302d10afec2b5b4c035af36c5bb56
```

Its expected document fingerprint for `event/media.processed/v1` is
`05a8b3cd9ea49d734240e3acd6daea223fe78615424c77be55265127872cb57e`, with one preserved version and two candidate samples.
The literal digest intentionally makes fixture edits reviewable.

Manual focused checks from that module:

```sh
GOWORK=off go mod tidy
GOWORK=off go test -run 'TestSchema|TestStrictJSONRejectsUnknownFields|TestManifestAndTopologyOfflineValidation' .
GOWORK=off go test -race .
GOWORK=off go test -run '^$' -fuzz '^FuzzSchemaCatalog$' -fuzztime=10s .
```

Then run the repository's aggregate `make check` using its supported Go toolchain. No CI scheduling change is needed.
These commands are verification instructions, not a record that the source candidate has passed them.
