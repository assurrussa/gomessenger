package messenger_test

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	messenger "github.com/assurrussa/gomessenger"
)

type findArticle struct {
	ID int64 `json:"id"`
}

type articleView struct {
	ID    int64
	Title string
}

type queryContextKey struct{}

type queryTracePropagator struct{ calls atomic.Int32 }

func (p *queryTracePropagator) Inject(_ context.Context, carrier map[string]string) {
	p.calls.Add(1)
	carrier["traceparent"] = testTraceParent
}

func (*queryTracePropagator) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

func TestQueryDescriptorIdentityAndBuilderConflicts(t *testing.T) {
	if _, err := messenger.NewQuery[int, string]("Bad Query", 0, nil); !errors.Is(err, messenger.ErrInvalidDescriptor) {
		t.Fatalf("invalid query descriptor = %v", err)
	}
	t.Run("must query", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("MustQuery did not panic")
			}
		}()
		_ = messenger.MustQuery[int, string]("Bad", 1, messenger.JSON[int]())
	})

	base := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]()).WithSchema("urn:query:catalog:1")
	if info := base.Info(); info.Kind != messenger.KindQuery || info.Schema != "urn:query:catalog:1" ||
		info.DataEncoding != messenger.DataJSON {
		t.Fatalf("query descriptor info = %#v", info)
	}

	textCodec, err := messenger.CustomCodec(
		"text/plain", messenger.DataText,
		func(value int) ([]byte, error) { return []byte(strconv.Itoa(value)), nil },
		func([]byte) (int, error) { return 0, nil },
	)
	if err != nil {
		t.Fatalf("text codec: %v", err)
	}
	tests := []struct {
		name    string
		declare func(*messenger.Builder)
	}{
		{
			name: "request type",
			declare: func(builder *messenger.Builder) {
				first := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]())
				other := messenger.MustQuery[string, string]("catalog.lookup", 1, messenger.JSON[string]())
				builder.HandleQueryFunc(first, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
				builder.RouteQuery(other, messenger.NewLocalSyncRoute())
			},
		},
		{
			name: "result type",
			declare: func(builder *messenger.Builder) {
				first := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]())
				other := messenger.MustQuery[int, int]("catalog.lookup", 1, messenger.JSON[int]())
				builder.HandleQueryFunc(first, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
				builder.RouteQuery(other, messenger.NewLocalSyncRoute())
			},
		},
		{
			name: "schema",
			declare: func(builder *messenger.Builder) {
				first := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]()).WithSchema("urn:a")
				other := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]()).WithSchema("urn:b")
				builder.HandleQueryFunc(first, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
				builder.RouteQuery(other, messenger.NewLocalSyncRoute())
			},
		},
		{
			name: "codec metadata",
			declare: func(builder *messenger.Builder) {
				first := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]())
				other := messenger.MustQuery[int, string]("catalog.lookup", 1, textCodec)
				builder.HandleQueryFunc(first, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
				builder.RouteQuery(other, messenger.NewLocalSyncRoute())
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := messenger.NewBuilder(messenger.WithSource(testSource))
			test.declare(builder)
			_, _, err := builder.Build()
			if !errors.Is(err, messenger.ErrDescriptorConflict) {
				t.Fatalf("build error = %v", err)
			}
		})
	}
}

func TestQueryBuilderRequiresExactlyOneHandlerAndRoute(t *testing.T) {
	query := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]())
	tests := []struct {
		name    string
		declare func(*messenger.Builder)
		want    error
	}{
		{
			name: "missing route",
			declare: func(builder *messenger.Builder) {
				builder.HandleQueryFunc(query, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
			},
			want: messenger.ErrRouteNotFound,
		},
		{
			name:    "missing handler",
			declare: func(builder *messenger.Builder) { builder.RouteQuery(query, messenger.NewLocalSyncRoute()) },
			want:    messenger.ErrHandlerNotFound,
		},
		{
			name: "duplicate handler",
			declare: func(builder *messenger.Builder) {
				handler := func(context.Context, int) (string, error) { return "", nil }
				builder.HandleQueryFunc(query, "catalog.lookup", handler)
				builder.HandleQueryFunc(query, "catalog.lookup.other", handler)
				builder.RouteQuery(query, messenger.NewLocalSyncRoute())
			},
			want: messenger.ErrHandlerConflict,
		},
		{
			name: "duplicate route",
			declare: func(builder *messenger.Builder) {
				builder.HandleQueryFunc(query, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
				builder.RouteQuery(query, messenger.NewLocalSyncRoute())
				builder.RouteQuery(query, messenger.NewLocalSyncRoute())
			},
			want: messenger.ErrRouteConflict,
		},
		{
			name: "typed nil route",
			declare: func(builder *messenger.Builder) {
				builder.HandleQueryFunc(query, "catalog.lookup", func(context.Context, int) (string, error) { return "", nil })
				var route *messenger.LocalSyncRoute
				builder.RouteQuery(query, route)
			},
			want: messenger.ErrRouteNotFound,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			builder := messenger.NewBuilder(messenger.WithSource(testSource))
			test.declare(builder)
			_, _, err := builder.Build()
			if !errors.Is(err, test.want) {
				t.Fatalf("build error = %v, want %v", err, test.want)
			}
		})
	}
}

func TestMessengerSyncQueryMetadataLineageMiddlewareAndObservations(t *testing.T) {
	id := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000010")
	parentID := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000009")
	correlationID := mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000008")
	fixedTime := time.Unix(1_710_000_000, 123).UTC()
	var encodeCalls atomic.Int32
	codec, err := messenger.CustomCodec(
		"application/json", messenger.DataJSON,
		func(value findArticle) ([]byte, error) {
			encodeCalls.Add(1)
			return []byte(fmt.Sprintf(`{"id":%d}`, value.ID)), nil
		},
		func([]byte) (findArticle, error) { return findArticle{}, nil },
	)
	if err != nil {
		t.Fatalf("codec: %v", err)
	}
	query := messenger.MustQuery[findArticle, articleView]("article.find", 1, codec).WithSchema("urn:article:find:1")
	expectedMetadata := messenger.Metadata{
		ID:            id,
		Kind:          messenger.KindQuery,
		Name:          "article.find",
		SchemaVersion: 1,
		Source:        "urn:service:catalog",
		Time:          fixedTime,
		CorrelationID: correlationID,
		CausationID:   parentID,
		ContentType:   "application/json",
		Schema:        "urn:article:find:1",
		Headers:       map[string]string{"traceparent": testTraceParent},
	}
	observer := &recordingObserver{}
	propagator := &queryTracePropagator{}
	var middlewareOrder []string
	builder := messenger.NewBuilder(
		messenger.WithSource("urn:service:catalog"),
		messenger.WithIDGenerator(fixedGenerator{id: id}),
		messenger.WithClock(func() time.Time { return fixedTime }),
		messenger.WithContextPropagator(propagator),
		messenger.WithObserver(observer),
	)
	builder.UseMiddleware(func(
		ctx context.Context,
		metadata messenger.Metadata,
		handlerID string,
		next messenger.HandlerFunc,
	) error {
		middlewareOrder = append(middlewareOrder, "before:"+handlerID)
		metadata.Headers["traceparent"] = "mutated-copy"
		err := next(context.WithValue(ctx, queryContextKey{}, "from-middleware"))
		middlewareOrder = append(middlewareOrder, "after:"+handlerID)
		return err
	})
	builder.HandleQuery(query, "article.find.v1", func(
		ctx context.Context,
		message messenger.Message[findArticle],
	) (articleView, error) {
		middlewareOrder = append(middlewareOrder, "handler")
		if ctx.Value(queryContextKey{}) != "from-middleware" {
			return articleView{}, errors.New("replacement context missing")
		}
		metadata := message.Metadata
		if fromContext, ok := messenger.MetadataFromContext(ctx); !ok || !reflect.DeepEqual(fromContext, metadata) {
			return articleView{}, errors.New("metadata context mismatch")
		}
		if !reflect.DeepEqual(metadata, expectedMetadata) {
			return articleView{}, fmt.Errorf("metadata = %#v", metadata)
		}
		return articleView{ID: message.Payload.ID, Title: "typed result"}, nil
	})
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	ctx := messenger.ContextWithMetadata(t.Context(), messenger.Metadata{
		ID: parentID, CorrelationID: correlationID, Headers: map[string]string{"parent": "private"},
	})
	result, err := messenger.BindQuerier(instance, query).Query(ctx, findArticle{ID: 42})
	if err != nil {
		t.Fatalf("query: %v", err)
	}
	if result != (articleView{ID: 42, Title: "typed result"}) {
		t.Fatalf("result = %#v", result)
	}
	if got, want := fmt.Sprint(middlewareOrder), "[before:article.find.v1 handler after:article.find.v1]"; got != want {
		t.Fatalf("middleware order = %s, want %s", got, want)
	}
	if encodeCalls.Load() != 0 || propagator.calls.Load() != 1 {
		t.Fatalf("encode calls=%d propagator calls=%d", encodeCalls.Load(), propagator.calls.Load())
	}
	if len(observer.observations) != 2 || observer.observations[0].Operation != messenger.OperationHandle ||
		observer.observations[1].Operation != messenger.OperationQuery {
		t.Fatalf("observations = %#v", observer.observations)
	}
	for _, observation := range observer.observations {
		if observation.Kind != messenger.KindQuery || observation.Name != "article.find" ||
			observation.Route != "local.sync" || observation.HandlerID != "article.find.v1" {
			t.Fatalf("observation = %#v", observation)
		}
	}
}

func TestMessengerQueryPreservesZeroPointerAndInterfaceResults(t *testing.T) {
	type item struct{ Value int }
	zeroQuery := messenger.MustQuery[string, int]("result.zero", 1, messenger.JSON[string]())
	pointerQuery := messenger.MustQuery[string, *item]("result.pointer", 1, messenger.JSON[string]())
	interfaceQuery := messenger.MustQuery[string, any]("result.interface", 1, messenger.JSON[string]())
	nilInterfaceQuery := messenger.MustQuery[string, any]("result.interface.nil", 1, messenger.JSON[string]())
	nilInterfaceRequestQuery := messenger.MustQuery[any, string]("request.interface.nil", 1, messenger.JSON[any]())
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleQueryFunc(zeroQuery, "zero", func(context.Context, string) (int, error) { return 0, nil })
	builder.HandleQueryFunc(pointerQuery, "pointer", func(context.Context, string) (*item, error) {
		return nil, nil //nolint:nilnil // A nil pointer is a valid typed query result.
	})
	builder.HandleQueryFunc(interfaceQuery, "interface", func(context.Context, string) (any, error) {
		var value *item
		return value, nil
	})
	builder.HandleQueryFunc(nilInterfaceQuery, "interface.nil", func(context.Context, string) (any, error) {
		return nil, nil //nolint:nilnil // A nil interface is a valid present query result.
	})
	builder.HandleQueryFunc(nilInterfaceRequestQuery, "request.interface.nil", func(_ context.Context, value any) (string, error) {
		if value != nil {
			return "", fmt.Errorf("unexpected request value: %#v", value)
		}
		return "nil", nil
	})
	for _, route := range []func(){
		func() { builder.RouteQuery(zeroQuery, messenger.NewLocalSyncRoute()) },
		func() { builder.RouteQuery(pointerQuery, messenger.NewLocalSyncRoute()) },
		func() { builder.RouteQuery(interfaceQuery, messenger.NewLocalSyncRoute()) },
		func() { builder.RouteQuery(nilInterfaceQuery, messenger.NewLocalSyncRoute()) },
		func() { builder.RouteQuery(nilInterfaceRequestQuery, messenger.NewLocalSyncRoute()) },
	} {
		route()
	}
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if value, err := instance.Query(t.Context(), zeroQuery, "x"); err != nil || value != 0 {
		t.Fatalf("zero result = %d, %v", value, err)
	}
	if value, err := instance.Query(t.Context(), pointerQuery, "x"); err != nil || value != nil {
		t.Fatalf("pointer result = %#v, %v", value, err)
	}
	value, err := instance.Query(t.Context(), interfaceQuery, "x")
	if err != nil {
		t.Fatalf("typed nil interface: %v", err)
	}
	if reflected := reflect.ValueOf(value); !reflected.IsValid() || reflected.Kind() != reflect.Pointer || !reflected.IsNil() {
		t.Fatalf("typed nil interface result = %#v", value)
	}
	if value, err := instance.Query(t.Context(), nilInterfaceQuery, "x"); err != nil || value != nil {
		t.Fatalf("nil interface result = %#v, %v", value, err)
	}
	if value, err := instance.Query(t.Context(), nilInterfaceRequestQuery, nil); err != nil || value != "nil" {
		t.Fatalf("nil interface request = %#v, %v", value, err)
	}
}

func TestQueryMiddlewareErrorsPanicsAndSyntheticResults(t *testing.T) {
	handlerErr := errors.New("lookup failed")
	query := messenger.MustQuery[int, string]("middleware.query", 1, messenger.JSON[int]())

	t.Run("handler error", func(t *testing.T) {
		instance := buildQueryMessenger(t, query, nil, func(context.Context, messenger.Message[int]) (string, error) {
			return "", handlerErr
		})
		if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, handlerErr) {
			t.Fatalf("handler error = %v", err)
		}
	})

	t.Run("panic", func(t *testing.T) {
		instance := buildQueryMessenger(t, query, nil, func(context.Context, messenger.Message[int]) (string, error) {
			panic("boom")
		})
		_, err := instance.Query(t.Context(), query, 1)
		if err == nil || !strings.Contains(err.Error(), "handler query.handler panicked") {
			t.Fatalf("panic error = %v", err)
		}
		if strings.Contains(err.Error(), "boom") || strings.Contains(err.Error(), "runtime/debug") {
			t.Fatalf("panic details escaped through error: %v", err)
		}
	})

	t.Run("short circuit missing", func(t *testing.T) {
		instance := buildQueryMessenger(t, query, []messenger.Middleware{
			func(context.Context, messenger.Metadata, string, messenger.HandlerFunc) error { return nil },
		}, func(context.Context, messenger.Message[int]) (string, error) { return "unreachable", nil })
		if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, messenger.ErrQueryResultMissing) {
			t.Fatalf("short circuit error = %v", err)
		}
	})

	t.Run("swallowed handler error missing", func(t *testing.T) {
		instance := buildQueryMessenger(t, query, []messenger.Middleware{
			func(ctx context.Context, _ messenger.Metadata, _ string, next messenger.HandlerFunc) error {
				_ = next(ctx)
				return nil
			},
		}, func(context.Context, messenger.Message[int]) (string, error) { return "", handlerErr })
		if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, messenger.ErrQueryResultMissing) {
			t.Fatalf("swallowed error = %v", err)
		}
	})

	t.Run("double next", func(t *testing.T) {
		var calls atomic.Int32
		instance := buildQueryMessenger(t, query, []messenger.Middleware{
			func(ctx context.Context, _ messenger.Metadata, _ string, next messenger.HandlerFunc) error {
				if err := next(ctx); err != nil {
					return err
				}
				return next(ctx)
			},
		}, func(context.Context, messenger.Message[int]) (string, error) {
			calls.Add(1)
			return "result", nil
		})
		if _, err := instance.Query(t.Context(), query, 1); !errors.Is(err, messenger.ErrInvalidMessage) {
			t.Fatalf("double next error = %v", err)
		}
		if calls.Load() != 1 {
			t.Fatalf("handler calls = %d", calls.Load())
		}
	})

	t.Run("typed synthetic result", func(t *testing.T) {
		var calls atomic.Int32
		handler := messenger.ChainQueryHandler(
			func(context.Context, messenger.Message[int]) (string, error) {
				calls.Add(1)
				return "handler", nil
			},
			func(messenger.QueryHandler[int, string]) messenger.QueryHandler[int, string] {
				return func(context.Context, messenger.Message[int]) (string, error) { return "cached", nil }
			},
		)
		instance := buildQueryMessenger(t, query, nil, handler)
		if result, err := instance.Query(t.Context(), query, 1); err != nil || result != "cached" {
			t.Fatalf("synthetic result = %q, %v", result, err)
		}
		if calls.Load() != 0 {
			t.Fatalf("wrapped handler calls = %d", calls.Load())
		}
		if messenger.ChainQueryHandler[int, string](handler, nil) != nil ||
			messenger.HandleQueryPayload[int, string](nil) != nil {
			t.Fatal("invalid typed query chain was accepted")
		}
	})
}

func TestQueryLoggingAndObservationsNeverExposePayloadOrResult(t *testing.T) {
	type secretQuery struct{ Secret string }
	type secretResult struct{ Secret string }
	query := messenger.MustQuery[secretQuery, secretResult]("secret.lookup", 1, messenger.JSON[secretQuery]())
	logger := &testLogger{}
	builder := messenger.NewBuilder(
		messenger.WithSource(testSource),
		messenger.WithObserver(messenger.NewLoggingObserver(logger)),
	)
	builder.HandleQueryFunc(query, "secret.lookup", func(context.Context, secretQuery) (secretResult, error) {
		return secretResult{Secret: "result-secret-value"}, nil
	})
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	if _, err := instance.Query(t.Context(), query, secretQuery{Secret: "payload-secret-value"}); err != nil {
		t.Fatalf("query: %v", err)
	}
	records := logger.snapshot()
	if len(records) != 2 {
		t.Fatalf("log records = %#v", records)
	}
	for _, record := range records {
		encoded := fmt.Sprint(record)
		if strings.Contains(encoded, "payload-secret-value") || strings.Contains(encoded, "result-secret-value") {
			t.Fatalf("query data leaked to logs: %s", encoded)
		}
		for _, attr := range record.attrs {
			if attr.Key == "payload" || attr.Key == "result" || attr.Key == "headers" {
				t.Fatalf("unsafe query log attribute %q", attr.Key)
			}
		}
	}
}

func TestMessengerQueryRejectsUnknownDescriptorNilContextAndWireEncoding(t *testing.T) {
	query := messenger.MustQuery[int, string]("catalog.lookup", 1, messenger.JSON[int]())
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleQueryFunc(query, "catalog.lookup", func(_ context.Context, value int) (string, error) {
		return strconv.Itoa(value), nil
	})
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	//nolint:staticcheck // Verifies nil context rejection.
	if _, err := instance.Query(nil, query, 1); !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("nil context = %v", err)
	}
	unknown := messenger.MustQuery[int, string]("catalog.unknown", 1, messenger.JSON[int]())
	if _, err := instance.Query(t.Context(), unknown, 1); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("unknown query = %v", err)
	}
	wrongResult := messenger.MustQuery[int, int]("catalog.lookup", 1, messenger.JSON[int]())
	if _, err := instance.Query(t.Context(), wrongResult, 1); !errors.Is(err, messenger.ErrDescriptorConflict) {
		t.Fatalf("wrong result descriptor = %v", err)
	}
	metadata := messenger.Metadata{
		ID:            mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000011"),
		Kind:          messenger.KindQuery,
		Name:          query.Info().Name,
		SchemaVersion: 1,
		Source:        testSource,
		Time:          time.Now().UTC(),
		CorrelationID: mustMessageID(t, "018f4f2c-4a00-7000-8000-000000000012"),
		ContentType:   "application/json",
	}
	_, err = messenger.MarshalEnvelope(metadata, []byte(`{"id":1}`), messenger.DataJSON)
	if !errors.Is(err, messenger.ErrInvalidMessage) {
		t.Fatalf("query envelope = %v", err)
	}
}

func TestQueryManifestIsDeterministicAndOmitsResultType(t *testing.T) {
	queryV2 := messenger.MustQuery[int, articleView]("article.find", 2, messenger.JSON[int]())
	queryV1 := messenger.MustQuery[int, string]("article.find", 1, messenger.JSON[int]())
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.HandleQueryFunc(queryV2, "article.find.v2", func(context.Context, int) (articleView, error) {
		return articleView{}, nil
	})
	builder.RouteQuery(queryV2, messenger.NewLocalSyncRoute())
	builder.HandleQueryFunc(queryV1, "article.find.v1", func(context.Context, int) (string, error) { return "", nil })
	builder.RouteQuery(queryV1, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build: %v", err)
	}
	manifest := instance.Manifest()
	if manifest.SpecVersion != "1.0" || len(manifest.Descriptors) != 2 ||
		manifest.Descriptors[0].SchemaVersion != 1 || manifest.Descriptors[1].SchemaVersion != 2 {
		t.Fatalf("manifest = %#v", manifest)
	}
	for _, descriptor := range manifest.Descriptors {
		if descriptor.Kind != messenger.KindQuery || descriptor.Route != "local.sync" || len(descriptor.HandlerIDs) != 1 {
			t.Fatalf("query manifest descriptor = %#v", descriptor)
		}
	}
	encoded, err := instance.MarshalManifest()
	if err != nil {
		t.Fatalf("marshal manifest: %v", err)
	}
	if strings.Contains(strings.ToLower(string(encoded)), "result") {
		t.Fatalf("manifest leaked result identity: %s", encoded)
	}
	if err := manifest.Validate(); err != nil {
		t.Fatalf("manifest validate: %v", err)
	}
	missingRoute := manifest
	missingRoute.Descriptors = append([]messenger.ManifestDescriptor(nil), manifest.Descriptors...)
	missingRoute.Descriptors[0].Route = ""
	if err := missingRoute.Validate(); !errors.Is(err, messenger.ErrRouteNotFound) {
		t.Fatalf("missing query route = %v", err)
	}
	missingHandler := manifest
	missingHandler.Descriptors = append([]messenger.ManifestDescriptor(nil), manifest.Descriptors...)
	missingHandler.Descriptors[0].HandlerIDs = nil
	if err := missingHandler.Validate(); !errors.Is(err, messenger.ErrHandlerNotFound) {
		t.Fatalf("missing query handler = %v", err)
	}
	multipleHandlers := manifest
	multipleHandlers.Descriptors = append([]messenger.ManifestDescriptor(nil), manifest.Descriptors...)
	multipleHandlers.Descriptors[0].HandlerIDs = []string{"article.find.v1", "article.find.shadow"}
	if err := multipleHandlers.Validate(); !errors.Is(err, messenger.ErrHandlerConflict) {
		t.Fatalf("multiple query handlers = %v", err)
	}
}

func buildQueryMessenger[Q, R any](
	t *testing.T,
	query messenger.Query[Q, R],
	middlewares []messenger.Middleware,
	handler messenger.QueryHandler[Q, R],
) *messenger.Messenger {
	t.Helper()
	builder := messenger.NewBuilder(messenger.WithSource(testSource))
	builder.UseMiddleware(middlewares...)
	builder.HandleQuery(query, "query.handler", handler)
	builder.RouteQuery(query, messenger.NewLocalSyncRoute())
	instance, _, err := builder.Build()
	if err != nil {
		t.Fatalf("build query messenger: %v", err)
	}
	return instance
}

var (
	_ messenger.LocalQueryRoute                   = (*messenger.LocalSyncRoute)(nil)
	_ messenger.LocalQueryRoute                   = (*messenger.LocalAsyncRoute)(nil)
	_ messenger.Querier[findArticle, articleView] = queryFacadeProbe{}
)

type queryFacadeProbe struct{}

func (queryFacadeProbe) Query(context.Context, findArticle) (articleView, error) {
	return articleView{}, nil
}
