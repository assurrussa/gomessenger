package messenger

import "context"

// ContextPropagator injects and extracts distributed context through immutable
// message headers.
type ContextPropagator interface {
	Inject(ctx context.Context, carrier map[string]string)
	Extract(ctx context.Context, carrier map[string]string) context.Context
}

type noopContextPropagator struct{}

func (noopContextPropagator) Inject(context.Context, map[string]string) {}

func (noopContextPropagator) Extract(ctx context.Context, _ map[string]string) context.Context {
	return ctx
}

// NoopContextPropagator returns a propagator that leaves contexts and carriers
// unchanged.
func NoopContextPropagator() ContextPropagator { return noopContextPropagator{} }
