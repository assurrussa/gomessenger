package capacity

import "runtime/debug"

const (
	outboxModulePath = "github.com/assurrussa/outbox"
	unknownValue     = "unknown"
)

func outboxModuleVersion() string {
	build, ok := debug.ReadBuildInfo()
	if !ok {
		return unknownValue
	}
	return dependencyVersion(build.Deps, outboxModulePath)
}

func dependencyVersion(dependencies []*debug.Module, path string) string {
	for _, dependency := range dependencies {
		if dependency == nil || dependency.Path != path {
			continue
		}
		if dependency.Replace != nil {
			if dependency.Replace.Version != "" {
				return dependency.Replace.Version
			}
			return "devel (local replace)"
		}
		if dependency.Version != "" {
			return dependency.Version
		}
		return unknownValue
	}
	return unknownValue
}
