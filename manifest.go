package messenger

import (
	"encoding/json"
	"fmt"
	"sort"
)

// ManifestSpecVersion is the current topology manifest contract.
const ManifestSpecVersion = "1.0"

// Manifest is a deterministic, secret-free description of runtime topology.
type Manifest struct {
	SpecVersion string               `json:"specVersion"`
	Source      string               `json:"source"`
	Descriptors []ManifestDescriptor `json:"descriptors"`
	Services    []string             `json:"services,omitempty"`
}

// ManifestDescriptor describes one typed descriptor and its static route.
type ManifestDescriptor struct {
	DescriptorInfo
	Route      string   `json:"route,omitempty"`
	HandlerIDs []string `json:"handlerIds,omitempty"`
}

// Validate checks manifest structure without constructing a runtime.
func (m Manifest) Validate() error {
	if m.SpecVersion != ManifestSpecVersion {
		return fmt.Errorf("%w: manifest specVersion must be %s", ErrInvalidMessage, ManifestSpecVersion)
	}
	if err := validateSource(m.Source); err != nil {
		return err
	}
	descriptors := make(map[descriptorKey]struct{}, len(m.Descriptors))
	for _, descriptor := range m.Descriptors {
		info := descriptor.DescriptorInfo
		if !info.Kind.valid() || len(info.Name) > maxDescriptorNameLength ||
			!descriptorNamePattern.MatchString(info.Name) ||
			info.SchemaVersion <= 0 || info.ContentType == "" || !info.DataEncoding.valid() {
			return fmt.Errorf("%w: manifest descriptor %s v%d", ErrInvalidDescriptor,
				info.Name, info.SchemaVersion)
		}
		key := keyFor(info)
		if _, exists := descriptors[key]; exists {
			return fmt.Errorf("%w: duplicate manifest descriptor %s v%d", ErrDescriptorConflict,
				info.Name, info.SchemaVersion)
		}
		descriptors[key] = struct{}{}
		if descriptor.Route != "" && !validStableID(descriptor.Route) {
			return fmt.Errorf("%w: invalid manifest route %q", ErrRouteConflict, descriptor.Route)
		}
		if info.Kind == KindCommand && len(descriptor.HandlerIDs) > 1 {
			return fmt.Errorf("%w: command %s v%d has multiple handlers", ErrHandlerConflict,
				info.Name, info.SchemaVersion)
		}
		handlers := make(map[string]struct{}, len(descriptor.HandlerIDs))
		for _, handlerID := range descriptor.HandlerIDs {
			if !validStableID(handlerID) {
				return fmt.Errorf("%w: invalid manifest handler %q", ErrHandlerConflict, handlerID)
			}
			if _, exists := handlers[handlerID]; exists {
				return fmt.Errorf("%w: duplicate manifest handler %q", ErrHandlerConflict, handlerID)
			}
			handlers[handlerID] = struct{}{}
		}
	}
	services := make(map[string]struct{}, len(m.Services))
	for _, serviceID := range m.Services {
		if !validStableID(serviceID) {
			return fmt.Errorf("%w: invalid manifest service %q", ErrServiceConflict, serviceID)
		}
		if _, exists := services[serviceID]; exists {
			return fmt.Errorf("%w: duplicate manifest service %q", ErrServiceConflict, serviceID)
		}
		services[serviceID] = struct{}{}
	}
	return nil
}

// Manifest returns a defensive copy of the messenger topology.
func (m *Messenger) Manifest() Manifest {
	copyManifest := m.manifest
	copyManifest.Descriptors = make([]ManifestDescriptor, len(m.manifest.Descriptors))
	for index, descriptor := range m.manifest.Descriptors {
		copyManifest.Descriptors[index] = descriptor
		copyManifest.Descriptors[index].HandlerIDs = append([]string(nil), descriptor.HandlerIDs...)
	}
	copyManifest.Services = append([]string(nil), m.manifest.Services...)
	return copyManifest
}

// MarshalManifest returns deterministic indented JSON suitable for gomessengerctl.
func (m *Messenger) MarshalManifest() ([]byte, error) {
	return json.MarshalIndent(m.Manifest(), "", "  ")
}

func buildManifest(
	source string,
	commands map[descriptorKey]commandBinding,
	events map[descriptorKey]eventBinding,
	services []namedService,
) Manifest {
	descriptors := make([]ManifestDescriptor, 0, len(commands)+len(events))
	for _, command := range commands {
		descriptor := ManifestDescriptor{DescriptorInfo: command.descriptor.info}
		if command.route != nil {
			descriptor.Route = command.route.Name()
		}
		if command.handlerID != "" {
			descriptor.HandlerIDs = []string{command.handlerID}
		}
		descriptors = append(descriptors, descriptor)
	}
	for _, event := range events {
		descriptor := ManifestDescriptor{DescriptorInfo: event.descriptor.info}
		if event.route != nil {
			descriptor.Route = event.route.Name()
		}
		for _, subscriber := range event.subscribers {
			descriptor.HandlerIDs = append(descriptor.HandlerIDs, subscriber.id)
		}
		descriptors = append(descriptors, descriptor)
	}
	sort.Slice(descriptors, func(i, j int) bool {
		if descriptors[i].Kind != descriptors[j].Kind {
			return descriptors[i].Kind < descriptors[j].Kind
		}
		if descriptors[i].Name != descriptors[j].Name {
			return descriptors[i].Name < descriptors[j].Name
		}
		return descriptors[i].SchemaVersion < descriptors[j].SchemaVersion
	})
	serviceIDs := make([]string, len(services))
	for index, service := range services {
		serviceIDs[index] = service.id
	}
	sort.Strings(serviceIDs)
	return Manifest{
		SpecVersion: ManifestSpecVersion,
		Source:      source,
		Descriptors: descriptors,
		Services:    serviceIDs,
	}
}
