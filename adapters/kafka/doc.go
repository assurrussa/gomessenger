// Package kafka provides native-envelope routes and durable at-least-once
// Kafka consumers for GoMessenger.
//
// The package owns franz-go clients so it can enforce transactional producer,
// consumer-group, and read-committed settings. Hosts still supply broker
// addresses, authentication and TLS options, stable instance identity,
// topology intent, process supervision, and database connections.
package kafka
