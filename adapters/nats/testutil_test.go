package nats_test

const (
	testNamespace        = "test"
	testEventName        = "media.processed"
	testContentType      = "application/json"
	testStreamName       = "MESSAGES"
	testDLQStreamName    = "MESSAGES_DLQ"
	testConsumerID       = "media-worker"
	testMessageSubject   = "test.event.media.processed.v1"
	testPermanentFailure = "permanent"
	testRejectedError    = "rejected"
	testTraceParent      = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
	testTraceState       = "vendor=value"
	testNativeMediaType  = "application/vnd.gomessenger+json; version=1.0"
	testOldBrokerID      = "old-id"
)
