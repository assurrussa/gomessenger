package inboxcapacity

import (
	"fmt"
	"net/url"
)

func withApplicationName(dsn, applicationName string) (string, error) {
	parsed, err := url.Parse(dsn)
	if err != nil {
		return "", fmt.Errorf("parse PostgreSQL workload DSN: %w", err)
	}
	query := parsed.Query()
	query.Set("application_name", applicationName)
	parsed.RawQuery = query.Encode()
	return parsed.String(), nil
}
