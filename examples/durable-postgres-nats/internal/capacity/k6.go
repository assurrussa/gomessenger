package capacity

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"time"
)

type k6Process struct {
	command     *exec.Cmd
	done        chan error
	logFile     *os.File
	summaryPath string
	cancel      context.CancelFunc
}

type rawK6Summary struct {
	Metrics map[string]rawK6Metric `json:"metrics"`
}

type rawK6Metric struct {
	Values map[string]float64 `json:"values"`
}

func startK6(
	ctx context.Context,
	config Config,
	stageID string,
	rate int,
	duration time.Duration,
) (*k6Process, error) {
	stageBase := filepath.Join(config.ResultDir(), "k6-"+stageID)
	logFile, err := os.OpenFile(stageBase+".log", os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o640)
	if err != nil {
		return nil, fmt.Errorf("create k6 stage log: %w", err)
	}
	summaryPath := stageBase + ".json"
	preallocatedVUs := max(16, int(math.Ceil(float64(rate)*0.25)))
	maximumVUs := max(preallocatedVUs, int(math.Ceil(float64(rate)*config.E2EP95SLO.Seconds()*1.25)))
	processCtx, cancel := context.WithCancel(ctx)
	// The binary and script are explicit local capacity-runner settings, never HTTP input.
	//nolint:gosec // Running that configured executable is the controller's purpose.
	command := exec.CommandContext(processCtx, config.K6Binary, "run", config.K6Script)
	command.Env = append(os.Environ(),
		"CAPACITY_APP_URL="+config.AppURL,
		"CAPACITY_RUN_ID="+config.RunID,
		"CAPACITY_STAGE_ID="+stageID,
		"CAPACITY_RATE="+strconv.Itoa(rate),
		"CAPACITY_DURATION="+duration.String(),
		"CAPACITY_PREALLOCATED_VUS="+strconv.Itoa(preallocatedVUs),
		"CAPACITY_MAX_VUS="+strconv.Itoa(maximumVUs),
		"CAPACITY_K6_SUMMARY="+summaryPath,
	)
	writer := io.MultiWriter(os.Stdout, logFile)
	command.Stdout = writer
	command.Stderr = writer
	if err := command.Start(); err != nil {
		cancel()
		_ = logFile.Close()
		return nil, fmt.Errorf("start k6 stage %s: %w", stageID, err)
	}
	process := &k6Process{
		command: command, done: make(chan error, 1), logFile: logFile, summaryPath: summaryPath,
		cancel: cancel,
	}
	go func() {
		process.done <- command.Wait()
	}()
	return process, nil
}

func (p *k6Process) finish(commandErr error) (K6Result, error) {
	p.cancel()
	closeErr := p.logFile.Close()
	exitCode := 0
	if commandErr != nil {
		var exitErr *exec.ExitError
		if !errors.As(commandErr, &exitErr) {
			return K6Result{}, errors.Join(fmt.Errorf("run k6: %w", commandErr), closeErr)
		}
		exitCode = exitErr.ExitCode()
	}
	if closeErr != nil {
		return K6Result{}, fmt.Errorf("close k6 log: %w", closeErr)
	}
	return readK6Result(p.summaryPath, exitCode)
}

func readK6Result(summaryPath string, exitCode int) (K6Result, error) {
	summaryData, err := os.ReadFile(summaryPath)
	if err != nil {
		return K6Result{}, fmt.Errorf("read k6 summary: %w", err)
	}
	var summary rawK6Summary
	if err := json.Unmarshal(summaryData, &summary); err != nil {
		return K6Result{}, fmt.Errorf("decode k6 summary: %w", err)
	}
	iterations, err := requiredMetric(summary, "iterations", "count")
	if err != nil {
		return K6Result{}, err
	}
	accepted, err := requiredMetric(summary, "accepted_orders", "count")
	if err != nil {
		return K6Result{}, err
	}
	httpRequests, err := requiredMetric(summary, "http_reqs", "count")
	if err != nil {
		return K6Result{}, err
	}
	return K6Result{
		ExitCode:             exitCode,
		Iterations:           int64(math.Round(iterations)),
		DroppedIterations:    int64(math.Round(optionalMetric(summary, "dropped_iterations", "count"))),
		OfferedIterations:    int64(math.Round(iterations + optionalMetric(summary, "dropped_iterations", "count"))),
		AcceptedOrders:       int64(math.Round(accepted)),
		HTTPRequests:         int64(math.Round(httpRequests)),
		HTTPFailureRate:      optionalMetric(summary, "http_req_failed", "rate"),
		AcceptedRate:         optionalMetric(summary, "order_accepted", "rate"),
		HTTPRequestP95Millis: optionalMetric(summary, "http_req_duration", "p(95)"),
	}, nil
}

func (p *k6Process) abort() error {
	p.cancel()
	commandErr := <-p.done
	closeErr := p.logFile.Close()
	if commandErr != nil && !errors.Is(commandErr, context.Canceled) {
		var exitErr *exec.ExitError
		if !errors.As(commandErr, &exitErr) {
			return errors.Join(commandErr, closeErr)
		}
	}
	return closeErr
}

func requiredMetric(summary rawK6Summary, name, value string) (float64, error) {
	metric, ok := summary.Metrics[name]
	if !ok {
		return 0, fmt.Errorf("k6 summary is missing metric %q", name)
	}
	result, ok := metric.Values[value]
	if !ok {
		return 0, fmt.Errorf("k6 metric %q is missing value %q", name, value)
	}
	return result, nil
}

func optionalMetric(summary rawK6Summary, name, value string) float64 {
	metric, ok := summary.Metrics[name]
	if !ok {
		return 0
	}
	return metric.Values[value]
}
