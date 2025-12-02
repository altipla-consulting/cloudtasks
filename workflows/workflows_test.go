package workflows_test

import (
	"context"
	"log/slog"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/altipla-consulting/errors"
	"github.com/altipla-consulting/telemetry"
	"github.com/altipla-consulting/telemetry/logging"
	"github.com/segmentio/ksuid"
	"github.com/stretchr/testify/require"

	"github.com/altipla-consulting/cloudtasks"
	"github.com/altipla-consulting/cloudtasks/workflows"
)

var waitCh = make(chan struct{})

func waitWorkflow(t *testing.T, timeout time.Duration) {
	select {
	case <-waitCh:
		return
	case <-time.After(timeout):
		require.Fail(t, "workflows: timed out")
	}
}

var queue = cloudtasks.NewQueue(
	"cloudtasks-tests",
	cloudtasks.WithForcedProductionMode(),
	cloudtasks.WithHostname(os.Getenv("REMOTE_HOSTNAME")),
	cloudtasks.WithProject("altipla-dev", "709027458951"),
	cloudtasks.WithRegion("europe-west1"),
	cloudtasks.WithServiceAccountEmail("cloudtasks-tests@altipla-dev.iam.gserviceaccount.com"),
)

func init() {
	telemetry.Configure(logging.Debug())
}

func initTestbed(t *testing.T) {
	if os.Getenv("REMOTE_HOSTNAME") == "" {
		t.Skip("skipping remote test without REMOTE_HOSTNAME env variable")
	}

	mux := http.NewServeMux()
	mux.Handle(cloudtasks.Handler())
	server := &http.Server{
		Addr:    "0.0.0.0:25000",
		Handler: mux,
	}
	go func() {
		require.NoError(t, server.ListenAndServe())
	}()
	slog.Debug("test server started")
	t.Cleanup(func() {
		require.NoError(t, server.Close())
		slog.Debug("test server stopped")
	})
}

var simpleWorkflow = workflows.Define("simple", func(run *workflows.Run[any]) error {
	workflows.Step(run, "step-a", func(ctx context.Context) error {
		slog.Debug("step a")
		return nil
	})

	workflows.Step(run, "step-b", func(ctx context.Context) error {
		slog.Debug("step b")
		return nil
	})

	waitCh <- struct{}{}
	return nil
})

func TestSimple(t *testing.T) {
	initTestbed(t)

	require.NoError(t, simpleWorkflow.Start(context.Background(), queue, nil))
	waitWorkflow(t, 10*time.Second)
}

func TestSimpleWithName(t *testing.T) {
	initTestbed(t)

	name := ksuid.New().String()
	require.NoError(t, simpleWorkflow.Start(context.Background(), queue, "payload", workflows.WithName(name)))
	waitWorkflow(t, 10*time.Second)
}

var returnWorkflow = workflows.Define("return", func(run *workflows.Run[string]) error {
	stepA := workflows.StepReturn(run, "step-a", func(ctx context.Context) (string, error) {
		if run.Payload != "start-payload" {
			return "", errors.Errorf("run.Payload is %v, expected start-payload", run.Payload)
		}

		slog.Debug("step a")
		return "step-a-return", nil
	})

	stepB := workflows.StepReturn(run, "step-b", func(ctx context.Context) (int32, error) {
		if stepA != "step-a-return" {
			return 0, errors.Errorf("step a returned %v, expected step-a-return", stepA)
		}

		slog.Debug("step b")
		return 5, nil
	})

	workflows.Step(run, "end", func(ctx context.Context) error {
		if stepB != 5 {
			return errors.Errorf("step b returned %v, expected 5", stepB)
		}

		return nil
	})

	waitCh <- struct{}{}
	return nil
})

func TestReturn(t *testing.T) {
	initTestbed(t)

	require.NoError(t, returnWorkflow.Start(context.Background(), queue, "start-payload"))
	waitWorkflow(t, 10*time.Second)
}

func TestReturnWithName(t *testing.T) {
	initTestbed(t)

	name := ksuid.New().String()
	require.NoError(t, returnWorkflow.Start(context.Background(), queue, "start-payload", workflows.WithName(name)))
	waitWorkflow(t, 10*time.Second)
}

var panicContinuesAfterRetries = workflows.Define("panic-continues-after-retries", func(run *workflows.Run[any]) error {
	workflows.Step(run, "step-a", func(ctx context.Context) error {
		slog.Debug("step a", slog.Int64("retries", run.TaskRetries))
		if run.TaskRetries < 3 {
			panic("step a panic")
		}
		return nil
	})

	workflows.Step(run, "step-b", func(ctx context.Context) error {
		slog.Debug("step b")
		return nil
	})

	waitCh <- struct{}{}
	return nil
})

func TestPanicContinuesAfterRetries(t *testing.T) {
	initTestbed(t)

	require.NoError(t, panicContinuesAfterRetries.Start(context.Background(), queue, nil))
	waitWorkflow(t, 10*time.Second)
}

var interrupted = workflows.Define("interrupted", func(run *workflows.Run[any]) error {
	workflows.Step(run, "step-a", func(ctx context.Context) error {
		return errors.New("step error")
	})

	workflows.Step(run, "step-b", func(ctx context.Context) error {
		slog.Error("never reached")
		return nil
	})

	waitCh <- struct{}{}
	return nil
})

func TestInterrupted(t *testing.T) {
	initTestbed(t)

	require.NoError(t, interrupted.Start(context.Background(), queue, nil))
	waitWorkflow(t, 1*time.Minute)
}
