package workflows_test

import (
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/altipla-consulting/errors"
	"github.com/stretchr/testify/require"

	"github.com/altipla-consulting/cloudtasks"
	"github.com/altipla-consulting/cloudtasks/workflows"
)

var waitCh = make(chan struct{})

func waitWorkflow(t *testing.T) {
	select {
	case <-waitCh:
		return
	case <-time.After(10 * time.Second):
		require.Fail(t, "workflows: timed out")
	}
}

var queue = cloudtasks.NewQueue("test-workflows")

func initTestbed() {
	slog.SetLogLoggerLevel(slog.LevelDebug)
}

var simpleWorkflow = workflows.Define("simple", func(run *workflows.Run[any]) error {
	err := workflows.Step(run, "step-a", func(ctx context.Context) error {
		slog.Debug("step a")
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = workflows.Step(run, "step-b", func(ctx context.Context) error {
		slog.Debug("step b")
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = workflows.Step(run, "end", func(ctx context.Context) error {
		waitCh <- struct{}{}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
})

func TestSimple(t *testing.T) {
	initTestbed()

	require.NoError(t, simpleWorkflow.Start(context.Background(), queue, nil))
	waitWorkflow(t)
}

var returnWorkflow = workflows.Define("return", func(run *workflows.Run[string]) error {
	stepA, err := workflows.StepReturn(run, "step-a", func(ctx context.Context) (string, error) {
		if run.Payload != "start-payload" {
			return "", errors.Errorf("run.Payload is %v, expected start-payload", run.Payload)
		}

		slog.Debug("step a")
		return "step-a-return", nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	stepB, err := workflows.StepReturn(run, "step-b", func(ctx context.Context) (int32, error) {
		if stepA != "step-a-return" {
			return 0, errors.Errorf("step a returned %v, expected step-a-return", stepA)
		}

		slog.Debug("step b")
		return 5, nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	err = workflows.Step(run, "end", func(ctx context.Context) error {
		if stepB != 5 {
			return errors.Errorf("step b returned %v, expected 5", stepB)
		}

		waitCh <- struct{}{}
		return nil
	})
	if err != nil {
		return errors.Trace(err)
	}

	return nil
})

func TestReturn(t *testing.T) {
	initTestbed()

	require.NoError(t, returnWorkflow.Start(context.Background(), queue, "start-payload"))
	waitWorkflow(t)
}

var errorWorkflow = workflows.Define("error", func(run *workflows.Run[any]) error {
	err := workflows.Step(run, "step-a", func(ctx context.Context) error {
		return errors.New("step-a-error")
	})
	if err != nil {
		waitCh <- struct{}{}
		return errors.Trace(err)
	}

	return nil
})

func TestError(t *testing.T) {
	initTestbed()

	require.NoError(t, errorWorkflow.Start(context.Background(), queue, nil))
	waitWorkflow(t)
}
