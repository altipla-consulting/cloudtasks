package workflows

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"

	"github.com/altipla-consulting/errors"
	"github.com/segmentio/ksuid"

	"github.com/altipla-consulting/cloudtasks"
)

var errWorkflowRunStep = errors.New("workflows: run step")

type Workflow[TPayload any] struct {
	name string
	def  func(run *Run[TPayload]) error
	task *cloudtasks.Function
}

type Run[TPayload any] struct {
	Payload TPayload

	ctx   context.Context
	task  *cloudtasks.Function
	queue cloudtasks.Queue
	step  int
	state runState[TPayload]
}

func Define[TPayload any](name string, def func(run *Run[TPayload]) error) *Workflow[TPayload] {
	w := &Workflow[TPayload]{
		name: name,
		def:  def,
	}
	w.task = cloudtasks.Func("workflow:"+name, w.runStepFn)
	return w
}

type runState[TPayload any] struct {
	ID      string
	Payload TPayload
	Step    int
	Names   []string
	Returns []json.RawMessage
}

func (s *runState[TPayload]) taskName() string {
	return fmt.Sprintf("%s:%d", s.ID, s.Step)
}

func (w *Workflow[TPayload]) Start(ctx context.Context, queue cloudtasks.Queue, payload TPayload) error {
	state := runState[TPayload]{
		ID:      ksuid.New().String(),
		Payload: payload,
	}
	return errors.Trace(w.task.Call(ctx, queue, state, cloudtasks.WithName(state.taskName())))
}

func (w *Workflow[TPayload]) runStepFn(ctx context.Context, task *cloudtasks.Task) error {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok && errors.Is(err, errWorkflowRunStep) {
				return
			}
			panic(r)
		}
	}()

	var state runState[TPayload]
	if err := task.Read(&state); err != nil {
		return errors.Trace(err)
	}

	slog.Debug("workflows: run step", slog.String("workflow", w.name), slog.Int("step", state.Step), slog.String("run-id", state.ID))

	run := &Run[TPayload]{
		Payload: state.Payload,
		ctx:     ctx,
		task:    w.task,
		queue:   task.Queue,
		state:   state,
	}
	if err := w.def(run); err != nil {
		return errors.Trace(err)
	}

	return nil
}

func Step[TPayload any](run *Run[TPayload], name string, step func(ctx context.Context) error) error {
	_, err := StepReturn(run, name, func(ctx context.Context) (bool, error) {
		return false, errors.Trace(step(ctx))
	})
	return errors.Trace(err)
}

func StepReturn[TReturn any, TPayload any](run *Run[TPayload], name string, step func(ctx context.Context) (TReturn, error)) (TReturn, error) {
	defer func() { run.step++ }()

	fullname := fmt.Sprintf("%s:%d", name, run.step)

	if run.step < run.state.Step {
		if run.state.Names[run.step] == fullname {
			var ret TReturn
			if err := json.Unmarshal(run.state.Returns[run.step], &ret); err != nil {
				return ret, errors.Trace(err)
			}
			return ret, nil
		}
		return *new(TReturn), errors.Errorf("workflows: step %s called in the wrong order, expected %s", fullname, run.state.Names[run.step])
	}

	ret, err := step(run.ctx)
	if err != nil {
		return ret, errors.Trace(err)
	}

	run.state.Names = append(run.state.Names, fullname)
	raw, err := json.Marshal(ret)
	if err != nil {
		return ret, errors.Trace(err)
	}
	run.state.Returns = append(run.state.Returns, raw)
	run.state.Step++
	if err := run.task.Call(run.ctx, run.queue, run.state, cloudtasks.WithName(run.state.taskName())); err != nil {
		return ret, errors.Errorf("workflows: failed to call step %s: %w", fullname, err)
	}

	panic(errWorkflowRunStep)
}
