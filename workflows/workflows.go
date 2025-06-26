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

// baseState contiene los campos comunes que no dependen del tipo genérico
type startOptions struct {
	ID string
}

func (s *runState[TPayload]) taskName() string {
	return fmt.Sprintf("%s:%d", s.ID, s.Step)
}

type runState[TPayload any] struct {
	startOptions
	Step    int
	Names   []string
	Returns []json.RawMessage
	Payload TPayload
}

// StartOption configura workflows cuando se crean, sin ser genérico
type StartOption func(state *startOptions)

// WithName configura un nombre personalizado para el workflow. Por defecto será autogenerado. Un nombre personalizado podría ser problemático
// con tombstones (nombres de workflow que no se pueden repetir) y controles de concurrencia, así que asígnalo con cuidado y lee
// la documentación de Google Cloud Tasks antes de usarlo.
func WithName(name string) StartOption {
	return func(state *startOptions) {
		state.ID = name
	}
}

func (w *Workflow[TPayload]) Start(ctx context.Context, queue cloudtasks.Queue, payload TPayload, opts ...StartOption) error {
	state := runState[TPayload]{
		startOptions: startOptions{
			ID: ksuid.New().String(),
		},
		Payload: payload,
	}

	for _, opt := range opts {
		opt(&state.startOptions)
	}

	return errors.Trace(w.task.Call(ctx, queue, state, cloudtasks.WithName(state.taskName())))
}

func (w *Workflow[TPayload]) runStepFn(ctx context.Context, task *cloudtasks.Task) (retErr error) {
	defer func() {
		if r := recover(); r != nil {
			if err, ok := r.(error); ok {
				if errors.Is(err, errWorkflowRunStep) {
					return
				}
				retErr = errors.Trace(err)
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

func Step[TPayload any](run *Run[TPayload], name string, step func(ctx context.Context) error) {
	StepReturn(run, name, func(ctx context.Context) (bool, error) {
		return false, step(ctx)
	})
}

func StepReturn[TReturn any, TPayload any](run *Run[TPayload], name string, step func(ctx context.Context) (TReturn, error)) TReturn {
	defer func() { run.step++ }()

	fullname := fmt.Sprintf("%s:%d", name, run.step)

	if run.step < run.state.Step {
		if run.state.Names[run.step] == fullname {
			var ret TReturn
			if err := json.Unmarshal(run.state.Returns[run.step], &ret); err != nil {
				panic(errors.Trace(err))
			}
			return ret
		}
		panic(errors.Errorf("workflows: step %s called in the wrong order, expected %s", fullname, run.state.Names[run.step]))
	}

	ret, err := step(run.ctx)
	if err != nil {
		panic(errors.Trace(err))
	}

	run.state.Names = append(run.state.Names, fullname)
	raw, err := json.Marshal(ret)
	if err != nil {
		panic(errors.Errorf("workflows: failed to serialize return value of %s: %w", fullname, err))
	}
	run.state.Returns = append(run.state.Returns, raw)
	run.state.Step++
	if err := run.task.Call(run.ctx, run.queue, run.state, cloudtasks.WithName(run.state.taskName())); err != nil {
		panic(errors.Errorf("workflows: failed to call step %s: %w", fullname, err))
	}

	panic(errWorkflowRunStep)
}
