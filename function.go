package cloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"reflect"
	"runtime"
	"testing"
)

var (
	// registry of all delayed functions
	funcs = make(map[string]*Function)

	// precomputed types
	contextType = reflect.TypeOf((*context.Context)(nil)).Elem()
	errorType   = reflect.TypeOf((*error)(nil)).Elem()
)

// Handler should be implemented by any task function.
type Handler func(ctx context.Context, task *Task) error

// Function is a stored task implementation.
type Function struct {
	key string
	h   Handler
	err error
}

// Func builds and registers a new task implementation.
func Func(key string, h Handler) *Function {
	f := &Function{
		key: key,
		h:   h,
	}

	if old := funcs[f.key]; old != nil {
		_, file, _, _ := runtime.Caller(1)
		old.err = fmt.Errorf("cloudtasks: multiple functions registered for %s in %s", key, file)
	}
	funcs[f.key] = f

	return f
}

// Task builds a task invocation to the function. You can later send the task
// in batches using queue.SendTasks() or directly invoke Call() to make both things
// at the same time.
//
// The payload can be a proto.Message or any other kind of interface that can
// be serialized to JSON. It would then be read on the task.
func (f *Function) Task(payload interface{}, opts ...TaskOption) (*Task, error) {
	if f.err != nil {
		return nil, f.err
	}

	task := &Task{
		key: f.key,
	}
	for _, opt := range opts {
		opt(task)
	}

	var err error
	task.payload, err = json.Marshal(payload)
	if err != nil {
		return nil, fmt.Errorf("cloudtasks: cannot marshal task payload %T: %w", payload, err)
	}

	// Cloud Tasks enforces a 100KiB limit on every task. We proactively alert
	// before that limit to improve our error messages with the payload prefix.
	if len(task.payload) > 95*1024 {
		return nil, fmt.Errorf("cloudtasks: task has a payload longer than 95KiB: %d bytes: %v ...", len(task.payload), string(task.payload[:100]))
	}

	return task, nil
}

// Call builds a task invocation and directly sends it individually to the queue.
//
// If you are going to send multiple tasks at the same time is more efficient to
// build all of them with Task() first and then send them in batches with queue.SendTasks().
// If sending a single task this function will be similar in performance to the batch
// method described before.
func (f *Function) Call(ctx context.Context, queue Queue, payload interface{}, opts ...TaskOption) error {
	_, err := f.CallTask(ctx, queue, payload, opts...)
	return err
}

// CallTask builds a task invocation, sends it and returns the built task.
func (f *Function) CallTask(ctx context.Context, queue Queue, payload interface{}, opts ...TaskOption) (*Task, error) {
	task, err := f.Task(payload, opts...)
	if err != nil {
		return nil, err
	}
	return task, queue.Send(ctx, task)
}

// TestCall makes a direct call to the handler with the payload as incoming payload.
// It requires a testing argument to be sure it is only used in tests.
func (f *Function) TestCall(t *testing.T, payload interface{}) error {
	send, err := f.Task(payload)
	if err != nil {
		return err
	}

	task := &Task{
		key:     f.key,
		name:    generateRandomString(10),
		payload: send.payload,
	}
	return f.h(context.Background(), task)
}

func generateRandomString(n int) string {
	var letters = []rune("abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789")
	b := make([]rune, n)
	for i := range b {
		b[i] = letters[rand.Intn(len(letters))]
	}
	return string(b)
}
