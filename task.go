package cloudtasks

import (
	"crypto/md5"
	"encoding/json"
	"fmt"
	"log/slog"
	"strings"
)

// Task is a task sent or receieved from a queue.
type Task struct {
	key     string // key we are invoking
	name    string // name of the task, autogenerated if empty
	payload []byte // body of the request

	// Read only. The current retry count.
	Retries int64

	// Queue that received the task.
	Queue Queue
}

// Name returns the name of the task. By default it is autogenerated.
func (task *Task) Name() string {
	return task.name
}

// Read unmarshals the task payload into the provided destination.
func (task *Task) Read(dest interface{}) error {
	if err := json.Unmarshal(task.payload, dest); err != nil {
		return fmt.Errorf("cloudtasks: cannot read task payload: %w", err)
	}
	return nil
}

// LogValue implements slog.LogValuer.
func (task *Task) LogValue() slog.Value {
	return slog.GroupValue(
		slog.String("name", task.name),
		slog.String("key", task.key),
		slog.String("payload", string(task.payload)),
		slog.Int64("retries", task.Retries),
	)
}

var _ slog.LogValuer = new(Task)

// TaskOption configures tasks when creating them.
type TaskOption func(task *Task)

// WithName configures a custom name for the task. By default it will be autogenerated. A custom name could be problematic
// with tombstones (task names that can't be repeated) and concurrency controls, so assign it with care and read
// Google Cloud Tasks documentation before using it.
func WithName(name string) TaskOption {
	return func(task *Task) {
		task.name = name
	}
}

// ExternalTask should be filled with the data of the task to call in an external Cloud Run application.
type ExternalTask struct {
	URL     string
	Payload any
	name    string
}

var _ slog.LogValuer = new(ExternalTask)

// LogValue implements slog.LogValuer.
func (task *ExternalTask) LogValue() slog.Value {
	payload, err := json.Marshal(task.Payload)
	if err != nil {
		return slog.GroupValue(slog.String("url", task.URL), slog.String("payload-err", err.Error()))
	}
	return slog.GroupValue(slog.String("url", task.URL), slog.String("payload", string(payload)))
}

// generateTaskName generates name with hash.
func generateTaskName(parent, name string) string {
	if name != "" {
		hash := fmt.Sprintf("%x", md5.Sum([]byte(string(name))))[:8]
		return strings.Join([]string{parent, "tasks", hash}, "/")
	}
	return name
}
