package cloudtasks

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"os"
	"path"
	"strconv"
	"strings"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	pb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/compute/metadata"
	"github.com/VictoriaMetrics/metrics"
	"github.com/altipla-consulting/errors"
	"google.golang.org/api/idtoken"
)

var (
	allQueues []*gcloudQueue
)

// Queue abstract any remote or local system that can execute a task.
type Queue interface {
	// Send a new task to the queue.
	Send(ctx context.Context, task *Task) error

	// SendExternal sends a new task to an external URL.
	SendExternal(ctx context.Context, task *ExternalTask) error
}

// QueueOption configures queues when creating them.
type QueueOption func(*gcloudQueue)

// WithRegion configures a custom region for the queue. By default it will use the region of the Cloud Run service.
func WithRegion(region string) QueueOption {
	return func(queue *gcloudQueue) {
		queue.region = region
	}
}

// WithHostname configures the queue for an application outside of Cloud Run. Pass only the hostname, without the
// protocol or trailing slash.
func WithHostname(hostname string) QueueOption {
	return func(queue *gcloudQueue) {
		queue.audience = fmt.Sprintf("https://%s", hostname)
	}
}

type gcloudQueue struct {
	name string

	region   string
	audience string

	initErr                 error
	client                  *cloudtasks.Client
	project, numericProject string
	serviceAccountEmail     string
}

// NewQueue initializes a new queue.
func NewQueue(name string, opts ...QueueOption) Queue {
	// Local deployments won't have this environment variable defined.
	if os.Getenv("K_SERVICE") == "" {
		return &localQueue{name: name}
	}

	queue := &gcloudQueue{name: name}
	allQueues = append(allQueues, queue)

	for _, opt := range opts {
		opt(queue)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	var err error
	queue.client, err = cloudtasks.NewClient(ctx)
	if err != nil {
		queue.initErr = fmt.Errorf("cloudtasks: cannot initialize remote client: %w", err)
		return queue
	}

	queue.project, err = metadata.ProjectIDWithContext(ctx)
	if err != nil {
		queue.initErr = fmt.Errorf("cloudtasks: cannot get google project name: %w", err)
		return queue
	}
	if queue.region == "" {
		// Detect Cloud Run to read the region directly or extract it from the zone in other environments.
		if os.Getenv("K_CONFIGURATION") != "" || os.Getenv("CLOUD_RUN_JOB") != "" {
			region, err := metadata.GetWithContext(ctx, "instance/region")
			if err != nil {
				queue.initErr = fmt.Errorf("cloudtasks: cannot get google cloud run region: %w", err)
				return queue
			}
			queue.region = path.Base(region)
		} else {
			zone, err := metadata.ZoneWithContext(ctx)
			if err != nil {
				queue.initErr = fmt.Errorf("cloudtasks: cannot get google zone: %w", err)
				return queue
			}
			zone = path.Base(zone)
			queue.region = zone[:strings.LastIndex(zone, "-")]
		}
	}
	if queue.audience == "" {
		queue.numericProject, err = metadata.NumericProjectIDWithContext(ctx)
		if err != nil {
			queue.initErr = fmt.Errorf("cloudtasks: cannot get google numeric project: %w", err)
			return queue
		}
		queue.audience = fmt.Sprintf("https://%s-%s.%s.run.app", os.Getenv("K_SERVICE"), queue.numericProject, queue.region)
	}

	queue.serviceAccountEmail, err = metadata.EmailWithContext(ctx, "default")
	if err != nil {
		queue.initErr = fmt.Errorf("cloudtasks: cannot get default service account email: %w", err)
		return queue
	}

	return queue
}

func (queue *gcloudQueue) Send(ctx context.Context, task *Task) error {
	parent := strings.Join([]string{"projects", queue.project, "locations", queue.region, "queues", queue.name}, "/")
	req := &pb.CreateTaskRequest{
		Parent: parent,
		Task: &pb.Task{
			Name: generateTaskName(parent, task.name),
			MessageType: &pb.Task_HttpRequest{
				HttpRequest: &pb.HttpRequest{
					HttpMethod: pb.HttpMethod_POST,
					Url:        fmt.Sprintf("%s/_cloudtasks/%s", queue.audience, queue.name),
					Body:       task.payload,
					Headers: map[string]string{
						"Content-Type": "application/json",
						"Altipla-Task": task.key,
					},
					AuthorizationHeader: &pb.HttpRequest_OidcToken{
						OidcToken: &pb.OidcToken{
							ServiceAccountEmail: queue.serviceAccountEmail,
							Audience:            queue.audience,
						},
					},
				},
			},
		},
	}
	var lastErr error
	for i := 0; i < 3 && ctx.Err() == nil; i++ {
		if err := queue.createTask(ctx, req); err != nil {
			lastErr = fmt.Errorf("%w: %w", ErrCannotSendTask, err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	if lastErr != nil {
		return lastErr
	}

	metrics.GetOrCreateCounter(fmt.Sprintf("cloudtasks_sent_total{queue=%q,task=%q}", queue.name, task.key)).Inc()

	return nil
}

func (queue *gcloudQueue) SendExternal(ctx context.Context, task *ExternalTask) error {
	u, err := url.Parse(task.URL)
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot parse external task URL: %w", err)
	}
	payload, err := json.Marshal(task.Payload)
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot marshal task payload %T: %w", payload, err)
	}

	parent := strings.Join([]string{"projects", queue.project, "locations", queue.region, "queues", queue.name}, "/")
	req := &pb.CreateTaskRequest{
		Parent: parent,
		Task: &pb.Task{
			Name: generateTaskName(parent, task.name),
			MessageType: &pb.Task_HttpRequest{
				HttpRequest: &pb.HttpRequest{
					HttpMethod: pb.HttpMethod_POST,
					Url:        task.URL,
					Body:       payload,
					Headers:    map[string]string{"Content-Type": "application/json"},
					AuthorizationHeader: &pb.HttpRequest_OidcToken{
						OidcToken: &pb.OidcToken{
							ServiceAccountEmail: queue.serviceAccountEmail,
							Audience:            fmt.Sprintf("https://%s/", u.Hostname()),
						},
					},
				},
			},
		},
	}
	var lastErr error
	for i := 0; i < 3 && ctx.Err() == nil; i++ {
		if err := queue.createTask(ctx, req); err != nil {
			lastErr = fmt.Errorf("%w: %w", ErrCannotSendTask, err)
			time.Sleep(1 * time.Second)
			continue
		}
		break
	}
	if lastErr != nil {
		return lastErr
	}

	return nil
}

func (queue *gcloudQueue) createTask(ctx context.Context, req *pb.CreateTaskRequest) error {
	// Cloud Tasks enforces a timeout of less than 30 seconds server side. This will
	// ensure all our calls are less than the limit to avoid the InvalidArgument error.
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	_, err := queue.client.CreateTask(ctx, req)
	return err
}

func (queue *gcloudQueue) taskHandler(w http.ResponseWriter, r *http.Request) error {
	bearer := extractBearer(r.Header.Get("Authorization"))
	if bearer == "" {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token format %q", r.Header.Get("Authorization")), http.StatusUnauthorized)
		return nil
	}
	jwt, err := idtoken.Validate(r.Context(), bearer, queue.audience)
	if err != nil {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: %s", bearer, err), http.StatusUnauthorized)
		return nil
	}
	email, _ := jwt.Claims["email"].(string)
	if email != queue.serviceAccountEmail {
		return errors.Errorf("cloudtasks: unexpected email %q in token %q", email, bearer)
	}

	key := r.Header.Get("Altipla-Task")
	if key == "" {
		key = r.Header.Get("X-Altipla-Task")
	}
	if key == "" {
		return errors.Errorf("cloudtasks: missing task key")
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		return errors.Trace(err)
	}
	retries, err := strconv.ParseInt(r.Header.Get("X-CloudTasks-TaskRetryCount"), 10, 64)
	if err != nil {
		return errors.Trace(err)
	}
	task := &Task{
		key:     key,
		name:    r.Header.Get("X-CloudTasks-TaskName"),
		payload: payload,
		Retries: retries,
	}

	metrics.GetOrCreateCounter(fmt.Sprintf("cloudtasks_received_total{queue=%q,task=%q}", queue.name, task.key)).Inc()

	start := time.Now()
	if err := safeCall(r.Context(), key, task); err != nil {
		slog.Error("cloudtasks: task failed",
			slog.String("task", task.key),
			slog.String("queue", queue.name),
			slog.String("error", err.Error()),
			slog.String("details", errors.Details(err)),
			slog.Int64("retries", task.Retries),
		)
		metrics.GetOrCreateCounter(fmt.Sprintf("cloudtasks_failed_total{queue=%q,task=%q}", queue.name, task.key)).Inc()
		return errors.Trace(err)
	}

	slog.Debug("cloudtasks: task completed",
		slog.String("task", task.name),
		slog.String("queue", queue.name),
		slog.String("function", key),
		slog.Int64("retries", task.Retries),
	)
	metrics.GetOrCreateCounter(fmt.Sprintf("cloudtasks_success_total{queue=%q,task=%q}", queue.name, task.key)).Inc()
	metrics.GetOrCreateHistogram(fmt.Sprintf("cloudtasks_duration_seconds{queue=%q,task=%q}", queue.name, task.key)).UpdateDuration(start)

	return nil
}

func extractBearer(authorization string) string {
	parts := strings.SplitN(authorization, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
}

func safeCall(ctx context.Context, key string, task *Task) (err error) {
	defer func() {
		if r := errors.Recover(recover()); r != nil {
			err = r
		}
	}()
	return funcs[key].fn(ctx, task)
}

type localQueue struct {
	name string
}

func (queue *localQueue) Send(ctx context.Context, task *Task) error {
	go func() {
		if err := funcs[task.key].fn(context.Background(), task); err != nil {
			slog.Error("cloudtasks: failed to execute simulated task",
				slog.String("err", err.Error()),
				slog.String("task", task.key),
			)
			fmt.Println(errors.Stack(err))
			return
		}

		slog.Info("cloudtasks: task simulation completed",
			slog.String("task", task.name),
			slog.String("queue", queue.name),
			slog.String("function", task.key),
		)
	}()

	return nil
}

func (queue *localQueue) SendExternal(ctx context.Context, task *ExternalTask) error {
	slog.DebugContext(ctx, "cloudtasks: simulated external task", "task", task)
	return nil
}
