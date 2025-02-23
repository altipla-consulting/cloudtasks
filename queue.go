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
	"sync"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	pb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/compute/metadata"
	"github.com/altipla-consulting/errors"
	"google.golang.org/api/idtoken"
)

var (
	initOnce                    sync.Once
	initErr                     error
	client                      *cloudtasks.Client
	googleProject, googleRegion string
	googleNumericProject        string
	serviceAccountEmail         string

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

// NewQueue initializes a new queue.
func NewQueue(name string, opts ...QueueOption) Queue {
	if os.Getenv("K_SERVICE") == "" {
		return &localQueue{name: name}
	}

	queue := &gcloudQueue{name: name}
	allQueues = append(allQueues, queue)
	return queue
}

// WithRegion configures a custom region for the queue. By default it will use the region of the Cloud Run service.
func WithRegion(region string) QueueOption {
	return func(queue *gcloudQueue) {
		queue.region = region
	}
}

type gcloudQueue struct {
	name   string
	region string
}

func initGlobals(ctx context.Context) error {
	var err error
	client, err = cloudtasks.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot initialize remote client: %w", err)
	}

	googleProject, err = metadata.ProjectID()
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot get google project name: %w", err)
	}
	googleNumericProject, err = metadata.NumericProjectID()
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot get google numeric project: %w", err)
	}
	region, err := metadata.Get("instance/region")
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot get google project region: %w", err)
	}
	googleRegion = path.Base(region)

	serviceAccountEmail, err = metadata.Email("default")
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot get default service account email: %w", err)
	}

	return nil
}

func (queue *gcloudQueue) Send(ctx context.Context, task *Task) error {
	initOnce.Do(func() {
		initErr = initGlobals(ctx)
	})
	if initErr != nil {
		return initErr
	}

	region := queue.region
	if region == "" {
		region = googleRegion
	}
	req := &pb.CreateTaskRequest{
		Parent: strings.Join([]string{"projects", googleProject, "locations", region, "queues", queue.name}, "/"),
		Task: &pb.Task{
			MessageType: &pb.Task_HttpRequest{
				HttpRequest: &pb.HttpRequest{
					HttpMethod: pb.HttpMethod_POST,
					Url:        fmt.Sprintf("https://%s-%s.%s.run.app/_cloudtasks/%s", os.Getenv("K_SERVICE"), googleNumericProject, googleRegion, queue.name),
					Body:       task.payload,
					Headers: map[string]string{
						"Content-Type":   "application/json",
						"X-Altipla-Task": task.key,
					},
					AuthorizationHeader: &pb.HttpRequest_OidcToken{
						OidcToken: &pb.OidcToken{
							ServiceAccountEmail: serviceAccountEmail,
							Audience:            fmt.Sprintf("https://%s-%s.%s.run.app/", os.Getenv("K_SERVICE"), googleNumericProject, googleRegion),
						},
					},
				},
			},
		},
	}
	var lastErr error
	for i := 0; i < 3 && ctx.Err() == nil; i++ {
		if err := createTask(ctx, req); err != nil {
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

func (queue *gcloudQueue) SendExternal(ctx context.Context, task *ExternalTask) error {
	initOnce.Do(func() {
		initErr = initGlobals(ctx)
	})
	if initErr != nil {
		return initErr
	}

	u, err := url.Parse(task.URL)
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot parse external task URL: %w", err)
	}
	payload, err := json.Marshal(task.Payload)
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot marshal task payload %T: %w", payload, err)
	}

	region := queue.region
	if region == "" {
		region = googleRegion
	}
	req := &pb.CreateTaskRequest{
		Parent: strings.Join([]string{"projects", googleProject, "locations", region, "queues", queue.name}, "/"),
		Task: &pb.Task{
			MessageType: &pb.Task_HttpRequest{
				HttpRequest: &pb.HttpRequest{
					HttpMethod: pb.HttpMethod_POST,
					Url:        task.URL,
					Body:       payload,
					Headers: map[string]string{
						"Content-Type": "application/json",
					},
					AuthorizationHeader: &pb.HttpRequest_OidcToken{
						OidcToken: &pb.OidcToken{
							ServiceAccountEmail: serviceAccountEmail,
							Audience:            fmt.Sprintf("https://%s/", u.Hostname()),
						},
					},
				},
			},
		},
	}
	var lastErr error
	for i := 0; i < 3 && ctx.Err() == nil; i++ {
		if err := createTask(ctx, req); err != nil {
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

func createTask(ctx context.Context, req *pb.CreateTaskRequest) error {
	// Cloud Tasks enforces a timeout of less than 30 seconds server side. This will
	// ensure all our calls are less than the limit to avoid the InvalidArgument error.
	ctx, cancel := context.WithTimeout(ctx, 25*time.Second)
	defer cancel()
	_, err := client.CreateTask(ctx, req)
	return err
}

func (queue *gcloudQueue) taskHandler(w http.ResponseWriter, r *http.Request) error {
	initOnce.Do(func() {
		initErr = initGlobals(r.Context())
	})
	if initErr != nil {
		return errors.Errorf("cloudtasks: failed to initialize globals: %w", initErr)
	}

	bearer := extractBearer(r.Header.Get("Authorization"))
	if bearer == "" {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token format %q", r.Header.Get("Authorization")), http.StatusUnauthorized)
		return nil
	}
	region := queue.region
	if region == "" {
		region = googleRegion
	}
	audience := fmt.Sprintf("https://%s-%s.%s.run.app/", os.Getenv("K_SERVICE"), googleNumericProject, region)
	jwt, err := idtoken.Validate(r.Context(), bearer, audience)
	if err != nil {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: %s", bearer, err), http.StatusUnauthorized)
		return nil
	}
	email, _ := jwt.Claims["email"].(string)
	if email != serviceAccountEmail {
		return errors.Errorf("cloudtasks: unexpected email %q in token %q", email, bearer)
	}

	key := r.Header.Get("X-Altipla-Task")
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
	if err := safeCall(r.Context(), key, task); err != nil {
		slog.Error("cloudtasks: task failed",
			slog.String("task", task.key),
			slog.String("queue", queue.name),
			slog.String("error", err.Error()),
			slog.String("details", errors.Details(err)),
			slog.Int64("retries", task.Retries),
		)
		return errors.Trace(err)
	}

	slog.Info("cloudtasks: task completed",
		slog.String("task", task.name),
		slog.String("queue", queue.name),
		slog.String("function", key),
		slog.Int64("retries", task.Retries),
	)

	return nil
}

func extractBearer(authorization string) string {
	parts := strings.SplitN(authorization, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
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

func safeCall(ctx context.Context, key string, task *Task) (err error) {
	defer func() {
		if r := errors.Recover(recover()); r != nil {
			err = r
		}
	}()
	return funcs[key].fn(ctx, task)
}
