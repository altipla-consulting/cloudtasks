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
	"github.com/altipla-consulting/telemetry"
	"google.golang.org/api/idtoken"
)

var (
	initOnce                    sync.Once
	initErr                     error
	client                      *cloudtasks.Client
	googleProject, googleRegion string
	serviceAccountEmail         string
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

// NewQueue initializes a new queue. It needs:
// - The Cloud Run project hash. For example if you have URLs like "https://foo-service-9omj3qcv6b-ew.a.run.app/" the hash will be "9omj3qcv6b".
// - The queue name.
func NewQueue(runProjectHash string, name string, opts ...QueueOption) Queue {
	if runProjectHash == "" {
		panic("cloudtasks: runProjectHash cannot be empty")
	}

	if os.Getenv("K_SERVICE") == "" {
		return new(localQueue)
	}

	queue := &gcloudQueue{
		name:           name,
		runProjectHash: runProjectHash,
	}
	http.Handle("/_cloudtasks/"+name, http.HandlerFunc(queue.taskHandler))
	return queue
}

// WithRegion configures a custom region for the queue. By default it will use the region of the Cloud Run service.
func WithRegion(region string) QueueOption {
	return func(queue *gcloudQueue) {
		queue.region = region
	}
}

type gcloudQueue struct {
	name           string
	region         string
	runProjectHash string
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

	googleRegion, err = metadata.Get("region")
	if err != nil {
		return fmt.Errorf("cloudtasks: cannot get google project region: %w", err)
	}
	googleRegion = path.Base(googleRegion)

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
					Url:        fmt.Sprintf("https://%s-%s-ew.a.run.app/_cloudtasks/%s", os.Getenv("K_SERVICE"), queue.runProjectHash, queue.name),
					Body:       task.payload,
					Headers: map[string]string{
						"Content-Type":   "application/json",
						"X-Altipla-Task": task.key,
					},
					AuthorizationHeader: &pb.HttpRequest_OidcToken{
						OidcToken: &pb.OidcToken{
							ServiceAccountEmail: serviceAccountEmail,
							Audience:            fmt.Sprintf("https://%s-%s-ew.a.run.app/", os.Getenv("K_SERVICE"), queue.runProjectHash),
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

func (queue *gcloudQueue) taskHandler(w http.ResponseWriter, r *http.Request) {
	initOnce.Do(func() {
		initErr = initGlobals(r.Context())
	})
	if initErr != nil {
		slog.Error("cloudtasks: failed to initialize globals", "err", errors.LogValue(initErr))
		telemetry.ReportError(r.Context(), initErr)
		return
	}

	bearer := extractBearer(r.Header.Get("Authorization"))
	if bearer == "" {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token format: %q", r.Header.Get("Authorization")), http.StatusUnauthorized)
		return
	}
	region := queue.region
	if region == "" {
		region = googleRegion
	}
	audience := fmt.Sprintf("https://%s-%s-%s.a.run.app/", os.Getenv("K_SERVICE"), queue.runProjectHash, regionCode(region))
	jwt, err := idtoken.Validate(r.Context(), bearer, audience)
	if err != nil {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: %s", bearer, err), http.StatusUnauthorized)
		return
	}
	email, _ := jwt.Claims["email"].(string)
	if email != serviceAccountEmail {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: invalid email %q", bearer, email), http.StatusUnauthorized)
		return
	}

	key := r.Header.Get("X-Altipla-Task")
	if key == "" {
		http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: missing task key", bearer), http.StatusUnauthorized)
		return
	}

	payload, err := io.ReadAll(r.Body)
	if err != nil {
		slog.Error("cloudtasks: cannot read task payload", "err", errors.LogValue(err))
		telemetry.ReportErrorRequest(r, err)
		return
	}
	retries, err := strconv.ParseInt(r.Header.Get("X-CloudTasks-TaskRetryCount"), 10, 64)
	if err != nil {
		slog.Error("cloudtasks: cannot parse task retry count", "err", errors.LogValue(err))
		telemetry.ReportErrorRequest(r, err)
		return
	}
	task := &Task{
		key:     key,
		name:    r.Header.Get("X-CloudTasks-TaskName"),
		payload: payload,
		Retries: retries,
	}
	if err := funcs[key].h(r.Context(), task); err != nil {
		slog.Error("cloudtasks: cannot execute task",
			"err", errors.LogValue(err),
			"task", task)
		telemetry.ReportErrorRequest(r, err)
		return
	}
}

func extractBearer(authorization string) string {
	parts := strings.SplitN(authorization, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
}

type localQueue struct{}

func (queue *localQueue) Send(ctx context.Context, task *Task) error {
	go func() {
		if err := funcs[task.key].h(context.Background(), task); err != nil {
			slog.Error("cloudtasks: cannot execute task",
				slog.String("err", err.Error()),
				slog.String("task", task.key))
		}
	}()
	return nil
}

func (queue *localQueue) SendExternal(ctx context.Context, task *ExternalTask) error {
	slog.DebugContext(ctx, "cloudtasks: simulated external task", "task", task)
	return nil
}
