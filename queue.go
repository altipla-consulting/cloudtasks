package cloudtasks

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"os"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	cloudtasks "cloud.google.com/go/cloudtasks/apiv2"
	pb "cloud.google.com/go/cloudtasks/apiv2/cloudtaskspb"
	"cloud.google.com/go/compute/metadata"
	"google.golang.org/api/idtoken"
)

var (
	initOnce                    sync.Once
	initErr                     error
	client                      *cloudtasks.Client
	googleProject, googleRegion string
	serviceAccountEmail         string
)

type Queue interface {
	Send(ctx context.Context, task *Task) error
}

type router interface {
	Post(string, func(http.ResponseWriter, *http.Request) error)
}

type QueueOption func(*gcloudQueue)

func NewQueue(r router, runProjectHash string, name string, opts ...QueueOption) Queue {
	if runProjectHash == "" {
		panic("cloudtasks: runProjectHash cannot be empty")
	}

	r.Post("/_cloudtasks/"+name, taskHandler(runProjectHash))

	if os.Getenv("K_SERVICE") == "" {
		return &localQueue{
			name: name,
		}
	}
	return &gcloudQueue{
		name:           name,
		runProjectHash: runProjectHash,
	}
}

func WithRegion(region string) QueueOption {
	return func(queue *gcloudQueue) {
		queue.region = region
	}
}

type gcloudQueue struct {
	name           string
	region         string
	runProjectHash string
	target         string
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
	audience := fmt.Sprintf("https://%s-%s-ew.a.run.app/", os.Getenv("K_SERVICE"), queue.runProjectHash)
	req := &pb.CreateTaskRequest{
		Parent: strings.Join([]string{"projects", googleProject, "locations", region, "queues", queue.name}, "/"),
		Task: &pb.Task{
			MessageType: &pb.Task_HttpRequest{
				HttpRequest: &pb.HttpRequest{
					HttpMethod: pb.HttpMethod_POST,
					Url:        queue.target,
					Body:       task.payload,
					Headers: map[string]string{
						"Content-Type":   "application/json",
						"X-Altipla-Task": task.key,
					},
					AuthorizationHeader: &pb.HttpRequest_OidcToken{
						OidcToken: &pb.OidcToken{
							ServiceAccountEmail: serviceAccountEmail,
							Audience:            audience,
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

type localQueue struct {
	name string
}

func (queue *localQueue) Send(ctx context.Context, task *Task) error {
	if err := funcs[task.key].h(ctx, task); err != nil {
		return fmt.Errorf("cloudtasks: cannot execute task %q: %w", task.key, err)
	}
	return nil
}

func taskHandler(runProjectHash string) func(w http.ResponseWriter, r *http.Request) error {
	return func(w http.ResponseWriter, r *http.Request) error {
		initOnce.Do(func() {
			initErr = initGlobals(r.Context())
		})
		if initErr != nil {
			return initErr
		}

		bearer := extractBearer(r.Header.Get("Authorization"))
		if bearer == "" {
			http.Error(w, fmt.Sprintf("cloudtasks: bad token format: %q", r.Header.Get("Authorization")), http.StatusUnauthorized)
			return nil
		}
		audience := fmt.Sprintf("https://%s-%s-ew.a.run.app/", os.Getenv("K_SERVICE"), runProjectHash)
		jwt, err := idtoken.Validate(r.Context(), bearer, audience)
		if err != nil {
			http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: %s", bearer, err), http.StatusUnauthorized)
			return nil
		}
		email, _ := jwt.Claims["email"].(string)
		if email != serviceAccountEmail {
			http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: invalid email %q", bearer, email), http.StatusUnauthorized)
			return nil
		}

		key := r.Header.Get("X-Altipla-Task")
		if key == "" {
			http.Error(w, fmt.Sprintf("cloudtasks: bad token %q: missing task key", bearer), http.StatusUnauthorized)
			return nil
		}

		payload, err := io.ReadAll(r.Body)
		if err != nil {
			return fmt.Errorf("cloudtasks: cannot read task payload: %w", err)
		}
		retries, err := strconv.ParseInt(r.Header.Get("X-CloudTasks-TaskRetryCount"), 10, 64)
		if err != nil {
			return fmt.Errorf("cloudtasks: cannot parse task retry count: %w", err)
		}
		task := &Task{
			key:     key,
			name:    r.Header.Get("X-CloudTasks-TaskName"),
			payload: payload,
			Retries: retries,
		}
		if err := funcs[key].h(r.Context(), task); err != nil {
			return fmt.Errorf("cloudtasks: cannot execute task %q: %w", key, err)
		}

		return nil
	}
}

func extractBearer(authorization string) string {
	parts := strings.SplitN(authorization, " ", 2)
	if len(parts) != 2 || parts[0] != "Bearer" {
		return ""
	}
	return parts[1]
}