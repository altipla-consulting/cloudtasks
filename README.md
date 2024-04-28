
# cloudtasks

[![Go Reference](https://pkg.go.dev/badge/github.com/altipla-consulting/cloudtasks.svg)](https://pkg.go.dev/github.com/altipla-consulting/cloudtasks)

Google Cloud Tasks integration with Cloud Run apps.


## Install

```shell
go get github.com/altipla-consulting/cloudtasks
```


## Usage

### Declare queues

To set up the queue system with its corresponding models and APIs, you'll need to call the code from your application's main function:

```go
func main() {
  if err := models.ConnectQueues(); err != nil {
    log.Fatal(err)
  }

  ...
}
```

In the `models/queues.go` file:

```go
var (
  QueueFoo cloudtasks.Queue
  QueueBar cloudtasks.Queue
)

func ConnectQueues() error {
  // PROJECT_HASH should be replaced by your Cloud Run project hash.
  // For example if you have URLs like "https://foo-service-9omj3qcv6b-ew.a.run.app/" the hash will be "9omj3qcv6b".
  QueueFoo = cloudtasks.NewQueue("PROJECT_HASH", "foo")
  QueueBar = cloudtasks.NewQueue("PROJECT_HASH", "bar")

  // It could also come from a global setting constant.
  QueueGlobal = cloudtasks.NewQueue(config.ProjectHash, "global")

  // Register the queue handlers.
  http.Handle(QueueFoo.Handler())
  http.Handle(QueueBar.Handler())
  http.Handle(QueueGlobal.Handler())

  return nil
}
```

Queues manage task consumption rates and the maximum number of concurrent tasks. These are pre-configured in Google Cloud.

A single queue can efficiently process multiple task types. It's recommended to use one queue for similar rate requirements instead of creating multiple queues, as this approach minimizes runtime overhead.


### Defining tasks

Define background tasks within the same package that will execute them. By convention, task functions are named with the suffix `xxFn`, indicating they're background functions. The first argument in `cloudtasks.Func` is a unique string for debugging purposes. Declaring two tasks with the same name will trigger a panic.

```go
var fooFn = cloudtasks.Func("foo", func(ctx context.Context, task *cloudtasks.Task) error {
  var arg int64
  if err := task.Read(&arg); err != nil {
    return errors.Trace(err)
  }

  // ... task content

  return nil
})
```

Ensure global task declarations occur during initialization, typically in the init() function.


### Task Invocation

Invoke tasks by calling the previously defined functions:

```go
func FooHandler(...) {
  // ... other code

  var arg int64
  if err := fooFn.Call(r.Context(), models.QueueFoo, arg); err != nil {
    return errors.Trace(err)
  }

  // ... other code
}
```

Tasks can accept any JSON-serializable data as arguments, excluding structs with private fields or methods. This flexibility allows for various data types, including basic types like numbers and strings, or more complex structures needed for task execution.


### External task invocation

Invoke tasks of external applications with our helper that has retry and authorization built-in:

```go
func FooHandler(w http.ResponseWriter, r *http.Request) error {
  // ... other code

  task := &cloudtasks.ExternalTask{
    URL: "https://foo-service-9omj3qcv6b-ew.a.run.app/myurl",
    Payload: arg,
  }
  if err := models.QueueFoo.SendExternal(r.Context(), task); err != nil {
    return errors.Trace(err)
  }

  // ... other code
}
```


## Contributing

You can make pull requests or create issues in GitHub. Any code you send should be formatted using `make gofmt`.


## License

[MIT License](LICENSE)
