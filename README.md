
# cloudtasks

[![Go Reference](https://pkg.go.dev/badge/github.com/altipla-consulting/cloudtasks.svg)](https://pkg.go.dev/github.com/altipla-consulting/cloudtasks)

Google Cloud Tasks integration with Cloud Run apps.


## Install

```shell
go get github.com/altipla-consulting/cloudtasks
```


## Usage

### Declare queues

To set up the queue system with its corresponding models and APIs, you'll need to instantiate a [github.com/altipla-consulting/doris](doris).Server in your application's main function. Any other server that can register the handler will do too.

```go
func main() {
	s := doris.NewServer()

	if err := models.ConnectQueues(s); err != nil {
		log.Fatal(err)
	}

  ...

	s.Serve()
}
```

In the `models/queues.go` file:

```go
var (
  QueueFoo cloudtasks.Queue
  QueueBar cloudtasks.Queue
)

func ConnectQueues(s *doris.Server) error {
  // PROJECT_HASH should be replaced by your Cloud Run project hash.
  // For example if you have URLs like "https://foo-service-9omj3qcv6b-ew.a.run.app/" the hash will be "9omj3qcv6b".
  QueueFoo = cloudtasks.NewQueue(s, "PROJECT_HASH", "foo")
  QueueBar = cloudtasks.NewQueue(s, "PROJECT_HASH", "bar")

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
  if err := fooTask.Call(r.Context(), models.QueueFoo, arg); err != nil {
    return errors.Trace(err)
  }

  // ... other code
}
```

Tasks can accept any JSON-serializable data as arguments, excluding structs with private fields or methods. This flexibility allows for various data types, including basic types like numbers and strings, or more complex structures needed for task execution.


## Contributing

You can make pull requests or create issues in GitHub. Any code you send should be formatted using `make gofmt`.


## License

[MIT License](LICENSE)
