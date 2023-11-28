package cloudtasks

import "errors"

// ErrCannotSendTask is returned from a Send call when the task has been correctly prepared but cannot be sent to the
// remote service. It can give you an opportunity to call the underlying implementation directly instead.
var ErrCannotSendTask = errors.New("cloudtasks: cannot send task")
