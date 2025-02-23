package cloudtasks

import (
	"net/http"

	"github.com/altipla-consulting/doris"
)

func Handler() (string, http.Handler) {
	mux := http.NewServeMux()
	for _, queue := range allQueues {
		mux.Handle("/_cloudtasks/"+queue.name, doris.Handler(queue.taskHandler))
	}
	return "/_cloudtasks/", mux
}
