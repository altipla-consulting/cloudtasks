package cloudtasks

import "net/http"

func Handler() (string, http.Handler) {
	mux := http.NewServeMux()
	for _, queue := range queues {
		mux.HandleFunc("/_cloudtasks/"+queue.name, queue.taskHandler)
	}
	return "/_cloudtasks/", mux
}
