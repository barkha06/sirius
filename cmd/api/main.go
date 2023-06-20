package main

import (
	_ "expvar" // Register the expvar handlers
	"fmt"
	"github.com/couchbase/gocb/v2"
	"github.com/couchbaselabs/sirius/internal/generate"
	"github.com/couchbaselabs/sirius/internal/server_requests"
	"github.com/couchbaselabs/sirius/internal/tasks-manager"
	"github.com/pkg/profile"
	"log"
	"net/http"
	_ "net/http/pprof" // Register the pprof handlers
	"os"
)

const webPort = "4000"
const TaskQueueSize = 100

type Config struct {
	taskManager    *tasks_manager.TaskManager
	serverRequests *server_requests.ServerRequests
}

type DebugServer struct {
	*http.Server
}

// NewDebugServer provides new debug http server
func NewDebugServer(address string) *DebugServer {
	return &DebugServer{
		&http.Server{
			Addr:    address,
			Handler: http.DefaultServeMux,
		},
	}
}

func main() {
	registerInterfaces()
	defer profile.Start(profile.MemProfile).Stop()
	gocb.SetLogger(gocb.DefaultStdioLogger())

	app := Config{
		taskManager:    tasks_manager.NewTasKManager(TaskQueueSize),
		serverRequests: server_requests.NewServerRequests(),
	}
	go generate.Generate()

	//define the server
	log.Printf("Starting Document Loading Service at port %s\n", webPort)
	srv := http.Server{
		Addr:    fmt.Sprintf(":%s", webPort),
		Handler: app.routes(),
	}
	// start the server
	go func() {
		err := srv.ListenAndServe()
		if err != nil {
			app.taskManager.StopTaskManager()
			log.Println(err)
			os.Exit(-1)
		}
	}()

	debugServer := NewDebugServer(fmt.Sprintf("%s:%d", "0.0.0.0", 6060))
	log.Println("Starting Sirius profiling service at 6060")

	log.Fatal(debugServer.ListenAndServe())

}
