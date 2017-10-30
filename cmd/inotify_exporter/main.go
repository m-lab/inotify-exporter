// Copyright 2017 inotify-exporter Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// inotify_exporter monitors file creation, deletion, and other events within
// an M-Lab experiment data directory. Aggregate event counts are exported for
// Prometheus monitoring. These events may be logged for offline auditing.
package main

import (
	"log"
	"net/http"
	"path"
	"strings"
	"time"

	"github.com/rjeczalik/notify"
	flag "github.com/spf13/pflag"

	"github.com/m-lab/inotify-exporter/watch"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

var (
	rootPaths = []string{}
)

// Prometheus Metrics.
var (
	createCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "inotify_file_create_total",
		Help: "A running count of all files created in watched directories.",
	})
	// createExtensions.WithLabelValues("s2c_snaplog").Inc()
	createExtensions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inotify_extension_create_total",
			Help: "A running count of file extensions created in watched directories.",
		},
		[]string{"ext"},
	)
	deleteCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "inotify_file_delete_total",
		Help: "A running count of all files deleted from watched directories.",
	})
	deleteExtensions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inotify_extension_delete_total",
			Help: "A running count of file extensions deleted from watched directories.",
		},
		[]string{"ext"},
	)
	closeWriteCount = prometheus.NewCounter(prometheus.CounterOpts{
		Name: "inotify_file_closewrite_total",
		Help: "A running count of all files with closewrite events in watched directories.",
	})
	closeWriteExtensions = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "inotify_extension_closewrite_total",
			Help: "A running count of file extensions with closewrite events in watched directories.",
		},
		[]string{"ext"},
	)
)

func init() {
	// Metrics have to be registered to be exposed:
	prometheus.MustRegister(createCount)
	prometheus.MustRegister(createExtensions)
	prometheus.MustRegister(deleteCount)
	prometheus.MustRegister(deleteExtensions)
	prometheus.MustRegister(closeWriteCount)
	prometheus.MustRegister(closeWriteExtensions)

	// Flags.
	flag.StringArrayVar(&rootPaths, "path", nil, "Path of root directory to watch.")
}

// onEvent processes a single inotify event. Only file events are considered.
// shortPath should be relative to the base directory of inotify watch. The
// prefix of shortPath should match the pattern: YYYY/MM/DD.
func onEvent(t time.Time, ev notify.EventInfo, shortPath string) {
	// The event is for a file under a YYYY/MM/DD/* prefix.
	switch ev.Event() {
	case notify.InCreate:
		createCount.Inc()
		createExtensions.WithLabelValues(getExtension(ev.Path())).Inc()
	case notify.InDelete:
		deleteCount.Inc()
		deleteExtensions.WithLabelValues(getExtension(ev.Path())).Inc()
	case notify.InCloseWrite:
		closeWriteCount.Inc()
		closeWriteExtensions.WithLabelValues(getExtension(ev.Path())).Inc()
	default:
		// No change.
	}
	return
}

// getExtension extracts the filename extension and if it ends with .gz,
// returns the last two extensions.
func getExtension(filename string) string {
	if strings.HasSuffix(filename, ".gz") {
		// Extract the preceeding extension if the file ends with .gz.
		return path.Ext(filename[:len(filename)-3]) + ".gz"
	} else {
		return path.Ext(filename)
	}
}

func main() {
	flag.Parse()

	// Directories are watched indefinitely, so this is never used.
	stop := make(chan bool)

	log.SetFlags(log.LstdFlags | log.Lmicroseconds | log.LUTC)
	for _, path := range rootPaths {
		log.Printf("Adding watch: %s\n", path)
		go watch.DirRecursively(path, stop, onEvent)
	}

	// The Handler function provides a default handler to expose metrics
	// via an HTTP server. "/metrics" is the usual endpoint for that.
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(":9393", nil))
}
