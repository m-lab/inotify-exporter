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
// an M-Lab experiment data directory. These events are logged for offline
// auditing and counts are exported for monitoring directly by Prometheus.
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

var (
	rootPath = flag.String("path", "", "Path of root directory to watch.")
)

func addSubDirs(rootDir string, w *fsnotify.Watcher) {
	filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			fmt.Printf("Adding subdir %s %#v\n", path, info)
			w.Add(path)
		}
		return nil
	})
}

func watchEventsForever(w *fsnotify.Watcher) {
	fileCount := 0

	// NOTE: vim on save, causes an InCreate event but no corresponding
	// InDelete. This results in the count increasing without the number of
	// files actually increasing.

	for {

		select {
		case event := <-w.Events:
			date := time.Now().Format(time.RFC3339Nano)
			switch {
			case event.Op&fsnotify.Create == fsnotify.Create:
				fileCount += 1
			case event.Op&fsnotify.Remove == fsnotify.Remove:
				fileCount -= 1
			default:
				// No change.
			}
			fmt.Printf("%s %s %d %s\n", date, event.Op, fileCount, event.Name)
		case err := <-w.Errors:
			fmt.Println(err)
		}

	}
}

/*
// Set up a watchpoint listening for events within a directory tree rooted
// at current working directory. Dispatch remove events to c.
if err := notify.Watch("./...", c, notify.Remove); err != nil {
    log.Fatal(err)
}
defer notify.Stop(c)


*/

func watchRoot(dir string) error {
	// Make the channel buffered to ensure no event is dropped. Notify will
	// drop an event if the receiver is not able to keep up the sending
	// pace.
	w, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer w.Close()

	// err := notify.Watch(dir, listener, notify.Create, notify.Remove, notify.Rename, notify.InCloseWrite)
	err = w.Add(dir)
	if err != nil {
		return err
	}

	watchEventsForever(w)
	return nil
}

func main() {
	flag.Parse()
	fmt.Printf("Watching: %s\n", *rootPath)

	err := watchRoot(*rootPath)
	if err != nil {
		log.Fatal(err)
	}
}
