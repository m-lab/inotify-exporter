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
	"time"

	"github.com/rjeczalik/notify"
	"golang.org/x/sys/unix"
)

var (
	path = flag.String("path", "", "Path of root directory to watch.")
)

func watchEventsForever(ch chan notify.EventInfo) {
	// <datetime> <event> <file-count> <filename> [other metadata]\n
	fileCount := 0

	// NOTE: vim on save, causes an InCreate event but no corresponding
	// InDelete. This results in the count increasing without the number of
	// files actually increasing.

	for {
		ev := <-ch
		date := time.Now().Format(time.RFC3339Nano)
		switch ev.Event() {
		case notify.InCreate:
			fileCount += 1
		case notify.InDelete:
			fileCount -= 1
		default:
			// No change.
		}
		s := ev.Sys().(*unix.InotifyEvent)
		fmt.Printf("%s %s %d %s %#v\n", date, ev.Event(), fileCount, ev.Path(), s)
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

func watchDir(dir string) error {
	// Make the channel buffered to ensure no event is dropped. Notify will
	// drop an event if the receiver is not able to keep up the sending
	// pace.
	listener := make(chan notify.EventInfo, 20)

	// err := notify.Watch(dir, listener, notify.Create, notify.Remove, notify.Rename, notify.InCloseWrite)
	err := notify.Watch(dir, listener, notify.InCreate, notify.InDelete, notify.Rename, notify.InCloseWrite)
	if err != nil {
		return err
	}

	watchEventsForever(listener)

	return nil
}

func main() {
	flag.Parse()
	fmt.Printf("Watching: %s\n", *path)

	err := watchDir(*path)
	if err != nil {
		log.Fatal(err)
	}
}
