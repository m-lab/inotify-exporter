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
	"time"

	"github.com/rjeczalik/notify"
)

var (
	rootPath = flag.String("path", "", "Path of root directory to watch.")
)

type iNotifyLogger struct {
	*os.File
}

func NewLogger(t time.Time) (*iNotifyLogger, error) {
	// For example: 20170828T14:27:27.480836000Z.inotify.log
	fname := fmt.Sprintf("%04d/%02d/%02d/%04d%02d%02dT%02d:%02d:%02d.%09dZ.inotify.log",
		t.Year(), (int)(t.Month()), t.Day(),
		t.Year(), (int)(t.Month()), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond())

	file, err := os.Create(fname)
	if err != nil {
		return nil, err
	}
	return &iNotifyLogger{file}, nil
}

func (l *iNotifyLogger) Event(date time.Time, event notify.EventInfo, count int) {
	// msg := fmt.Sprintf(
	// 	"%s %s %s %d\n", date.Format(time.RFC3339Nano), event.Event(), event.Path(), count)
	// fmt.Fprintf(l, msg)
	// fmt.Printf(msg)
	fmt.Fprintf(l,
		"%s %s %s %d\n", date.Format(time.RFC3339Nano), event.Event(), event.Path(), count)
}

func watchEventsForever(l chan notify.EventInfo) error {
	// <datetime> <event> <file-count> <filename> [other metadata]\n
	fileCount := 0

	startTime := time.Now().UTC()
	// Calculate a time when watching should stop.
	endTime := time.Date(
		startTime.Year(), startTime.Month(), startTime.Day()+1,
		1, 0, 0, 0, time.UTC)
	// endtime should always be larger than t.
	waitTime := endTime.Sub(startTime)
	// TODO: log the wait time.
	alarm := time.After(waitTime)

	// TODO: we must guarantee that the day directory matches the start time.
	logger, err := NewLogger(startTime)
	if err != nil {
		return err
	}

	// TODO: vim on save, causes an InCreate event but no corresponding
	// InDelete. This results in the count increasing without the number of
	// files actually increasing. How is this possible?

	for {
		select {
		case ev := <-l:
			t := time.Now().UTC()
			switch ev.Event() {
			case notify.InCreate:
				fmt.Printf("watchDay saw: %s\n", ev.Path())
				fileCount += 1
			case notify.InDelete:
				fileCount -= 1
			default:
				// No change.
			}
			logger.Event(t, ev, fileCount)
		case <-alarm:
			// We believe that there should be no more file events that we want to log.
			// So, stop this.
			logger.Close()
			// TODO: close log file.
			return nil
		}
	}
}

// TODO: notify supports "recursive" watchpoints. Need to test this to check for
// resource leaks.
// if err := notify.Watch("./...", c, notify.Remove); err != nil {
//     log.Fatal(err)
// }

func watchRoot(rootDir string, start time.Time) error {
	l := make(chan notify.EventInfo, 16)
	// Watch for new month directories created in the root dir.
	err := notify.Watch(rootDir, l, notify.InCreate)
	if err != nil {
		return err
	}
	defer notify.Stop(l)

	for {
		select {
		case ev := <-l:
			// now := time.Now().UTC()
			switch ev.Event() {
			case notify.InCreate:
				fmt.Printf("watchRoot saw: %s\n", ev.Path())
				go watchYear(ev.Path())
				fmt.Printf("watching year\n")
				// if err != nil {
				// 	// TODO: how to continue watching?
				// 	return err
				// }
			default:
				// No change.
			}
		}
	}
}

func watchYear(yearDir string) error {
	l := make(chan notify.EventInfo, 16)
	// Watch for new month directories created in the year dir.
	err := notify.Watch(yearDir, l, notify.InCreate)
	if err != nil {
		return err
	}
	defer notify.Stop(l)

	for {
		select {
		case ev := <-l:
			// now := time.Now().UTC()
			switch ev.Event() {
			case notify.InCreate:
				fmt.Printf("watchYear saw: %s\n", ev.Path())
				go watchMonth(ev.Path())
				fmt.Printf("watching month\n")
				// if err != nil {
				// 	// TODO: how to continue watching?
				// 	return err
				// }
			default:
				// No change.
			}
		}
	}
}

func watchMonth(monthDir string) error {
	l := make(chan notify.EventInfo, 128)
	// Watch for new day directories created in the month dir.
	err := notify.Watch(monthDir, l, notify.InCreate)
	if err != nil {
		return err
	}
	defer notify.Stop(l)

	for {
		select {
		case ev := <-l:
			// now := time.Now().UTC()
			switch ev.Event() {
			case notify.InCreate:
				fmt.Printf("watchMonth saw: %s\n", ev.Path())
				go watchDay(ev.Path())
				fmt.Printf("watching day\n")
				//if err != nil {
				//	// TODO: how to continue watching???
				//	return err
				//}
			default:
				// No change.
			}
		}
	}
}

func watchDay(dayDir string) error {
	// Make the channel buffered to ensure no event is dropped. Notify will
	// drop events if the receiver does not keep up.
	l := make(chan notify.EventInfo, 128)

	// TODO: what is the difference the notify library creates between Create & InCreate, etc?
	// err := notify.Watch(dir, listener, notify.Create, notify.Remove, notify.Rename, notify.InCloseWrite)
	err := notify.Watch(dayDir, l, notify.InCreate, notify.InDelete, notify.Rename, notify.InCloseWrite)
	if err != nil {
		return err
	}

	// TODO: do we need to handle the Stop case?
	defer notify.Stop(l)
	watchEventsForever(l)
	return nil
}

func main() {
	flag.Parse()
	// Watch root dir.
	// Immediately add watches to current date for:
	// * YYYY
	// * YYYY/MM
	// * YYYY/MM/DD
	// When we add a new month or add a new day, we can stop monitoring the
	// previous one shortly after.
	// So, at all times, there should be at least:
	// * one year watcher.
	// * one month watcher.
	// * one day watcher.
	//
	// Each watcher will have a time associated with it. When an event occurs,
	// it can check the date. If the date is in the future, we can stop
	// watching.
	//
	// We must not count deletes coming from delete logs safely.

	fmt.Printf("Watching: %s\n", *rootPath)
	for {
		start := time.Now().UTC()
		err := watchRoot(*rootPath, start)
		if err != nil {
			log.Fatal(err)
		}
	}

	// watchYear()
	// watchMonth()
	// err := watchDay(*rootPath)
	// if err != nil {
	// 	log.Fatal(err)
	// }
}
