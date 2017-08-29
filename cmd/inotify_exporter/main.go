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
	"strings"
	"time"

	"github.com/rjeczalik/notify"
	"golang.org/x/sys/unix"
)

var (
	rootPath = flag.String("path", "", "Path of root directory to watch.")
)

type iNotifyLogger struct {
	*os.File
}

// which comes first the date or the directory?
func currentDir(t time.Time) string {
	return fmt.Sprintf("%04d/%02d/%02d", t.Year(), (int)(t.Month()), t.Day())
}

// fixedRFC3339Nano guarantees a fixed format RFC3339 time format. The Go
// time.Format function does not provide this guarantee because it trims
// trailing zeros.
func fixedRFC3339Nano(t time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%09dZ",
		t.Year(), (int)(t.Month()), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}

func NewLogger(t time.Time) (*iNotifyLogger, error) {
	// For example: 20170828T14:27:27.480836000Z.inotify.log
	d, err := os.Getwd()
	os.Chdir("2017/08/29")
	// fname := fmt.Sprintf("%s/%04d/%02d/%02d/%04d%02d%02dT%02d%02d%02d.%09dZ.inotify.log",
	fname := fmt.Sprintf("%04d%02d%02dT%02d%02d%02d.%09dZ.inotify.log",
		//d,
		//t.Year(), (int)(t.Month()), t.Day(),
		t.Year(), (int)(t.Month()), t.Day(), t.Hour(), 0, 0, 0)

	fmt.Fprintf(os.Stdout, "Creating: %s\n", fname)
	fmt.Fprintf(os.Stdout, "pwd: %s %s\n", d, err)
	file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	return &iNotifyLogger{file}, nil
}

func (l *iNotifyLogger) Event(date time.Time, event notify.EventInfo, count int) {
	p := event.Path()
	msg := fmt.Sprintf(
		"%s %s %s %d\n", fixedRFC3339Nano(date), event.Event(), p[len(*rootPath)+1:], count)
	fmt.Fprintf(l, msg)
	fmt.Printf(msg)
}

func watch(dir string, startTime, until time.Time,
	onEvent func(t time.Time, ev notify.EventInfo) error) error {

	// Make the channel buffered to ensure no event is dropped. Notify will
	// drop events if the receiver does not keep up.
	l := make(chan notify.EventInfo, 128)

	err := notify.Watch(dir, l, notify.InCreate, notify.InDelete, notify.InCloseWrite)
	if err != nil {
		return err
	}

	// TODO: do we need to handle the Stop case?
	defer notify.Stop(l)

	// TODO: check that until is larger than starttime.
	// The until time should always be larger than now.
	waitTime := until.Sub(startTime)
	// TODO: log the wait time.
	fmt.Printf("Watching until: %s\n", waitTime)
	alarm := time.After(waitTime)

	// TODO: we must guarantee that the day directory matches the start time.
	// logger, err := NewLogger(startTime)
	// if err != nil {
	// 	return err
	// }

	// TODO: vim on save, causes an InCreate event but no corresponding
	// InDelete. This results in the count increasing without the number of
	// files actually increasing. How is this possible?

	for {
		select {
		case ev := <-l:
			t := time.Now().UTC()
			onEvent(t, ev)
		case <-alarm:
			return nil
		}
	}
	return nil
}

const (
	nextDay int = iota
	nextMonth
	nextYear
	nextRagnarok
)

// waitUntil returns a time.Time of the next calendar day, month or year, plus one hour.
func waitUntil(startTime time.Time, d int) time.Time {
	// Note: time.Date normalizes dates; e.g. October 32 converts to November 1.
	switch d {
	case nextDay:
		return time.Date(startTime.Year(), startTime.Month(), startTime.Day()+1, 1, 0, 0, 0, time.UTC)
	case nextMonth:
		return time.Date(startTime.Year(), startTime.Month()+1, startTime.Day(), 1, 0, 0, 0, time.UTC)
	case nextYear:
		return time.Date(startTime.Year()+1, startTime.Month(), startTime.Day(), 1, 0, 0, 0, time.UTC)
	default:
		// Far into the future.
		return time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
	}
}

func isDir(ev notify.EventInfo) bool {
	unixEv, ok := ev.Sys().(*unix.InotifyEvent)
	if !ok {
		return false
	}
	return unixEv.Mask&unix.IN_ISDIR != 0
}

func watchCurrentDay(t time.Time, dir string) error {
	fileCount := 0
	// Setup a recursive watch on the day directory.
	watch(fmt.Sprintf("%s/...", dir), t, waitUntil(t, nextRagnarok),
		// onEvent
		func(t time.Time, ev notify.EventInfo) error {
			// TODO:can we distinguish between files and dirs on delete
			// events?
			// Only count files.
			if !isDir(ev) {
				p := ev.Path()
				shortPath := p[len(dir)+1:]

				fmt.Fprintln(os.Stdout, "shortPath:", shortPath)
				fmt.Fprintln(os.Stdout, "currentDir:", currentDir(t))
				if strings.HasPrefix(shortPath, currentDir(t)) {
					// Then this is a file event under a directory we care about.
					fmt.Fprintln(os.Stdout, "Look up logger for:", currentDir(t))
					logger, err := NewLogger(t)
					if err != nil {
						log.Fatal(err)
					}
					switch ev.Event() {
					case notify.InCreate:
						fileCount += 1
					case notify.InDelete:
						fileCount -= 1
					default:
						// No change.
					}
					// fmt.Println("Day event!", ev)
					logger.Event(t, ev, fileCount)
					// logger.Close()
				} else {
					fmt.Fprintln(os.Stdout, "Skipping event for: %s", ev)
				}
			}
			return nil
		},
	)
	// TODO: we never close the loggers...
	return nil
}

func main() {
	flag.Parse()
	// Watch root dir.
	//
	// If first start, immediately add watches for current date, if present:
	// * YYYY
	// * YYYY/MM
	// * YYYY/MM/DD
	//
	// Then wait.
	//
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
		// err := watchRoot(start, *rootPath)
		err := watchCurrentDay(start, *rootPath)
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
