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

/*func addSubDirs(rootDir string, l notify.EventInfo) {
	filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if info.IsDir() {
			fmt.Printf("Adding subdir %s %#v\n", path, info)
			watcher.WatchFlags(path, FSN_MOST)
		}
		return nil
	})
}*/

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
	p := event.Path()
	msg := fmt.Sprintf(
		"%s %s %s %d\n", fixedRFC3339Nano(date), event.Event(), p[len(*rootPath)+1:], count)
	fmt.Fprintf(l, msg)
	fmt.Printf(msg)
}

// TODO: notify supports "recursive" watchpoints. Need to test this to check for
// resource leaks.
// if err := notify.Watch("./...", c, notify.Remove); err != nil {
//     log.Fatal(err)
// }

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

func watchRoot(t time.Time, rootDir string) error {
	watch(rootDir, t, waitUntil(t, nextRagnarok),
		func(t time.Time, ev notify.EventInfo) error {
			switch ev.Event() {
			case notify.InCreate:
				// Only watch new directories that match the current year.
				curYear := fmt.Sprintf("%04d", t.Year())
				newYear := filepath.Base(ev.Path())
				if curYear == newYear {
					fmt.Println("Watching new year created!", newYear)
					go watchCurrentYear(t, ev.Path())
				}
			}
			fmt.Println("root event!", ev)
			return nil
		},
	)
	return nil
}

func watchCurrentYear(t time.Time, dir string) error {
	watch(dir, t, waitUntil(t, nextYear),
		// onEvent
		func(t time.Time, ev notify.EventInfo) error {
			switch ev.Event() {
			case notify.InCreate:
				// Only watch new directories that match the current month.
				curMonth := fmt.Sprintf("%02d", (int)(t.Month()))
				newMonth := filepath.Base(ev.Path())
				if curMonth == newMonth {
					fmt.Println("Watching new month created!", newMonth)
					go watchCurrentMonth(t, ev.Path())
				}
			}
			// fmt.Println("Year event!", ev)
			return nil
		},
	)
	return nil
}

func watchCurrentMonth(t time.Time, dir string) error {
	watch(dir, t, waitUntil(t, nextMonth),
		// onEvent
		func(t time.Time, ev notify.EventInfo) error {
			switch ev.Event() {
			case notify.InCreate:
				// Only watch new directories that match the current month.
				curDay := fmt.Sprintf("%02d", t.Day())
				newDay := filepath.Base(ev.Path())
				if curDay == newDay {
					fmt.Println("Watching new day created!", newDay)
					go watchCurrentDay(t, ev.Path())
				}
			}
			// fmt.Println("Month event!", ev)
			return nil
		},
	)
	return nil
}

func isDir(ev notify.EventInfo) bool {
	unixEv, ok := ev.Sys().(*unix.InotifyEvent)
	if !ok {
		return false
	}
	return unixEv.Mask&unix.IN_ISDIR != 0
}

func watchCurrentDay(t time.Time, dir string) error {

	if !strings.HasSuffix(dir, currentDir(t)) {
		fmt.Printf("%s does not have expected suffix %s\n", dir, currentDir(t))
		return fmt.Errorf("%s does not have expected suffix %s\n", dir, currentDir(t))
	}

	logger, err := NewLogger(t)
	if err != nil {
		return err
	}

	fileCount := 0
	// Setup a recursive watch on the day directory.
	watch(fmt.Sprintf("%s/...", dir), t, waitUntil(t, nextDay),
		// onEvent
		func(t time.Time, ev notify.EventInfo) error {
			// TODO:can we distinguish between files and dirs on delete
			// events?
			// Only count files.
			if !isDir(ev) {
				switch ev.Event() {
				case notify.InCreate:
					fileCount += 1
				case notify.InDelete:
					fileCount -= 1
				default:
					// No change.
				}
			}
			// fmt.Println("Day event!", ev)
			logger.Event(t, ev, fileCount)
			return nil
		},
	)
	logger.Close()
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
		err := watchRoot(start, *rootPath)
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
