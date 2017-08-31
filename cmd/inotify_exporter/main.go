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
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/rjeczalik/notify"
	"golang.org/x/sys/unix"
)

var (
	rootPath   = flag.String("path", "", "Path of root directory to watch.")
	dirPattern = regexp.MustCompile("20[0-9]{2}/[01][0-9]/[0123][0-9]")
)

type iNotifyLogger struct {
	*os.File
	Count int
}

type LogSet struct {
	Loggers   map[string]*iNotifyLogger
	dirPrefix string
}

// fixedRFC3339Nano guarantees a fixed format RFC3339 time format. The Go
// time.Format function does not provide this guarantee because it trims
// trailing zeros.
func fixedRFC3339Nano(t time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%09dZ",
		t.Year(), (int)(t.Month()), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}

// which comes first the date or the directory?
func currentDir(t time.Time) string {
	return fmt.Sprintf("%04d/%02d/%02d", t.Year(), (int)(t.Month()), t.Day())
}

func NewLogSet(prefix string) *LogSet {
	return &LogSet{make(map[string]*iNotifyLogger), prefix}
}

func (ls *LogSet) GetLogger(datePath string, ev notify.EventInfo) (*iNotifyLogger, error) {

	if ev.Event() == notify.InDelete {
		if strings.HasSuffix(ev.Path(), ".inotify.log") {
			// If this is a delete event of an inotify.log file, remove the
			// corresponding logger.
			fmt.Fprintf(os.Stdout, "Deleting: %s\n", ev.Path())
			delete(ls.Loggers, datePath)
			// Do not attempt to re-create a log for a log just deleted.
			return nil, fmt.Errorf("%s deleted.", ev.Path())
		}
	}

	if l, ok := ls.Loggers[datePath]; ok {
		return l, nil
	}

	// TODO: make this rotate hourly, somehow.
	t := dirDate(datePath)

	// For example: 20170828T14:27:27.480836000Z.inotify.log
	fname := fmt.Sprintf("%s/%04d/%02d/%02d/%04d%02d%02dT%02d:%02d:%02d.%09dZ.inotify.log",
		ls.dirPrefix,
		t.Year(), (int)(t.Month()), t.Day(),
		t.Year(), (int)(t.Month()), t.Day(), t.Hour(), 0, 0, 0)

	fmt.Fprintf(os.Stdout, "Creating: %s\n", fname)
	// Create is missing, append if present.
	file, err := os.OpenFile("/dev/null", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	l := &iNotifyLogger{file, 0}
	ls.Loggers[datePath] = l
	return l, nil
}

func (l *iNotifyLogger) Event(date time.Time, event notify.EventInfo) {
	p := event.Path()
	msg := fmt.Sprintf(
		"%s %s %s %d\n", fixedRFC3339Nano(date), event.Event(), p[len(*rootPath)+1:], l.Count)
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
	nextDay1 int = iota
	nextMonth
	nextYear
	nextRagnarok
)

// waitUntil returns a time.Time of the next calendar day, month or year, plus one hour.
func timeAfter(startTime time.Time, d int) time.Time {
	// Note: time.Date normalizes dates; e.g. October 32 converts to November 1.
	switch d {
	case nextDay1:
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

// nextDay returns a time.Time of the next calendar day.
func nextDay(t time.Time) time.Time {
	// Note: time.Date normalizes dates; e.g. October 32 converts to November 1.
	return time.Date(t.Year(), t.Month(), t.Day()+1, 0, 0, 0, 0, time.UTC)
}

// Converts a "YYYY/MM/DD" string into a time.Time.
func dirDate(datePath string) time.Time {
	// TODO: add some kind of check.
	y, _ := strconv.Atoi(datePath[:4])
	m, _ := strconv.Atoi(datePath[5:7])
	d, _ := strconv.Atoi(datePath[8:])
	return time.Date(y, (time.Month)(m), d, 0, 0, 0, 0, time.UTC)
}

// isDir checks whether the event applied to a directory.
func isDir(ev notify.EventInfo) bool {
	unixEv, ok := ev.Sys().(*unix.InotifyEvent)
	if !ok {
		return false
	}
	return unixEv.Mask&unix.IN_ISDIR != 0
}

// isValidPath checks whether the path matches the YYYY/MM/DD directory pattern
// after stripping the prefix from path.
func isValidPath(shortPath string) bool {

	if len(shortPath) < 10 {
		return false
	}

	// If the pattern doesn't match, then we don't care about this event.
	if !dirPattern.MatchString(shortPath) {
		return false
	}

	return true
}

// isInLoggingWindow checks whether the event time occurs within a window around the date.
func isInLoggingWindow(datePath string, evTime time.Time, d time.Duration) bool {

	// Check that the event time is within duration window around the path Time.
	pathTime := dirDate(datePath)
	// (pathTime - d) < evTime < (nextDay(pathTime) + d)
	if evTime.After(pathTime.Add(-d)) &&
		evTime.Before(nextDay(pathTime).Add(d)) {
		return true
	}

	return false
}

func watchCurrentDay(t time.Time, dir string) error {
	logs := NewLogSet(dir)
	// Setup a recursive watch on the day directory.
	watch(fmt.Sprintf("%s/...", dir), t, timeAfter(t, nextRagnarok),
		// onEvent
		func(t time.Time, ev notify.EventInfo) error {

			// TODO: use two levels; watch dirs and watch for a day on valid dirs.
			// This handles resource cleanup.

			// Only count files.
			if isDir(ev) {
				fmt.Printf("Dir: %s\n", ev)
				return nil
			}

			// Only consider paths that are under dir. This is a sanity check.
			if !strings.HasPrefix(ev.Path(), dir) {
				fmt.Printf("prefix failed: %s\n", ev)
				return nil
			}

			// Only accept paths that are valid and current within an hour.
			shortPath := strings.TrimPrefix(ev.Path(), dir+"/")
			if !isValidPath(shortPath) {
				fmt.Printf("invalid path: %s\n", shortPath)
				return nil
			}

			if !isInLoggingWindow(shortPath[:10], t, time.Hour) {
				// TODO: try logger close, to prevent resource leaks.
				fmt.Printf("bad window: %s %s\n", shortPath, t)
				return nil
			}

			// At this point, the event is:
			//  * for a file
			//  * under a valid path, e.g. yyyy/mm/dd/foobar.gz
			//  * within the current logging window.

			logger, err := logs.GetLogger(shortPath[:10], ev)
			if err != nil {
				// Probably failed to create the log file.
				log.Printf("Ignoring: %s %s", err, ev)
				return nil
			}

			switch ev.Event() {
			case notify.InCreate:
				logger.Count += 1
			case notify.InDelete:
				logger.Count -= 1
			default:
				// No change.
			}

			// TODO: if we observe delete events for the log file we should
			// remove that log from the LogSet.
			logger.Event(t, ev)
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
	//
	// So, at all times, there should be at least:
	// * one year watcher.
	// * one month watcher.
	// * one day watcher.
	//
	// Each watcher will have a date associated with it. When an event occurs,
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
