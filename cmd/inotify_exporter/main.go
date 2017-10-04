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
	"sync"
	"time"

	"github.com/rjeczalik/notify"
	"golang.org/x/sys/unix"
)

var (
	rootPath   = flag.String("path", "", "Path of root directory to watch.")
	dirPattern = regexp.MustCompile("20[0-9]{2}/[01][0-9]/[0123][0-9]")

	yearPattern   = regexp.MustCompile("20[0-9]{2}")
	monthPattern  = regexp.MustCompile("0[1-9]|1[0-2]")
	dayPattern    = regexp.MustCompile("0[1-9]|[12][0-9]|3[0-1]")
	activeWatches = &watches{make(map[string]chan notify.EventInfo), sync.Mutex{}}
)

// iNotifyLogger tracks metadata for logging file events.
type iNotifyLogger struct {
	// File is a file descriptor to the log file.
	*os.File

	// Count is the current file counter.
	Count int

	// Prefix is a redundant directory prefix that can be stripped from logs.
	Prefix string
}

type watches struct {
	channel map[string]chan notify.EventInfo
	mux     sync.Mutex
}

func (aw *watches) AddIfMissing(path string, wc chan notify.EventInfo) bool {
	aw.mux.Lock()
	defer aw.mux.Unlock()
	if _, ok := aw.channel[path]; !ok {
		aw.channel[path] = wc
		return true
	}
	return false
}

func (aw *watches) Remove(path string) {
	aw.mux.Lock()
	defer aw.mux.Unlock()
	delete(aw.channel, path)
}

func (aw *watches) DebugPrint() {
	fmt.Println("printing current watches")
	aw.mux.Lock()
	defer aw.mux.Unlock()
	for k, v := range aw.channel {
		fmt.Println("watch:", k, v)
	}
	fmt.Println("done printing")
}

// fixedRFC3339Nano guarantees a fixed format RFC3339 time format. The Go
// time.Format function does not provide this guarantee because it trims
// trailing zeros.
func fixedRFC3339Nano(t time.Time) string {
	return fmt.Sprintf("%04d-%02d-%02dT%02d:%02d:%02d.%09dZ",
		t.Year(), (int)(t.Month()), t.Day(),
		t.Hour(), t.Minute(), t.Second(), t.Nanosecond())
}

func NewLogger(prefixDir string, evTime time.Time) (*iNotifyLogger, error) {

	// Extract the YYYY/MM/DD suffix on dayDir.
	yearMonthDay := prefixDir[len(prefixDir)-10:]
	fmt.Printf("Creating logger for %s\n", yearMonthDay)
	// TODO: make this rotate hourly, somehow.
	t := dirDate(yearMonthDay)

	// For example: 20170828T14:27:27.480836000Z.inotify.log
	fname := fmt.Sprintf("%s/%04d%02d%02dT%02d:%02d:%02d.%09dZ.inotify.log",
		prefixDir, t.Year(), (int)(t.Month()), t.Day(), 0, 0, 0, 0)
	// fname := fmt.Sprintf("%s/%04d/%02d/%02d/%04d%02d%02dT%02d:%02d:%02d.%09dZ.inotify.log",
	// 	ls.dirPrefix,
	// 	t.Year(), (int)(t.Month()), t.Day(),
	// 	t.Year(), (int)(t.Month()), t.Day(), t.Hour(), 0, 0, 0)

	fmt.Printf("Creating: %s\n", fname)
	// Create if missing, append if present.
	file, err := os.OpenFile(fname, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return nil, err
	}
	l := &iNotifyLogger{file, 0, prefixDir[:len(prefixDir)-10]}
	return l, nil
}

func (l *iNotifyLogger) Event(t time.Time, ev notify.EventInfo) {
	path := ev.Path()
	msg := fmt.Sprintf(
		"%s %s %s %d\n", fixedRFC3339Nano(t), ev.Event(), path[len(l.Prefix):], l.Count)
	fmt.Fprintf(l, msg)
	fmt.Printf(msg)
}

func watchDir(dir string, startTime, until time.Time,
	afterWatch func(),
	onCreate func(t time.Time, ev notify.EventInfo) error) {

	// Notify drops events if the receiver does not keep up.
	c := make(chan notify.EventInfo, 128)
	err := notify.Watch(dir, c, notify.InCreate, notify.InDelete, notify.InCloseWrite)
	if err != nil {
		fmt.Printf("DIR: Failed to add watch: %s\n", err)
		return
	}
	defer func() {
		fmt.Println("Shutting down channel for", dir)
		notify.Stop(c)
	}()
	if !activeWatches.AddIfMissing(dir, c) {
		// Return triggers the defer operaitons above.
		return
	}
	defer func() {
		// At this point we added a watch above, and should remove it before
		// leaving this function.
		activeWatches.Remove(dir)
	}()
	afterWatch()

	// TODO: check that until is larger than starttime.
	// The until time should always be larger than now.
	waitTime := until.Sub(startTime)
	fmt.Printf("Watching %s for: %s\n", dir, waitTime)
	alarm := time.After(waitTime)

	for {
		select {
		case ev := <-c:
			if !isDir(ev) {
				fmt.Printf("File events on '%s' are ignored! %s\n", dir, ev)
				continue
			}
			fmt.Printf("DIR EVENT for %s:\n\t%s\n", dir, ev)
			if ev.Event() == notify.InDelete && dir == ev.Path() {
				// The watched directory is being removed, so we're done here.
				return
			}
			if ev.Event() != notify.InCreate {
				// We only process onCreate events.
				continue
			}

			t := time.Now().UTC()
			onCreate(t, ev)
		case <-alarm:
			// TODO: do we need an alarm any more?
			return
		}
	}
	return
}

// watchDay expects that dir includes the sub-path YYYY/MM/DD and creates a logger
// that will be passed to all events. After 'endTime', the watch will close the logger
// and return.
func watchDay(dayDir string, startTime, until time.Time,
	onEvent func(t time.Time, ev notify.EventInfo, logger *iNotifyLogger) error) error {

	// Notify will drop events if the receiver does not keep up. So, make the
	// channel buffered to ensure no event is dropped.
	c := make(chan notify.EventInfo, 128)

	// Start a recursive watch on dayDir.
	err := notify.Watch(fmt.Sprintf("%s/...", dayDir), c, notify.InCreate, notify.InDelete, notify.InCloseWrite)
	if err != nil {
		fmt.Printf("DAY: Failed to add watch: %s\n", err)
		return err
	}
	defer func() {
		fmt.Println("Shutting down channel for", dayDir)
		notify.Stop(c)
	}()
	if !activeWatches.AddIfMissing(dayDir, c) {
		// Return triggers the defer operaitons above.
		return nil
	}
	defer func() {
		// At this point we added a watch above, and should remove it before
		// leaving this function.
		activeWatches.Remove(dayDir)
	}()
	// afterWatch()

	// TODO: check that until is larger than starttime.
	// The until time should always be larger than now.
	waitTime := until.Sub(startTime)

	fmt.Printf("Watching %s for: %s\n", dayDir, waitTime)
	alarm := time.After(waitTime)

	logger, err := NewLogger(dayDir, startTime)
	if err != nil {
		log.Printf("Failed to create log file '%s': %s", dayDir, err)
		return err
	}
	defer logger.Close()

	for {
		select {
		case ev := <-c:
			// Process all events, since this is a recursive watch.
			fmt.Printf("DAY EVENT for %s:\n\t%s\n", dayDir, ev)
			if ev.Event() == notify.InDelete && dayDir == ev.Path() {
				// The watched directory is being removed, so we're done here.
				return nil
			}
			t := time.Now().UTC()
			onEvent(t, ev, logger)
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
	nextForever
)

var (
	untilForever time.Time = time.Date(10000, 1, 1, 0, 0, 0, 0, time.UTC)
)

// waitUntil returns a time.Time of the next calendar day, month or year, plus one hour.
func untilTime(startTime time.Time, d int) time.Time {
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

func isValidYear(year string) bool {
	if len(year) != 4 {
		return false
	}
	return yearPattern.MatchString(year)
}

func isValidMonth(month string) bool {
	if len(month) != 2 {
		return false
	}
	return monthPattern.MatchString(month)
}

func isValidDay(day string) bool {
	if len(day) != 2 {
		return false
	}
	return dayPattern.MatchString(day)
}

func formatYear(t time.Time) string {
	return fmt.Sprintf("%04d", t.Year())
}

func formatMonth(t time.Time) string {
	return fmt.Sprintf("%02d", (int)(t.Month()))
}

func formatDay(t time.Time) string {
	return fmt.Sprintf("%02d", t.Day())
}

// isValidPath checks whether the path prefix matches the YYYY/MM/DD directory pattern.
func isValidPath(datePath string) bool {
	if len(datePath) < 10 {
		return false
	}
	return isValidYear(datePath[:4]) && isValidMonth(datePath[5:7]) && isValidDay(datePath[8:10])
}

func getPathSuffix(prefixDir, eventPath string) string {
	// Only consider paths that are under prefix. This is a sanity check.
	if !strings.HasPrefix(eventPath, prefixDir) {
		return ""
	}
	return strings.TrimPrefix(eventPath, prefixDir+"/")
}

func watchRoot(start time.Time, rootDir string) error {
	// Watch the rootDir for new year directory events, effectively forever.
	watchDir(rootDir, start, untilForever,
		func() {
			// Try to add a watch for the current year.
			go watchYear(start, rootDir+"/"+formatYear(start))
		},
		func(t time.Time, ev notify.EventInfo) error {
			// Only accept paths that follow the YYYY directory pattern.
			shortPath := getPathSuffix(rootDir, ev.Path())
			if !isValidYear(shortPath) {
				fmt.Printf("Invalid year path: '%s'\n", shortPath)
				fmt.Println(ev)
				return nil
			}

			go watchYear(t, ev.Path())
			return nil
		},
	)
	return nil
}

func watchYear(start time.Time, yearDir string) error {
	// Watch the yearDir for new month directory events, until next year.
	watchDir(yearDir, start, untilTime(start, nextYear),
		func() {
			// Immediately try to add a watch for the current month.
			go watchMonth(start, yearDir+"/"+formatMonth(start))
		},
		func(t time.Time, ev notify.EventInfo) error {
			// Only accept paths that follow the MM directory pattern.
			shortPath := getPathSuffix(yearDir, ev.Path())
			if !isValidMonth(shortPath) {
				fmt.Printf("Invalid month path: '%s'\n", shortPath)
				fmt.Println(ev)
				return nil
			}

			go watchMonth(t, ev.Path())
			return nil
		},
	)
	return nil
}

func watchMonth(start time.Time, monthDir string) error {
	// Watch the monthDir for new day directory events, until next month.
	watchDir(monthDir, start, untilTime(start, nextMonth),
		func() {
			// Immediately try to add a watch for the current day.
			go watchCurrentDay(start, monthDir+"/"+formatDay(start))
		},
		func(t time.Time, ev notify.EventInfo) error {
			// Only accept paths that follow the DD directory pattern.
			shortPath := getPathSuffix(monthDir, ev.Path())
			if !isValidDay(shortPath) {
				fmt.Printf("Invalid day path: '%s'\n", shortPath)
				fmt.Println(ev)
				return nil
			}

			go watchCurrentDay(t, ev.Path())
			return nil
		},
	)
	return nil
}

func watchCurrentDay(start time.Time, dayDir string) error {
	prefixDir := dayDir[:len(dayDir)-10]

	// watchDay watches recursively on the day directory, until the next day.
	watchDay(dayDir, start, untilTime(start, nextDay),
		// onEvent
		func(t time.Time, ev notify.EventInfo, logger *iNotifyLogger) error {
			// Only count files.
			if isDir(ev) {
				return nil
			}

			// Only accept valid YYYY/MM/DD paths.
			shortPath := strings.TrimPrefix(ev.Path(), prefixDir)
			if !isValidPath(shortPath) {
				fmt.Printf("invalid file path: %s\n", shortPath)
				return nil
			}

			// The event is for a file under a YYYY/MM/DD/* prefix.
			switch ev.Event() {
			case notify.InCreate:
				logger.Count += 1
			case notify.InDelete:
				logger.Count -= 1
			default:
				// No change.
			}

			logger.Event(t, ev)
			return nil
		},
	)
	return nil
}

func main() {
	flag.Parse()
	// Watch root dir.
	//
	// On first start, immediately add watches for current date, if present:
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
		err := watchRoot(start, *rootPath)
		if err != nil {
			log.Fatal(err)
		}
	}
}
