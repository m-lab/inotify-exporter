// watch provides an interface for recursively watching file events under a
// YYYY/MM/DD data directory hierarchy.
package watch

import (
	"fmt"
	"log"
	"regexp"
	"strings"
	"time"

	"golang.org/x/sys/unix"

	// TODO: add vendor support for github.com/rjeczalik/notify
	"github.com/rjeczalik/notify"
)

var (
	// Patterns for matching date strings in directory names.
	yearPattern  = `20[0-9]{2}`
	monthPattern = `0[1-9]|1[0-2]`
	dayPattern   = `0[1-9]|[12][0-9]|3[0-1]`
	datePattern  = regexp.MustCompile(yearPattern + "/" + monthPattern + "/" + dayPattern)
)

// DirRecursively starts a recursive inotify watch on InCreate, InDelete, and
// InCloseWrite events on baseDir and subdirectories. The watch will remain
// active until the caller sends a value to the stop channel.
func DirRecursively(baseDir string, stop <-chan struct{},
	onEvent func(t time.Time, ev notify.EventInfo, shortPath string)) error {
	// Notify will drop events if the receiver does not keep up. So, make the
	// channel buffered to ensure no event is dropped.
	ch := make(chan notify.EventInfo, 128)

	// Start a recursive watch on baseDir.
	err := notify.Watch(fmt.Sprintf("%s/...", baseDir), ch,
		notify.InCreate, notify.InDelete, notify.InCloseWrite)
	if err != nil {
		log.Println(err)
		return err
	}
	defer notify.Stop(ch)

	var ev notify.EventInfo
	for {
		// Receive an event from the watch channel.
		select {
		case ev = <-ch:
		case <-stop:
			// The caller has signaled us to stop.

			// NOTE: the recursive behavior of notify.Watch consumes the delete
			// directory event on the watched directory. For non-recursive
			// behavior the delete event is sent to the channel and serves as
			// the stop condition. This seems like a bug in the recursive
			// implementation.
			return nil
		}

		// Only count files.
		if isDir(ev) {
			continue
		}

		shortPath := getPathSuffix(baseDir, ev.Path())
		// Only accept valid YYYY/MM/DD paths.
		if isValidPath(shortPath) {
			t := time.Now().UTC()
			log.Printf("%s %s %s\n", t, ev.Event(), ev.Path())
			// For all file events with a valid path, call the user-provided
			// onEvent function.
			onEvent(t, ev, shortPath)
		}
	}
	return nil
}

// isDir checks whether the event applied to a directory.
func isDir(ev notify.EventInfo) bool {
	unixEv, ok := ev.Sys().(*unix.InotifyEvent)
	if !ok {
		return false
	}
	return unixEv.Mask&unix.IN_ISDIR != 0
}

// isValidPath checks whether the path prefix matches the YYYY/MM/DD directory pattern.
func isValidPath(datePath string) bool {
	if len(datePath) < 10 {
		return false
	}
	return datePattern.MatchString(datePath[:10])
}

// getPathSuffix strips the prefixDir from the eventPath and returns only the
// path suffix. If the prefixDir is not present, getPathSuffix returns the
// empty string.
func getPathSuffix(prefixDir, eventPath string) string {
	// Only consider paths that are under prefix. This is a sanity check.
	if !strings.HasPrefix(eventPath, prefixDir) {
		return ""
	}
	return strings.TrimPrefix(eventPath, prefixDir+"/")
}
