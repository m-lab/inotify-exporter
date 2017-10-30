package watch_test

import (
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/m-lab/inotify-exporter/watch"
	"github.com/rjeczalik/notify"
)

func TestDirRecursively(t *testing.T) {
	done := make(chan bool)
	events := make(chan string, 20)
	stop := make(chan bool)

	// Try to watch a directory that does not exist, should return immediately.
	err := watch.DirRecursively("DirDoesNotExist", stop,
		func(et time.Time, ev notify.EventInfo, shortPath string) {
			// No events should be processed.
			t.Fatalf("%s %s %s\n", et, ev, shortPath)
		},
	)
	if err == nil {
		t.Fatal(err)
	}

	// Create a temp, base directory for testing real file events.
	tmpDir, err := ioutil.TempDir("", "TestDirRecursively-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tmpDir)
	if err != nil {
		t.Fatal(err)
	}

	// Create an event handling function that sends all events to the events channel.
	onEvent := func(et time.Time, ev notify.EventInfo, shortPath string) {
		// Send the short path to the events channel.
		t.Logf("Handling event for %s\n", shortPath)
		events <- shortPath
	}

	// Watch a directory that does exist will block. So, run this in a go
	// routine where we can signal that the function is 'done' after it
	// returns.
	go func() {
		err := watch.DirRecursively(tmpDir, stop, onEvent)
		if err != nil {
			t.Fatal(err)
		}
		// Signal that we returned successfully.
		done <- true
	}()

	// Create a YYYY/MM/DD directory path.
	dateDir := tmpDir + "/2017/10/11"
	err = os.MkdirAll(dateDir, 0755)
	if err != nil {
		t.Fatal(err)
	}

	// Wait until the watch is ready. Without a delay, the notify.Watch is still
	// initializing and the following events are missed.
	time.Sleep(500 * time.Millisecond)

	// Create a file in the tmpDir/YYYY/MM/DD directory.
	fName := dateDir + "/" + "foo"
	f, err := os.Create(fName)
	if err != nil {
		t.Fatal(err)
	}
	defer f.Close()

	// Verify that we receive an event from onEvent above.
	select {
	case <-events:
		break
	case <-time.After(time.Second * 2):
		t.Fatal("timeout waiting for event.")
	}

	// Send a stop signal to watch.DirRecursively so that it returns.
	stop <- true

	// Verify that watch.DirRecursively returns.
	select {
	case <-done:
		t.Log("Returned successfully")
	case <-time.After(time.Second * 2):
		t.Fatal("timeout waiting for event.")
	}
}
