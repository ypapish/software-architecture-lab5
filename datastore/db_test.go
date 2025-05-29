package datastore

import (
	"fmt"
	"os"
	"testing"
)

func TestDb(t *testing.T) {
	tmp := t.TempDir()
	db, err := Open(tmp)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close()
	})

	pairs := [][]string{
		{"k1", "v1"},
		{"k2", "v2"},
		{"k3", "v3"},
		{"k2", "v2.1"},
	}

	t.Run("put/get", func(t *testing.T) {
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
			value, err := db.Get(pair[0])
			if err != nil {
				t.Errorf("Cannot get %s: %s", pairs[0], err)
			}
			if value != pair[1] {
				t.Errorf("Bad value returned expected %s, got %s", pair[1], value)
			}
		}
	})

	t.Run("file growth", func(t *testing.T) {
		sizeBefore, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		for _, pair := range pairs {
			err := db.Put(pair[0], pair[1])
			if err != nil {
				t.Errorf("Cannot put %s: %s", pairs[0], err)
			}
		}
		sizeAfter, err := db.Size()
		if err != nil {
			t.Fatal(err)
		}
		if sizeAfter <= sizeBefore {
			t.Errorf("Size does not grow after put (before %d, after %d)", sizeBefore, sizeAfter)
		}
	})

	t.Run("new db process", func(t *testing.T) {
		if err := db.Close(); err != nil {
			t.Fatal(err)
		}
		db, err = Open(tmp)
		if err != nil {
			t.Fatal(err)
		}

		uniquePairs := make(map[string]string)
		for _, pair := range pairs {
			uniquePairs[pair[0]] = pair[1]
		}

		for key, expectedValue := range uniquePairs {
			value, err := db.Get(key)
			if err != nil {
				t.Errorf("Cannot get %s: %s", key, err)
			}
			if value != expectedValue {
				t.Errorf("Get(%q) = %q, wanted %q", key, value, expectedValue)
			}
		}
	})
}

func TestSegmentCreation(t *testing.T) {
	tmp := t.TempDir()

	// Small segment size for testing (100 bytes)
	db, err := OpenWithMaxSize(tmp, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Write enough data to exceed the limit
	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("key%d", i)
		value := fmt.Sprintf("value%d", i)
		err := db.Put(key, value)
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify that multiple segments were created
	if len(db.segments) <= 1 {
		t.Errorf("Expected multiple segments, got %d", len(db.segments))
	}
}

func TestMergeSegments(t *testing.T) {
	tmp := t.TempDir()

	// Very small segment size for testing
	db, err := OpenWithMaxSize(tmp, 50)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create multiple segments
	for i := 0; i < 10; i++ {
		err := db.Put("key", "value")
		if err != nil {
			t.Fatal(err)
		}
	}

	// Force merge
	db.mergeSegments()

	// Verify only one segment remains after merge
	if len(db.segments) != 1 {
		t.Errorf("Expected 1 segment after merge, got %d", len(db.segments))
	}
}

func TestRecoveryAfterMergeFailure(t *testing.T) {
	tmp := t.TempDir()

	db, err := OpenWithMaxSize(tmp, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	err = db.Put("key1", "value1")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put("key2", "value2")
	if err != nil {
		t.Fatal(err)
	}

	segPath := db.segments[0].filePath
	tmpPath := segPath + ".tmp"
	err = os.Rename(segPath, tmpPath)
	if err != nil {
		t.Fatal(err)
	}

	db.mergeSegments()

	err = os.Rename(tmpPath, segPath)
	if err != nil {
		t.Fatal(err)
	}

	value, err := db.Get("key1")
	if err != nil || value != "value1" {
		t.Errorf("Data lost after failed merge")
	}

	value, err = db.Get("key2")
	if err != nil || value != "value2" {
		t.Errorf("Data lost after failed merge")
	}
}

func TestLatestValueAfterMerge(t *testing.T) {
	tmp := t.TempDir()

	db, err := OpenWithMaxSize(tmp, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Add multiple versions of same key
	err = db.Put("key", "value1")
	if err != nil {
		t.Fatal(err)
	}
	err = db.Put("key", "value2")
	if err != nil {
		t.Fatal(err)
	}

	// Trigger merge
	db.mergeSegments()

	// Verify we get the latest value
	value, err := db.Get("key")
	if err != nil {
		t.Fatal(err)
	}
	if value != "value2" {
		t.Errorf("Expected 'value2', got '%s'", value)
	}
}

func TestSegmentFileNaming(t *testing.T) {
	tmp := t.TempDir()

	db, err := OpenWithMaxSize(tmp, 100)
	if err != nil {
		t.Fatal(err)
	}
	defer db.Close()

	// Create multiple segments
	for i := 0; i < 3; i++ {
		err := db.Put("key", "value")
		if err != nil {
			t.Fatal(err)
		}
	}

	// Verify segment file names
	files, err := os.ReadDir(tmp)
	if err != nil {
		t.Fatal(err)
	}

	expectedNames := map[string]bool{
		"current-data": true,
		"segment-1":    true,
		"segment-2":    true,
	}

	for _, file := range files {
		if !expectedNames[file.Name()] {
			t.Errorf("Unexpected file name: %s", file.Name())
		}
	}
}
