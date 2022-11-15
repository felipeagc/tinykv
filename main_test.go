package main

import (
	"bytes"
	"os"
	"testing"
)

const (
	DB_PATH = "/tmp/test.db"
)

func cleanDB() {
	os.Remove(DB_PATH)
}

func TestSimple(t *testing.T) {
	cleanDB()

	db, err := OpenDB(DB_PATH)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	checkFound := func(key, value []byte) {
		foundValue, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if foundValue == nil {
			t.Errorf("did not find value for key '%s'", string(key))
			return
		}
		if !bytes.Equal(foundValue, value) {
			t.Errorf("wrong value found, expected '%s'", string(value))
		}
	}

	checkMissing := func(key []byte) {
		foundValue, err := db.Get(key)
		if err != nil {
			t.Fatal(err)
		}
		if foundValue != nil {
			t.Errorf("found missing key '%s'", string(key))
		}
	}

	db.Set([]byte("hello1"), []byte("world1"))
	db.Set([]byte("hello3"), []byte("world3"))
	db.Set([]byte("hello2"), []byte("world2"))

	checkFound([]byte("hello1"), []byte("world1"))
	checkFound([]byte("hello2"), []byte("world2"))
	checkFound([]byte("hello3"), []byte("world3"))
	checkMissing([]byte("missing"))
}
