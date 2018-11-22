package main

import (
	"database/sql"
	"fmt"
	"os"
	"strconv"
	"testing"

	"github.com/dgraph-io/badger"
	"github.com/lib/pq"
	bolt "go.etcd.io/bbolt"
)

func TestMain(m *testing.M) {
	os.Exit(m.Run())
}

func BenchmarkBoltWrite(b *testing.B) {
	db, err := bolt.Open("my.db", 0600, nil)
	if err != nil {
		b.Fatal(err)
	}
	if db == nil {
		fmt.Println("NIL")
	}

	defer db.Close()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Start a writable transaction.
		tx, err := db.Begin(true)
		if err != nil {
			b.Fatal(err)
		}

		// Use the transaction...
		bu, err := tx.CreateBucketIfNotExists([]byte("MyBucket"))
		if err != nil {
			tx.Rollback()
			b.Fatal(err)
		}
		for i := 0; i < 100; i++ {
			key := []byte(strconv.Itoa(n) + "answer" + strconv.Itoa(i))
			val := []byte(strconv.Itoa(i + 42))
			if err := bu.Put(key, val); err != nil {
				tx.Rollback()
				b.Fatal(err)
			}
		}

		// Commit the transaction and check for error.
		if err := tx.Commit(); err != nil {
			b.Fatal(err)
		}

	}
}

// badger vomits with logs by default. supressing it
type nillog struct{}

func (n nillog) Errorf(s string, args ...interface{}) {
	return
}
func (n nillog) Infof(s string, args ...interface{}) {
	return
}
func (n nillog) Warningf(s string, args ...interface{}) {
	return
}

func BenchmarkBadgerWrite(b *testing.B) {
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	badger.SetLogger(nillog{})
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		// Start a writable transaction.
		txn := db.NewTransaction(true)

		for i := 0; i < 100; i++ {
			key := []byte(strconv.Itoa(n) + "answer" + strconv.Itoa(i))
			val := []byte(strconv.Itoa(i + 42))
			err := txn.Set(key, val)
			if err != nil {
				txn.Discard()
				b.Fatal(err)
			}
		}

		// Commit the transaction and check for error.
		if err := txn.Commit(); err != nil {
			b.Fatal(err)
		}
		txn.Discard()
	}
}

func BenchmarkBadgerBatchWrite(b *testing.B) {
	opts := badger.DefaultOptions
	opts.Dir = "/tmp/badger"
	opts.ValueDir = "/tmp/badger"
	badger.SetLogger(nillog{})
	db, err := badger.Open(opts)
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()

	for n := 0; n < b.N; n++ {
		wb := db.NewWriteBatch()

		for i := 0; i < 100; i++ {
			key := []byte(strconv.Itoa(n) + "answer" + strconv.Itoa(i))
			val := []byte(strconv.Itoa(i + 42))
			err := wb.Set(key, val, 0)
			if err != nil {
				wb.Cancel()
				b.Fatal(err)
			}
		}

		// Commit the transaction and check for error.
		if err := wb.Flush(); err != nil {
			wb.Cancel()
			b.Fatal(err)
		}
	}
}

func BenchmarkPgWrite(b *testing.B) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	_, err = db.Exec("create table if not exists test1(key varchar(20) primary key, val varchar(20))")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		// Start a writable transaction.
		txn, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}

		for i := 0; i < 100; i++ {
			key := strconv.Itoa(n) + "answer" + strconv.Itoa(i)
			val := strconv.Itoa(i + 42)
			_, err := txn.Exec("insert into test1 values($1,$2) on conflict (key) do update set val=excluded.val", key, val)
			if err != nil {
				txn.Rollback()
				b.Fatal(err)
			}
		}

		// Commit the transaction and check for error.
		if err := txn.Commit(); err != nil {
			txn.Rollback()
			b.Fatal(err)
		}

	}
}

func BenchmarkPgWriteUnlogged(b *testing.B) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	_, err = db.Exec("create unlogged table if not exists test2(key varchar(20) primary key, val varchar(20))")
	if err != nil {
		b.Fatal(err)
	}
	for n := 0; n < b.N; n++ {
		// Start a writable transaction.
		txn, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}

		for i := 0; i < 100; i++ {
			key := strconv.Itoa(n) + "answer" + strconv.Itoa(i)
			val := strconv.Itoa(i + 42)
			_, err := txn.Exec("insert into test2 values($1,$2) on conflict (key) do update set val=excluded.val", key, val)
			if err != nil {
				txn.Rollback()
				b.Fatal(err)
			}
		}

		// Commit the transaction and check for error.
		if err := txn.Commit(); err != nil {
			txn.Rollback()
			b.Fatal(err)
		}

	}
}

func BenchmarkPgWriteBatch(b *testing.B) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	_, err = db.Exec("create table if not exists test3(key varchar(20) primary key, val varchar(20))")
	if err != nil {
		b.Fatal(err)
	}

	for n := 0; n < b.N; n++ {
		// Start a writable transaction.
		txn, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		k := []string{}
		v := []string{}
		for i := 0; i < 100; i++ {
			key := strconv.Itoa(n) + "answer" + strconv.Itoa(i)
			val := strconv.Itoa(i + 42)
			k = append(k, key)
			v = append(v, val)
		}
		_, err = txn.Exec(`insert into test3 
		select key, val from 
			unnest($1::text[], $2::text[]) v(key, val)
		on conflict (key) do update set val=excluded.val`,
			pq.Array(k), pq.Array(v))
		if err != nil {
			txn.Rollback()
			b.Fatal(err)
		}

		// Commit the transaction and check for error.
		if err := txn.Commit(); err != nil {
			txn.Rollback()
			b.Fatal(err)
		}

	}
}

func BenchmarkPgWriteBatchUnlogged(b *testing.B) {
	db, err := sql.Open("postgres", "postgres://postgres:postgres@localhost:5432/postgres?sslmode=disable")
	if err != nil {
		b.Fatal(err)
	}
	defer db.Close()
	b.ResetTimer()
	_, err = db.Exec("create unlogged table if not exists test4(key varchar(20) primary key, val varchar(20))")
	if err != nil {
		b.Fatal(err)
	}

	for n := 0; n < b.N; n++ {
		// Start a writable transaction.
		txn, err := db.Begin()
		if err != nil {
			b.Fatal(err)
		}
		k := []string{}
		v := []string{}
		for i := 0; i < 100; i++ {
			key := "answer" + strconv.Itoa(i)
			val := strconv.Itoa(i + 42)
			k = append(k, key)
			v = append(v, val)
		}
		_, err = txn.Exec(`insert into test4 
		select key, val from 
			unnest($1::text[], $2::text[]) v(key, val)
		on conflict (key) do update set val=excluded.val`,
			pq.Array(k), pq.Array(v))
		if err != nil {
			txn.Rollback()
			b.Fatal(err)
		}

		// Commit the transaction and check for error.
		if err := txn.Commit(); err != nil {
			txn.Rollback()
			b.Fatal(err)
		}

	}
}
