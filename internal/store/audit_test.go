package store

import (
	"context"
	"database/sql"
	"strings"
	"testing"
	"time"
)

// testDB opens the same Postgres the gateway tests use and skips when it is
// not running. Audit behaviour is the one thing here that cannot be checked
// without a real database: the interesting properties are about rows
// actually reaching the table, and when.
func testDB(t *testing.T) *sql.DB {
	t.Helper()
	db, err := NewDB("postgres://gateway:gateway@localhost:5432/gateway?sslmode=disable")
	if err != nil {
		t.Skipf("postgres not available: %v", err)
	}
	// Same schema the production binary migrates to, so a new column shows
	// up here as a real failure rather than as a mystery later.
	if err := AutoMigrate(context.Background(), db); err != nil {
		t.Fatalf("migrate: %v", err)
	}
	return db
}

// countRows is a polling count: the auditor writes on its own goroutine, so
// "not there yet" and "never written" look identical in a single query.
func countRows(t *testing.T, db *sql.DB, path string) int {
	t.Helper()
	var n int
	if err := db.QueryRowContext(context.Background(),
		`SELECT count(*) FROM request_logs WHERE path=$1`, path).Scan(&n); err != nil {
		t.Fatalf("count: %v", err)
	}
	return n
}

// resetRows claims a path for one test. Clearing up front as well as on the
// way out matters more than it looks: when one of these tests fails it is
// usually because the worker is still writing after stop returned, and a
// cleanup that races it leaves rows behind — which then fail the *next* run
// with a count nobody can explain. Starting from zero keeps a failure from
// cascading.
func resetRows(t *testing.T, db *sql.DB, path string) {
	t.Helper()
	t.Cleanup(func() {
		db.ExecContext(context.Background(), `DELETE FROM request_logs WHERE path=$1`, path)
	})
	if _, err := db.ExecContext(context.Background(),
		`DELETE FROM request_logs WHERE path=$1`, path); err != nil {
		t.Fatalf("reset: %v", err)
	}
}

// A stop that returns before the worker finishes is the cheapest way to lose
// data in this system: it drops the tail of every buffered batch on every
// deploy, and nothing reports it, because the process exits successfully and
// the rows simply never existed.
func TestAuditorStopDrainsWhatIsQueued(t *testing.T) {
	db := testDB(t)
	const path = "/audit-drain"
	resetRows(t, db, path)

	// More than one batch, so the drain has to cover rows the batch-size
	// trigger and the timer never got to.
	const rows = 150

	ch, stop := StartAuditor(db, 1024)
	for i := 0; i < rows; i++ {
		ch <- LogEntry{Method: "POST", Path: path, StatusCode: 200}
	}

	// Deliberately no sleep before stopping. Waiting first would let the
	// timer flush the rows and pass even if stop returned immediately.
	stop()

	if got := countRows(t, db, path); got != rows {
		t.Errorf("wrote %d rows, want %d: stop returned before the drain finished", got, rows)
	}
}

// Batching without a timer means a partially filled batch waits for more
// traffic that may never come. On a quiet deployment that is the worst case:
// the one request you made to check whether logging works is the one you
// cannot see.
func TestAuditorFlushesAPartialBatchOnATimer(t *testing.T) {
	db := testDB(t)
	const path = "/audit-timer"
	resetRows(t, db, path)

	ch, stop := StartAuditor(db, 64)
	defer stop()

	// A single row, nowhere near auditBatchSize.
	ch <- LogEntry{Method: "POST", Path: path, StatusCode: 200}

	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		if countRows(t, db, path) > 0 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Errorf("one row sat unwritten for 3s; a partial batch needs a timer, not just a batch size")
}

// A batch fails as a unit, so one unacceptable row used to cost its
// neighbours too. This pins the fallback: the good rows still land.
func TestOneBadRowDoesNotTakeTheBatchWithIt(t *testing.T) {
	db := testDB(t)
	const path = "/audit-mixed"
	resetRows(t, db, path)

	ch, stop := StartAuditor(db, 64)

	// reject_reason is VARCHAR(32), and a new rejection gate is exactly how a
	// value outgrows it. One over-long reason is a real, ordinary failure —
	// it must not also delete the rows that happened to travel with it.
	overlong := strings.Repeat("x", 64)

	ch <- LogEntry{Method: "POST", Path: path, StatusCode: 200}
	ch <- LogEntry{Method: "POST", Path: path, StatusCode: 200, RejectReason: overlong}
	ch <- LogEntry{Method: "POST", Path: path, StatusCode: 200}
	stop()

	// Not "all three": the bad row is expected to be lost. The assertion is
	// that the failure costs the row that caused it and nothing else.
	if got := countRows(t, db, path); got != 2 {
		t.Errorf("wrote %d rows, want 2: a bad row cost its neighbours too", got)
	}
}
