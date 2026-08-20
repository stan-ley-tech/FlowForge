// Package store persists FlowForge's state to SQLite. Every operation
// that needs more than one statement to stay consistent (admitting a run
// under a concurrency cap, completing a step and advancing the next one,
// reclaiming an expired lease) runs inside a single transaction via
// Store.Atomic, so a crash mid-operation never leaves half-applied state.
package store

import (
	"context"
	"database/sql"
	"fmt"

	_ "modernc.org/sqlite"
)

// dbtx is satisfied by both *sql.DB and *sql.Tx, which lets queries be
// written once and used either standalone (for reads) or inside a
// transaction (for anything that mutates state).
type dbtx interface {
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	QueryRowContext(ctx context.Context, query string, args ...any) *sql.Row
}

type Store struct {
	db *sql.DB
}

func Open(path string) (*Store, error) {
	db, err := sql.Open("sqlite", path+"?_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)")
	if err != nil {
		return nil, fmt.Errorf("open sqlite: %w", err)
	}
	// A single connection turns every sequence of statements issued from
	// the engine layer into something serialized against every other
	// sequence, which is what lets multi-step operations like admission
	// control stay correct without hand-rolled locking.
	db.SetMaxOpenConns(1)

	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("apply schema: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error {
	return s.db.Close()
}

// Queries exposes every read/write operation the engine needs. It's
// implemented once against the dbtx interface so the same methods work
// whether called directly against the database or against an open
// transaction.
type Queries struct {
	db dbtx
}

// Read returns a Queries bound to the database directly, for operations
// that don't need transactional atomicity with anything else.
func (s *Store) Read() *Queries {
	return &Queries{db: s.db}
}

// Atomic runs fn inside a transaction, committing if fn returns nil and
// rolling back otherwise.
func (s *Store) Atomic(ctx context.Context, fn func(q *Queries) error) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return fmt.Errorf("begin tx: %w", err)
	}
	if err := fn(&Queries{db: tx}); err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit()
}
