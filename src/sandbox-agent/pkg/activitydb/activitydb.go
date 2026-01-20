// Package activitydb provides SQLite-based storage for sandbox activity timestamps.
// This replaces Kubernetes annotation-based tracking to avoid API rate limiting.
package activitydb

import (
	"context"
	"database/sql"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "modernc.org/sqlite"
)

// Config holds ActivityDB configuration.
type Config struct {
	// Path is the directory path for the SQLite database file.
	// Default: /var/lib/sandbox-agent
	Path string
	// Filename is the database filename.
	// Default: activity.db
	Filename string
}

// ActivityDB manages sandbox activity timestamps in SQLite.
type ActivityDB struct {
	db   *sql.DB
	mu   sync.RWMutex
	path string
}

// New creates a new ActivityDB instance.
func New(cfg Config) (*ActivityDB, error) {
	if cfg.Path == "" {
		cfg.Path = "/var/lib/sandbox-agent"
	}
	if cfg.Filename == "" {
		cfg.Filename = "activity.db"
	}

	// Ensure directory exists
	if err := os.MkdirAll(cfg.Path, 0755); err != nil {
		return nil, fmt.Errorf("create directory %s: %w", cfg.Path, err)
	}

	dbPath := filepath.Join(cfg.Path, cfg.Filename)

	// Open with WAL mode for better concurrent performance
	db, err := sql.Open("sqlite", dbPath+"?_pragma=journal_mode(WAL)&_pragma=synchronous(NORMAL)&_pragma=busy_timeout(5000)")
	if err != nil {
		return nil, fmt.Errorf("open database: %w", err)
	}

	// Create table if not exists
	if err := initSchema(db); err != nil {
		db.Close()
		return nil, fmt.Errorf("init schema: %w", err)
	}

	slog.Info("activity database initialized", "path", dbPath)
	return &ActivityDB{db: db, path: dbPath}, nil
}

func initSchema(db *sql.DB) error {
	_, err := db.Exec(`
		CREATE TABLE IF NOT EXISTS sandbox_activity (
			session_id TEXT PRIMARY KEY,
			last_used_at INTEGER NOT NULL
		);
		CREATE INDEX IF NOT EXISTS idx_last_used_at ON sandbox_activity(last_used_at);
	`)
	return err
}

// UpdateActivity updates or inserts the last-used timestamp for a session to now.
func (a *ActivityDB) UpdateActivity(ctx context.Context, sessionID string) error {
	return a.SetLastUsed(ctx, sessionID, time.Now().Unix())
}

// SetLastUsed sets a specific timestamp for a session.
func (a *ActivityDB) SetLastUsed(ctx context.Context, sessionID string, unixTimestamp int64) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.db.ExecContext(ctx, `
		INSERT INTO sandbox_activity (session_id, last_used_at)
		VALUES (?, ?)
		ON CONFLICT(session_id) DO UPDATE SET last_used_at = excluded.last_used_at
	`, sessionID, unixTimestamp)
	return err
}

// GetLastUsed returns the last-used timestamp for a session.
// Returns (0, false) if not found.
func (a *ActivityDB) GetLastUsed(ctx context.Context, sessionID string) (int64, bool) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	var ts int64
	err := a.db.QueryRowContext(ctx,
		"SELECT last_used_at FROM sandbox_activity WHERE session_id = ?",
		sessionID,
	).Scan(&ts)
	if err != nil {
		return 0, false
	}
	return ts, true
}

// GetAllLastUsed returns a map of session_id -> last_used_at for all sessions.
// Used for bulk queries when listing sandboxes.
func (a *ActivityDB) GetAllLastUsed(ctx context.Context) (map[string]int64, error) {
	a.mu.RLock()
	defer a.mu.RUnlock()

	rows, err := a.db.QueryContext(ctx, "SELECT session_id, last_used_at FROM sandbox_activity")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make(map[string]int64)
	for rows.Next() {
		var sessionID string
		var ts int64
		if err := rows.Scan(&sessionID, &ts); err != nil {
			return nil, err
		}
		result[sessionID] = ts
	}
	return result, rows.Err()
}

// Delete removes a session's activity record.
func (a *ActivityDB) Delete(ctx context.Context, sessionID string) error {
	a.mu.Lock()
	defer a.mu.Unlock()

	_, err := a.db.ExecContext(ctx, "DELETE FROM sandbox_activity WHERE session_id = ?", sessionID)
	return err
}

// Cleanup removes activity records for sessions older than the given duration.
// Returns the number of records deleted.
func (a *ActivityDB) Cleanup(ctx context.Context, olderThan time.Duration) (int64, error) {
	a.mu.Lock()
	defer a.mu.Unlock()

	cutoff := time.Now().Add(-olderThan).Unix()
	result, err := a.db.ExecContext(ctx, "DELETE FROM sandbox_activity WHERE last_used_at < ?", cutoff)
	if err != nil {
		return 0, err
	}
	return result.RowsAffected()
}

// Close closes the database connection.
func (a *ActivityDB) Close() error {
	return a.db.Close()
}
