package storage

import (
	"testing"
	"time"
)

func TestSandboxStateCache_GetSet(t *testing.T) {
	cache := NewSandboxStateCache()

	// Test Get on empty cache
	if entry := cache.Get("session1"); entry != nil {
		t.Errorf("expected nil for non-existent session, got %v", entry)
	}

	// Test Set and Get
	now := time.Now()
	cache.Set("session1", now)

	entry := cache.Get("session1")
	if entry == nil {
		t.Fatal("expected entry, got nil")
	}
	if !entry.LastModified.Equal(now) {
		t.Errorf("expected LastModified %v, got %v", now, entry.LastModified)
	}
	if entry.RefreshedAt.IsZero() {
		t.Error("expected RefreshedAt to be set")
	}
}

func TestSandboxStateCache_Delete(t *testing.T) {
	cache := NewSandboxStateCache()

	cache.Set("session1", time.Now())
	cache.Delete("session1")

	if entry := cache.Get("session1"); entry != nil {
		t.Errorf("expected nil after delete, got %v", entry)
	}
}

func TestSandboxStateCache_All(t *testing.T) {
	cache := NewSandboxStateCache()

	now := time.Now()
	cache.Set("session1", now)
	cache.Set("session2", now.Add(-time.Hour))

	all := cache.All()
	if len(all) != 2 {
		t.Errorf("expected 2 entries, got %d", len(all))
	}

	if _, ok := all["session1"]; !ok {
		t.Error("expected session1 in results")
	}
	if _, ok := all["session2"]; !ok {
		t.Error("expected session2 in results")
	}
}

func TestSandboxStateCache_Sync(t *testing.T) {
	cache := NewSandboxStateCache()

	// Add initial entries
	cache.Set("existing1", time.Now())
	cache.Set("existing2", time.Now())

	// Sync with new discovery
	discovered := map[string]time.Time{
		"existing1": time.Now(),
		"new1":      time.Now(),
		"new2":      time.Time{}, // zero time is fine
	}

	added, removed := cache.Sync(discovered)

	// Check added
	if len(added) != 2 {
		t.Errorf("expected 2 added, got %d: %v", len(added), added)
	}

	// Check removed
	if len(removed) != 1 {
		t.Errorf("expected 1 removed, got %d: %v", len(removed), removed)
	}
	found := false
	for _, r := range removed {
		if r == "existing2" {
			found = true
		}
	}
	if !found {
		t.Error("expected existing2 to be removed")
	}

	// Verify cache state
	all := cache.All()
	if len(all) != 3 {
		t.Errorf("expected 3 entries after sync, got %d", len(all))
	}
}

func TestSandboxStateCache_NeedsRefresh(t *testing.T) {
	cache := NewSandboxStateCache()

	// Add entry that was never refreshed (RefreshedAt is zero)
	cache.entries["new_session"] = &SandboxStateCacheEntry{
		LastModified: time.Now(),
		RefreshedAt:  time.Time{}, // never refreshed
	}

	// Add entry that was refreshed recently
	cache.Set("recent_session", time.Now())

	// Add entry that was refreshed long ago
	cache.entries["old_session"] = &SandboxStateCacheEntry{
		LastModified: time.Now().Add(-2 * time.Hour),
		RefreshedAt:  time.Now().Add(-2 * time.Hour), // refreshed 2 hours ago
	}

	// Check with 1 hour max age
	needsRefresh := cache.NeedsRefresh(1 * time.Hour)

	// Should include new_session (never refreshed) and old_session (refreshed > 1 hour ago)
	if len(needsRefresh) != 2 {
		t.Errorf("expected 2 sessions needing refresh, got %d: %v", len(needsRefresh), needsRefresh)
	}

	hasNew, hasOld := false, false
	for _, s := range needsRefresh {
		if s == "new_session" {
			hasNew = true
		}
		if s == "old_session" {
			hasOld = true
		}
	}
	if !hasNew {
		t.Error("expected new_session to need refresh")
	}
	if !hasOld {
		t.Error("expected old_session to need refresh")
	}
}

func TestSandboxStateCache_ExpiredSessions(t *testing.T) {
	cache := NewSandboxStateCache()

	// Add non-expired session
	cache.Set("recent", time.Now())

	// Add expired session (modified 2 hours ago)
	cache.entries["expired"] = &SandboxStateCacheEntry{
		LastModified: time.Now().Add(-2 * time.Hour),
		RefreshedAt:  time.Now(), // must be refreshed to be considered
	}

	// Add session that's expired but never refreshed (should be skipped)
	cache.entries["expired_but_not_refreshed"] = &SandboxStateCacheEntry{
		LastModified: time.Now().Add(-2 * time.Hour),
		RefreshedAt:  time.Time{}, // never refreshed
	}

	// Check with 1 hour TTL
	expired := cache.ExpiredSessions(1 * time.Hour)

	if len(expired) != 1 {
		t.Errorf("expected 1 expired session, got %d: %v", len(expired), expired)
	}
	if len(expired) > 0 && expired[0] != "expired" {
		t.Errorf("expected 'expired' session, got %v", expired[0])
	}
}

func TestSandboxStateCache_Concurrency(t *testing.T) {
	cache := NewSandboxStateCache()
	done := make(chan bool)

	// Writer goroutine
	go func() {
		for i := 0; i < 100; i++ {
			cache.Set("session", time.Now())
		}
		done <- true
	}()

	// Reader goroutine
	go func() {
		for i := 0; i < 100; i++ {
			_ = cache.Get("session")
			_ = cache.All()
			_ = cache.NeedsRefresh(time.Hour)
			_ = cache.ExpiredSessions(time.Hour)
		}
		done <- true
	}()

	// Wait for both
	<-done
	<-done
}
