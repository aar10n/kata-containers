package storage

import (
	"sync"
	"time"
)

// SandboxStateCacheEntry holds cached state metadata for a sandbox session.
type SandboxStateCacheEntry struct {
	LastModified time.Time // Most recent LastModified from S3 objects
	RefreshedAt  time.Time // When we last fetched LastModified from S3
}

// SandboxStateCache tracks sandbox state metadata to minimize S3 ListObjects calls.
// It caches LastModified times for each session's S3 prefix.
type SandboxStateCache struct {
	mu      sync.RWMutex
	entries map[string]*SandboxStateCacheEntry
}

// NewSandboxStateCache creates a new cache for sandbox state metadata.
func NewSandboxStateCache() *SandboxStateCache {
	return &SandboxStateCache{
		entries: make(map[string]*SandboxStateCacheEntry),
	}
}

// Get returns the cached entry for a session, or nil if not found.
func (c *SandboxStateCache) Get(sessionID string) *SandboxStateCacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	if entry, ok := c.entries[sessionID]; ok {
		// Return a copy to avoid race conditions
		return &SandboxStateCacheEntry{
			LastModified: entry.LastModified,
			RefreshedAt:  entry.RefreshedAt,
		}
	}
	return nil
}

// Set updates or creates a cache entry for a session.
func (c *SandboxStateCache) Set(sessionID string, lastModified time.Time) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.entries[sessionID] = &SandboxStateCacheEntry{
		LastModified: lastModified,
		RefreshedAt:  time.Now(),
	}
}

// Delete removes a session from the cache.
func (c *SandboxStateCache) Delete(sessionID string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.entries, sessionID)
}

// All returns a copy of all cached entries.
func (c *SandboxStateCache) All() map[string]SandboxStateCacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()
	result := make(map[string]SandboxStateCacheEntry, len(c.entries))
	for sessionID, entry := range c.entries {
		result[sessionID] = *entry
	}
	return result
}

// Sync updates the cache with discovered prefixes from S3.
// - Adds new sessions that were discovered but not in cache
// - Removes sessions that are in cache but no longer in S3
// Returns the list of session IDs that were added (new) and removed.
func (c *SandboxStateCache) Sync(discovered map[string]time.Time) (added, removed []string) {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Find sessions to remove (in cache but not discovered)
	for sessionID := range c.entries {
		if _, exists := discovered[sessionID]; !exists {
			removed = append(removed, sessionID)
		}
	}
	for _, sessionID := range removed {
		delete(c.entries, sessionID)
	}

	// Find sessions to add (discovered but not in cache)
	for sessionID, lastModified := range discovered {
		if _, exists := c.entries[sessionID]; !exists {
			added = append(added, sessionID)
			c.entries[sessionID] = &SandboxStateCacheEntry{
				LastModified: lastModified,
				RefreshedAt:  time.Time{}, // Not yet refreshed
			}
		}
	}

	return added, removed
}

// NeedsRefresh returns session IDs where the cached LastModified is older than maxAge.
// These sessions need their LastModified time refreshed from S3.
func (c *SandboxStateCache) NeedsRefresh(maxAge time.Duration) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	var needsRefresh []string
	for sessionID, entry := range c.entries {
		if entry.RefreshedAt.IsZero() || now.Sub(entry.RefreshedAt) > maxAge {
			needsRefresh = append(needsRefresh, sessionID)
		}
	}
	return needsRefresh
}

// ExpiredSessions returns session IDs where LastModified is older than the TTL.
// Only considers sessions that have been refreshed (RefreshedAt is not zero).
func (c *SandboxStateCache) ExpiredSessions(ttl time.Duration) []string {
	c.mu.RLock()
	defer c.mu.RUnlock()

	now := time.Now()
	var expired []string
	for sessionID, entry := range c.entries {
		// Only consider sessions that have been refreshed
		if entry.RefreshedAt.IsZero() {
			continue
		}
		// Check if the state is expired based on LastModified
		if !entry.LastModified.IsZero() && now.Sub(entry.LastModified) > ttl {
			expired = append(expired, sessionID)
		}
	}
	return expired
}
