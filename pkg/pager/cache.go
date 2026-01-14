package pager

import (
	"container/list"
	"sync"

	"mash-db/internal/common"
)

// CacheEntry represents a cached page with LRU tracking
type CacheEntry struct {
	PageNum uint32
	Page    *Page
	element *list.Element // Position in LRU list
}

// LRUCache is a thread-safe LRU cache for database pages
type LRUCache struct {
	capacity int
	cache    map[uint32]*CacheEntry
	lruList  *list.List // Front = most recently used, Back = least recently used
	mu       sync.RWMutex
	hits     uint64
	misses   uint64
}

// NewLRUCache creates a new LRU cache with the given capacity
func NewLRUCache(capacity int) *LRUCache {
	if capacity <= 0 {
		capacity = 100
	}
	return &LRUCache{
		capacity: capacity,
		cache:    make(map[uint32]*CacheEntry),
		lruList:  list.New(),
	}
}

// Get retrieves a page from the cache
// Returns nil if not found
func (c *LRUCache) Get(pageNum uint32) *Page {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[pageNum]; ok {
		// Move to front (most recently used)
		c.lruList.MoveToFront(entry.element)
		c.hits++
		return entry.Page
	}
	c.misses++
	return nil
}

// Put adds or updates a page in the cache
// Returns evicted page (if any) for flushing
func (c *LRUCache) Put(pageNum uint32, page *Page) *CacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check if already in cache
	if entry, ok := c.cache[pageNum]; ok {
		entry.Page = page
		c.lruList.MoveToFront(entry.element)
		return nil
	}

	// Check if we need to evict
	var evicted *CacheEntry
	if c.lruList.Len() >= c.capacity {
		evicted = c.evictLRU()
	}

	// Add new entry
	entry := &CacheEntry{
		PageNum: pageNum,
		Page:    page,
	}
	entry.element = c.lruList.PushFront(entry)
	c.cache[pageNum] = entry

	return evicted
}

// evictLRU removes the least recently used unpinned page
// Must be called with lock held
func (c *LRUCache) evictLRU() *CacheEntry {
	// Start from back (least recently used)
	for e := c.lruList.Back(); e != nil; e = e.Prev() {
		entry := e.Value.(*CacheEntry)
		// Only evict unpinned pages
		if entry.Page.PinCnt == 0 {
			c.lruList.Remove(e)
			delete(c.cache, entry.PageNum)
			return entry
		}
	}
	return nil
}

// Remove removes a specific page from the cache
func (c *LRUCache) Remove(pageNum uint32) *CacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[pageNum]; ok {
		c.lruList.Remove(entry.element)
		delete(c.cache, pageNum)
		return entry
	}
	return nil
}

// Contains checks if a page is in the cache
func (c *LRUCache) Contains(pageNum uint32) bool {
	c.mu.RLock()
	defer c.mu.RUnlock()
	_, ok := c.cache[pageNum]
	return ok
}

// Size returns the current number of pages in the cache
func (c *LRUCache) Size() int {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.lruList.Len()
}

// Capacity returns the maximum capacity of the cache
func (c *LRUCache) Capacity() int {
	return c.capacity
}

// Stats returns cache hit/miss statistics
func (c *LRUCache) Stats() (hits, misses uint64) {
	c.mu.RLock()
	defer c.mu.RUnlock()
	return c.hits, c.misses
}

// HitRate returns the cache hit rate as a percentage
func (c *LRUCache) HitRate() float64 {
	c.mu.RLock()
	defer c.mu.RUnlock()
	total := c.hits + c.misses
	if total == 0 {
		return 0
	}
	return float64(c.hits) / float64(total) * 100
}

// Clear removes all entries from the cache
// Returns all dirty pages for flushing
func (c *LRUCache) Clear() []*CacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	var dirtyPages []*CacheEntry
	for _, entry := range c.cache {
		if entry.Page.Dirty {
			dirtyPages = append(dirtyPages, entry)
		}
	}

	c.cache = make(map[uint32]*CacheEntry)
	c.lruList = list.New()

	return dirtyPages
}

// GetAllDirty returns all dirty pages in the cache
func (c *LRUCache) GetAllDirty() []*CacheEntry {
	c.mu.RLock()
	defer c.mu.RUnlock()

	var dirtyPages []*CacheEntry
	for _, entry := range c.cache {
		if entry.Page.Dirty {
			dirtyPages = append(dirtyPages, entry)
		}
	}
	return dirtyPages
}

// ForEach iterates over all cached pages
func (c *LRUCache) ForEach(fn func(pageNum uint32, page *Page) bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	for pageNum, entry := range c.cache {
		if !fn(pageNum, entry.Page) {
			return
		}
	}
}

// Touch marks a page as recently used without modifying it
func (c *LRUCache) Touch(pageNum uint32) {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[pageNum]; ok {
		c.lruList.MoveToFront(entry.element)
	}
}

// Pin increments the pin count for a cached page
func (c *LRUCache) Pin(pageNum uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[pageNum]; ok {
		entry.Page.PinCnt++
		return true
	}
	return false
}

// Unpin decrements the pin count for a cached page
func (c *LRUCache) Unpin(pageNum uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[pageNum]; ok {
		if entry.Page.PinCnt > 0 {
			entry.Page.PinCnt--
		}
		return true
	}
	return false
}

// MarkDirty marks a cached page as dirty
func (c *LRUCache) MarkDirty(pageNum uint32) bool {
	c.mu.Lock()
	defer c.mu.Unlock()

	if entry, ok := c.cache[pageNum]; ok {
		entry.Page.Dirty = true
		return true
	}
	return false
}

// EvictUnpinned evicts all unpinned pages and returns dirty ones for flushing
func (c *LRUCache) EvictUnpinned() []*CacheEntry {
	c.mu.Lock()
	defer c.mu.Unlock()

	var dirtyPages []*CacheEntry
	var toRemove []uint32

	for pageNum, entry := range c.cache {
		if entry.Page.PinCnt == 0 {
			if entry.Page.Dirty {
				dirtyPages = append(dirtyPages, entry)
			}
			toRemove = append(toRemove, pageNum)
		}
	}

	for _, pageNum := range toRemove {
		if entry, ok := c.cache[pageNum]; ok {
			c.lruList.Remove(entry.element)
			delete(c.cache, pageNum)
		}
	}

	return dirtyPages
}

// NewPage creates a new empty page
func NewPage() *Page {
	return &Page{
		Data:   [common.PageSize]byte{},
		Dirty:  false,
		PinCnt: 0,
	}
}
