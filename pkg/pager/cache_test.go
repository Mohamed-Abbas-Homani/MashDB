package pager

import (
	"testing"
)

func TestLRUCache_BasicOperations(t *testing.T) {
	cache := NewLRUCache(3)

	// Test empty cache
	if cache.Size() != 0 {
		t.Errorf("Expected size 0, got %d", cache.Size())
	}

	// Add pages
	page1 := NewPage()
	page1.Data[0] = 1
	cache.Put(1, page1)

	page2 := NewPage()
	page2.Data[0] = 2
	cache.Put(2, page2)

	if cache.Size() != 2 {
		t.Errorf("Expected size 2, got %d", cache.Size())
	}

	// Get existing page
	got := cache.Get(1)
	if got == nil {
		t.Error("Expected to get page 1")
	}
	if got.Data[0] != 1 {
		t.Errorf("Expected data[0]=1, got %d", got.Data[0])
	}

	// Get non-existing page
	got = cache.Get(999)
	if got != nil {
		t.Error("Expected nil for non-existing page")
	}
}

func TestLRUCache_Eviction(t *testing.T) {
	cache := NewLRUCache(3)

	// Fill cache
	for i := uint32(0); i < 3; i++ {
		page := NewPage()
		page.Data[0] = byte(i)
		cache.Put(i, page)
	}

	if cache.Size() != 3 {
		t.Errorf("Expected size 3, got %d", cache.Size())
	}

	// Add one more - should evict LRU (page 0)
	page := NewPage()
	page.Data[0] = 99
	evicted := cache.Put(99, page)

	if evicted == nil {
		t.Error("Expected eviction")
	}
	if evicted.PageNum != 0 {
		t.Errorf("Expected page 0 to be evicted, got %d", evicted.PageNum)
	}

	// Verify page 0 is no longer in cache
	if cache.Contains(0) {
		t.Error("Page 0 should have been evicted")
	}

	// Verify new page is in cache
	if !cache.Contains(99) {
		t.Error("Page 99 should be in cache")
	}
}

func TestLRUCache_LRUOrder(t *testing.T) {
	cache := NewLRUCache(3)

	// Add pages 0, 1, 2
	for i := uint32(0); i < 3; i++ {
		page := NewPage()
		page.Data[0] = byte(i)
		cache.Put(i, page)
	}

	// Access page 0 to make it most recently used
	cache.Get(0)

	// Add page 3 - should evict page 1 (now LRU)
	page := NewPage()
	evicted := cache.Put(3, page)

	if evicted.PageNum != 1 {
		t.Errorf("Expected page 1 to be evicted (LRU), got %d", evicted.PageNum)
	}
}

func TestLRUCache_PinnedPagesNotEvicted(t *testing.T) {
	cache := NewLRUCache(3)

	// Add pages and pin them
	for i := uint32(0); i < 3; i++ {
		page := NewPage()
		page.PinCnt = 1 // Pin the page
		cache.Put(i, page)
	}

	// Try to add another page - should not evict any (all pinned)
	page := NewPage()
	evicted := cache.Put(99, page)

	if evicted != nil {
		t.Error("Should not evict pinned pages")
	}

	// Cache should have grown beyond capacity
	if cache.Size() != 4 {
		t.Errorf("Expected size 4, got %d", cache.Size())
	}
}

func TestLRUCache_Stats(t *testing.T) {
	cache := NewLRUCache(10)

	page := NewPage()
	cache.Put(1, page)

	// Hit
	cache.Get(1)
	cache.Get(1)

	// Miss
	cache.Get(999)

	hits, misses := cache.Stats()
	if hits != 2 {
		t.Errorf("Expected 2 hits, got %d", hits)
	}
	if misses != 1 {
		t.Errorf("Expected 1 miss, got %d", misses)
	}

	hitRate := cache.HitRate()
	expected := float64(2) / float64(3) * 100
	if hitRate != expected {
		t.Errorf("Expected hit rate %.2f, got %.2f", expected, hitRate)
	}
}

func TestLRUCache_MarkDirty(t *testing.T) {
	cache := NewLRUCache(10)

	page := NewPage()
	cache.Put(1, page)

	if page.Dirty {
		t.Error("Page should not be dirty initially")
	}

	cache.MarkDirty(1)

	got := cache.Get(1)
	if !got.Dirty {
		t.Error("Page should be dirty after MarkDirty")
	}
}

func TestLRUCache_GetAllDirty(t *testing.T) {
	cache := NewLRUCache(10)

	// Add clean and dirty pages
	for i := uint32(0); i < 5; i++ {
		page := NewPage()
		page.Dirty = (i%2 == 0) // Pages 0, 2, 4 are dirty
		cache.Put(i, page)
	}

	dirty := cache.GetAllDirty()
	if len(dirty) != 3 {
		t.Errorf("Expected 3 dirty pages, got %d", len(dirty))
	}
}

func TestLRUCache_Remove(t *testing.T) {
	cache := NewLRUCache(10)

	page := NewPage()
	cache.Put(1, page)

	if !cache.Contains(1) {
		t.Error("Page 1 should be in cache")
	}

	removed := cache.Remove(1)
	if removed == nil {
		t.Error("Expected to remove page 1")
	}

	if cache.Contains(1) {
		t.Error("Page 1 should not be in cache after removal")
	}
}

func TestLRUCache_Clear(t *testing.T) {
	cache := NewLRUCache(10)

	// Add pages
	for i := uint32(0); i < 5; i++ {
		page := NewPage()
		page.Dirty = (i%2 == 0)
		cache.Put(i, page)
	}

	dirty := cache.Clear()
	if len(dirty) != 3 {
		t.Errorf("Expected 3 dirty pages returned, got %d", len(dirty))
	}

	if cache.Size() != 0 {
		t.Errorf("Cache should be empty after clear, got size %d", cache.Size())
	}
}

func TestLRUCache_Touch(t *testing.T) {
	cache := NewLRUCache(3)

	// Add pages 0, 1, 2
	for i := uint32(0); i < 3; i++ {
		page := NewPage()
		cache.Put(i, page)
	}

	// Touch page 0 (without reading its content)
	cache.Touch(0)

	// Add page 3 - should evict page 1 (page 0 was touched, making page 1 LRU)
	page := NewPage()
	evicted := cache.Put(3, page)

	if evicted.PageNum != 1 {
		t.Errorf("Expected page 1 to be evicted, got %d", evicted.PageNum)
	}
}

func TestLRUCache_PinUnpin(t *testing.T) {
	cache := NewLRUCache(10)

	page := NewPage()
	cache.Put(1, page)

	// Pin
	cache.Pin(1)
	got := cache.Get(1)
	if got.PinCnt != 1 {
		t.Errorf("Expected pin count 1, got %d", got.PinCnt)
	}

	// Pin again
	cache.Pin(1)
	got = cache.Get(1)
	if got.PinCnt != 2 {
		t.Errorf("Expected pin count 2, got %d", got.PinCnt)
	}

	// Unpin
	cache.Unpin(1)
	got = cache.Get(1)
	if got.PinCnt != 1 {
		t.Errorf("Expected pin count 1 after unpin, got %d", got.PinCnt)
	}
}

func TestLRUCache_ForEach(t *testing.T) {
	cache := NewLRUCache(10)

	for i := uint32(0); i < 5; i++ {
		page := NewPage()
		page.Data[0] = byte(i)
		cache.Put(i, page)
	}

	count := 0
	cache.ForEach(func(pageNum uint32, page *Page) bool {
		count++
		return true
	})

	if count != 5 {
		t.Errorf("Expected to iterate 5 pages, got %d", count)
	}

	// Test early termination
	count = 0
	cache.ForEach(func(pageNum uint32, page *Page) bool {
		count++
		return count < 3
	})

	if count != 3 {
		t.Errorf("Expected to iterate 3 pages with early termination, got %d", count)
	}
}
