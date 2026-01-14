package pager

import (
	"errors"
	"fmt"
	"os"
	"sync"

	"mash-db/internal/common"
)

var (
	ErrPageOutOfBounds = errors.New("page number out of bounds")
	ErrFileClosed      = errors.New("pager file is closed")
	ErrInvalidPageSize = errors.New("data size does not match page size")
	ErrAllPagesPinned  = errors.New("all pages are pinned, cannot evict")
)

// Page represents a single page of data
type Page struct {
	Data   [common.PageSize]byte
	Dirty  bool // Has been modified but not flushed
	PinCnt int  // Number of users currently using this page
}

// Pager manages reading and writing fixed-size pages to/from disk
type Pager struct {
	file     *os.File
	filePath string
	numPages uint32
	cache    *LRUCache
	mu       sync.Mutex
	closed   bool
}

// New creates a new Pager for the given file path
// If the file doesn't exist, it will be created
func New(filePath string, cacheSize int) (*Pager, error) {
	file, err := os.OpenFile(filePath, os.O_RDWR|os.O_CREATE, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open file: %w", err)
	}

	// Get file size to determine number of existing pages
	stat, err := file.Stat()
	if err != nil {
		file.Close()
		return nil, fmt.Errorf("failed to stat file: %w", err)
	}

	numPages := uint32(stat.Size() / common.PageSize)

	if cacheSize <= 0 {
		cacheSize = 100 // Default cache size
	}

	return &Pager{
		file:     file,
		filePath: filePath,
		numPages: numPages,
		cache:    NewLRUCache(cacheSize),
	}, nil
}

// NumPages returns the total number of pages in the file
func (p *Pager) NumPages() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.numPages
}

// ReadPage reads a page from disk or cache and pins it
// Caller must call UnpinPage when done with the page
func (p *Pager) ReadPage(pageNum uint32) (*Page, error) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil, ErrFileClosed
	}

	if pageNum >= common.MaxPages {
		return nil, ErrPageOutOfBounds
	}

	// Check cache first
	if page := p.cache.Get(pageNum); page != nil {
		page.PinCnt++
		return page, nil
	}

	// Page not in cache, read from disk
	page := NewPage()
	page.PinCnt = 1

	// If page exists in file, read it
	if pageNum < p.numPages {
		offset := int64(pageNum) * common.PageSize
		n, err := p.file.ReadAt(page.Data[:], offset)
		if err != nil && n != common.PageSize {
			return nil, fmt.Errorf("failed to read page %d: %w", pageNum, err)
		}
	}
	// If page doesn't exist yet, it's a new page (zeroed out)

	// Add to cache, handle eviction
	if evicted := p.cache.Put(pageNum, page); evicted != nil {
		if evicted.Page.Dirty {
			if err := p.flushPageInternal(evicted.PageNum, evicted.Page); err != nil {
				// Log error but continue - page is already evicted
				fmt.Printf("warning: failed to flush evicted page %d: %v\n", evicted.PageNum, err)
			}
		}
	}

	return page, nil
}

// WritePage writes data to a page (creates if doesn't exist)
// The page is marked dirty and will be flushed on Flush() or eviction
func (p *Pager) WritePage(pageNum uint32, data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrFileClosed
	}

	if len(data) != common.PageSize {
		return ErrInvalidPageSize
	}

	if pageNum >= common.MaxPages {
		return ErrPageOutOfBounds
	}

	// Check if page is in cache
	page := p.cache.Get(pageNum)
	if page == nil {
		// Create new page
		page = NewPage()
		if evicted := p.cache.Put(pageNum, page); evicted != nil {
			if evicted.Page.Dirty {
				if err := p.flushPageInternal(evicted.PageNum, evicted.Page); err != nil {
					return fmt.Errorf("failed to flush evicted page: %w", err)
				}
			}
		}
	}

	copy(page.Data[:], data)
	page.Dirty = true

	// Extend file tracking if necessary
	if pageNum >= p.numPages {
		p.numPages = pageNum + 1
	}

	return nil
}

// GetPage returns a page for modification (creates if doesn't exist)
// The page is pinned - caller must call UnpinPage when done
func (p *Pager) GetPage(pageNum uint32) (*Page, error) {
	return p.ReadPage(pageNum)
}

// UnpinPage decrements the pin count for a page
// If dirty is true, marks the page as dirty
func (p *Pager) UnpinPage(pageNum uint32, dirty bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if page := p.cache.Get(pageNum); page != nil {
		if page.PinCnt > 0 {
			page.PinCnt--
		}
		if dirty {
			page.Dirty = true
		}
	}
}

// Flush writes all dirty pages to disk
func (p *Pager) Flush() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrFileClosed
	}

	return p.flushAllInternal()
}

// flushAllInternal flushes all dirty pages (must hold lock)
func (p *Pager) flushAllInternal() error {
	dirtyPages := p.cache.GetAllDirty()
	for _, entry := range dirtyPages {
		if err := p.flushPageInternal(entry.PageNum, entry.Page); err != nil {
			return err
		}
	}
	return p.file.Sync()
}

// FlushPage writes a specific page to disk if dirty
func (p *Pager) FlushPage(pageNum uint32) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrFileClosed
	}

	page := p.cache.Get(pageNum)
	if page == nil || !page.Dirty {
		return nil
	}

	return p.flushPageInternal(pageNum, page)
}

// flushPageInternal writes a page to disk (must hold lock)
func (p *Pager) flushPageInternal(pageNum uint32, page *Page) error {
	offset := int64(pageNum) * common.PageSize
	_, err := p.file.WriteAt(page.Data[:], offset)
	if err != nil {
		return fmt.Errorf("failed to write page %d: %w", pageNum, err)
	}
	page.Dirty = false
	return nil
}

// AllocatePage returns the next available page number
func (p *Pager) AllocatePage() uint32 {
	p.mu.Lock()
	defer p.mu.Unlock()
	pageNum := p.numPages
	p.numPages++
	return pageNum
}

// Close flushes all pages and closes the file
func (p *Pager) Close() error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return nil
	}

	if err := p.flushAllInternal(); err != nil {
		return err
	}

	p.closed = true
	return p.file.Close()
}

// FilePath returns the path to the database file
func (p *Pager) FilePath() string {
	return p.filePath
}

// CacheStats returns cache hit/miss statistics
func (p *Pager) CacheStats() (hits, misses uint64, hitRate float64) {
	hits, misses = p.cache.Stats()
	hitRate = p.cache.HitRate()
	return
}

// CacheSize returns the current number of pages in cache
func (p *Pager) CacheSize() int {
	return p.cache.Size()
}

// CacheCapacity returns the maximum cache capacity
func (p *Pager) CacheCapacity() int {
	return p.cache.Capacity()
}
