package pager

import (
	"os"
	"path/filepath"
	"testing"

	"mash-db/internal/common"
)

func TestNewPager(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer p.Close()

	if p.NumPages() != 0 {
		t.Errorf("Expected 0 pages, got %d", p.NumPages())
	}

	if p.FilePath() != dbPath {
		t.Errorf("Expected path %s, got %s", dbPath, p.FilePath())
	}
}

func TestWriteAndReadPage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	// Write test data
	testData := make([]byte, common.PageSize)
	copy(testData, []byte("Hello, MashDB!"))

	err = p.WritePage(0, testData)
	if err != nil {
		t.Fatalf("Failed to write page: %v", err)
	}

	// Flush to disk
	err = p.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	p.Close()

	// Reopen and verify
	p2, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to reopen pager: %v", err)
	}
	defer p2.Close()

	if p2.NumPages() != 1 {
		t.Errorf("Expected 1 page, got %d", p2.NumPages())
	}

	page, err := p2.ReadPage(0)
	if err != nil {
		t.Fatalf("Failed to read page: %v", err)
	}

	if string(page.Data[:14]) != "Hello, MashDB!" {
		t.Errorf("Data mismatch: got %s", string(page.Data[:14]))
	}
}

func TestMultiplePages(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer p.Close()

	// Write multiple pages
	for i := uint32(0); i < 5; i++ {
		data := make([]byte, common.PageSize)
		data[0] = byte(i)
		err = p.WritePage(i, data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
	}

	err = p.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	if p.NumPages() != 5 {
		t.Errorf("Expected 5 pages, got %d", p.NumPages())
	}

	// Verify each page
	for i := uint32(0); i < 5; i++ {
		page, err := p.ReadPage(i)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", i, err)
		}
		if page.Data[0] != byte(i) {
			t.Errorf("Page %d: expected first byte %d, got %d", i, i, page.Data[0])
		}
	}
}

func TestAllocatePage(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer p.Close()

	page0 := p.AllocatePage()
	page1 := p.AllocatePage()
	page2 := p.AllocatePage()

	if page0 != 0 || page1 != 1 || page2 != 2 {
		t.Errorf("Expected pages 0,1,2 got %d,%d,%d", page0, page1, page2)
	}

	if p.NumPages() != 3 {
		t.Errorf("Expected 3 pages, got %d", p.NumPages())
	}
}

func TestCacheEviction(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Small cache size to force eviction
	p, err := New(dbPath, 3)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer p.Close()

	// Write more pages than cache can hold
	for i := uint32(0); i < 10; i++ {
		data := make([]byte, common.PageSize)
		data[0] = byte(i)
		err = p.WritePage(i, data)
		if err != nil {
			t.Fatalf("Failed to write page %d: %v", i, err)
		}
		// Unpin the page so it can be evicted
		p.UnpinPage(i, false)
	}

	err = p.Flush()
	if err != nil {
		t.Fatalf("Failed to flush: %v", err)
	}

	// All pages should still be readable
	for i := uint32(0); i < 10; i++ {
		page, err := p.ReadPage(i)
		if err != nil {
			t.Fatalf("Failed to read page %d: %v", i, err)
		}
		if page.Data[0] != byte(i) {
			t.Errorf("Page %d: expected first byte %d, got %d", i, i, page.Data[0])
		}
		p.UnpinPage(i, false)
	}
}

func TestPageOutOfBounds(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer p.Close()

	_, err = p.ReadPage(common.MaxPages)
	if err != ErrPageOutOfBounds {
		t.Errorf("Expected ErrPageOutOfBounds, got %v", err)
	}
}

func TestInvalidPageSize(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}
	defer p.Close()

	err = p.WritePage(0, []byte("too small"))
	if err != ErrInvalidPageSize {
		t.Errorf("Expected ErrInvalidPageSize, got %v", err)
	}
}

func TestPersistence(t *testing.T) {
	tmpDir := t.TempDir()
	dbPath := filepath.Join(tmpDir, "test.db")

	// Create and write
	p, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to create pager: %v", err)
	}

	data := make([]byte, common.PageSize)
	copy(data, []byte("Persistent data"))
	p.WritePage(0, data)
	p.Flush()
	p.Close()

	// Verify file exists
	info, err := os.Stat(dbPath)
	if err != nil {
		t.Fatalf("Database file not found: %v", err)
	}

	if info.Size() != common.PageSize {
		t.Errorf("Expected file size %d, got %d", common.PageSize, info.Size())
	}

	// Reopen and verify
	p2, err := New(dbPath, 10)
	if err != nil {
		t.Fatalf("Failed to reopen: %v", err)
	}
	defer p2.Close()

	page, _ := p2.ReadPage(0)
	if string(page.Data[:15]) != "Persistent data" {
		t.Errorf("Data not persisted correctly")
	}
}
