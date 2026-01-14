package common

const (
	// PageSize is the size of each page in bytes (4KB)
	PageSize = 4096

	// MaxPages is the maximum number of pages in a database file
	MaxPages = 1000000

	// HeaderPageNum is the page number reserved for the database header
	HeaderPageNum = 0
)
