package main

import (
	"fmt"
	"os"
)

const version = "0.1.0"

func main() {
	if len(os.Args) < 2 {
		printUsage()
		os.Exit(1)
	}

	dbPath := os.Args[1]
	fmt.Printf("MashDB v%s\n", version)
	fmt.Printf("Opening database: %s\n", dbPath)

	// TODO: Initialize pager, btree, and start REPL
	fmt.Println("Database engine not yet implemented. Coming soon!")
}

func printUsage() {
	fmt.Println("MashDB - A simple SQLite-like database in Go")
	fmt.Println()
	fmt.Println("Usage: mashdb <database-file>")
	fmt.Println()
	fmt.Println("Example:")
	fmt.Println("  mashdb mydb.db")
}
