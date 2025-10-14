package main

import (
	"log"
	"os"
)

func main() {
	if len(os.Args) > 1 {
		switch os.Args[1] {
		case "export":
			export(os.Args[2:])
			return
		case "load":
			load(os.Args[2:])
			return
		case "migrate":
			migrate(os.Args[2:])
			return
		}
	}
	log.Println("Usage:")
	log.Println("  logs-migrator export -dsn ... -table ... -pk ... -out ./export [flags]")
	log.Println("  logs-migrator load -dsn ... -tar ./export.tar.gz -dst-table ... -dst-columns ... [flags]")
}
