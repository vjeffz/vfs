package main

import (
	"fmt"
	"log"
	"os"

	"github.com/vjeffz/vfs/vfs"
)

func usage() {
	fmt.Println(`Usage:
  vfs encode <inputfile> s3://bucket/prefix/ [--force]
  vfs restore s3://bucket/prefix/ <outputfile>
  vfs delete s3://bucket/prefix/`)
}

func main() {
	if len(os.Args) < 3 {
		usage()
		os.Exit(1)
	}

	v, err := vfs.New()
	if err != nil {
		log.Fatalf("Failed to initialize VFS: %v", err)
	}

	switch os.Args[1] {
	case "encode":
		force := len(os.Args) == 5 && os.Args[4] == "--force"
		if len(os.Args) != 4 && !force {
			usage()
			os.Exit(1)
		}
		err = v.Encode(os.Args[2], os.Args[3], force)
	case "restore":
		if len(os.Args) != 4 {
			usage()
			os.Exit(1)
		}
		err = v.Restore(os.Args[2], os.Args[3])
	case "delete":
		if len(os.Args) != 3 {
			usage()
			os.Exit(1)
		}
		err = v.Delete(os.Args[2])
	default:
		fmt.Printf("Unknown command: %s\n", os.Args[1])
		usage()
		os.Exit(1)
	}

	if err != nil {
		log.Fatalf("%s failed: %v", os.Args[1], err)
	}
}

