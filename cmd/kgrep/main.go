package main

import (
	"fmt"
	"os"

	"kgrep/internal/app"
)

var version = "dev"

func main() {
	args := os.Args[1:]
	if len(args) == 1 && (args[0] == "--version" || args[0] == "-version") {
		fmt.Println(version)
		return
	}
	os.Exit(app.Run(version, args, os.Stdout, os.Stderr))
}
