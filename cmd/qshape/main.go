package main

import "os"

func main() {
	if err := Run(); err != nil {
		os.Exit(1)
	}
}
