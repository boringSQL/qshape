package main

import (
	"fmt"

	"github.com/spf13/cobra"
)

var (
	version = "dev"
	commit  = "none"
	date    = "unknown"
)

func versionCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "version",
		Short: "Print qshape version",
		Run: func(cmd *cobra.Command, args []string) {
			fmt.Printf("qshape %s (commit %s, built %s)\n", version, commit, date)
		},
	}
}
