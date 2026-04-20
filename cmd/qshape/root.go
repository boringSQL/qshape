package main

import (
	"io"
	"os"

	"github.com/spf13/cobra"
)

func Run() error {
	root := &cobra.Command{
		Use:   "qshape",
		Short: "Canonicalize, fingerprint, and cluster SQL at the AST level",
	}
	root.AddCommand(
		normalizeCmd(),
		fingerprintCmd(),
		captureCmd(),
		attributeCmd(),
		regresqlStubCmd(),
		versionCmd(),
	)
	return root.Execute()
}

func readSQLArg(args []string) (string, error) {
	if len(args) == 1 && args[0] != "-" {
		return args[0], nil
	}
	b, err := io.ReadAll(os.Stdin)
	if err != nil {
		return "", err
	}
	return string(b), nil
}
