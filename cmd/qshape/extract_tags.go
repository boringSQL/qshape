package main

import (
	"encoding/json"
	"io"
	"os"

	"github.com/boringsql/qshape/tags"
	"github.com/spf13/cobra"
)

func extractTagsCmd() *cobra.Command {
	var policyPath string
	cmd := &cobra.Command{
		Use:   "extract-tags",
		Short: "Extract + classify tags from one SQL statement on stdin",
		Args:  cobra.NoArgs,
		RunE: func(_ *cobra.Command, _ []string) error {
			return runExtractTags(policyPath, os.Stdin, os.Stdout)
		},
	}
	cmd.Flags().StringVar(&policyPath, "policy-file", "", "JSON policy override")
	return cmd
}

func runExtractTags(policyPath string, in io.Reader, out io.Writer) error {
	raw, err := io.ReadAll(in)
	if err != nil {
		return err
	}
	policy, err := loadPolicy(policyPath)
	if err != nil {
		return err
	}
	classified := tags.Classify(tags.Extract(string(raw)), policy)
	enc := json.NewEncoder(out)
	enc.SetIndent("", "  ")
	return enc.Encode(classified)
}
