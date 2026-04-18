package main

import (
	"fmt"

	"github.com/boringsql/qshape"
	"github.com/spf13/cobra"
)

func normalizeCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "normalize [sql|-]",
		Short: "Parse and deparse SQL to its canonical form",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			sql, err := readSQLArg(args)
			if err != nil {
				return err
			}
			out, err := qshape.Normalize(sql)
			if err != nil {
				return err
			}
			fmt.Println(out)
			return nil
		},
	}
}
