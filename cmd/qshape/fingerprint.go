package main

import (
	"fmt"

	"github.com/boringsql/qshape"
	"github.com/spf13/cobra"
)

func fingerprintCmd() *cobra.Command {
	return &cobra.Command{
		Use:   "fingerprint [sql|-]",
		Short: "Print the stable AST fingerprint for a SQL statement",
		Args:  cobra.MaximumNArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			sql, err := readSQLArg(args)
			if err != nil {
				return err
			}
			fp, err := qshape.Fingerprint(sql)
			if err != nil {
				return err
			}
			fmt.Println(fp)
			return nil
		},
	}
}
