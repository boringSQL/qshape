package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"

	"github.com/boringsql/qshape"
	"github.com/jackc/pgx/v5"
	"github.com/spf13/cobra"
)

func captureCmd() *cobra.Command {
	var (
		minCalls int64
		limit    int
		database string
	)
	cmd := &cobra.Command{
		Use:   "capture <conn-string>",
		Short: "Fetch pg_stat_statements (with timing) from a live PG and cluster it",
		Long: `Connect to a PostgreSQL node, read pg_stat_statements directly,
and emit JSON clusters on stdout.

Requires the pg_stat_statements extension to be loaded and created
in the target database. Requires PostgreSQL 13+ for the *_exec_time
columns; older releases use the legacy total_time naming and are not
supported.

Connection string accepts libpq URLs (postgres://user:pass@host/db)
or keyword/value form (host=... user=... dbname=...).`,
		Args: cobra.ExactArgs(1),
		RunE: func(_ *cobra.Command, args []string) error {
			return runCapture(args[0], minCalls, limit, database)
		},
	}
	cmd.Flags().Int64Var(&minCalls, "min-calls", 0, "exclude queries with calls <= this value")
	cmd.Flags().IntVar(&limit, "limit", 0, "limit to top N by total_exec_time (0 = no limit)")
	cmd.Flags().StringVar(&database, "database", "", "filter to a specific database name (default: all)")
	return cmd
}

func runCapture(connStr string, minCalls int64, limit int, database string) error {
	ctx := context.Background()
	conn, err := pgx.Connect(ctx, connStr)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer conn.Close(ctx)

	var sb strings.Builder
	sb.WriteString(`SELECT s.queryid, s.calls, s.query,
       s.total_exec_time, s.mean_exec_time, s.stddev_exec_time, s.rows
FROM pg_stat_statements s`)
	args := []any{}
	where := []string{}
	if database != "" {
		sb.WriteString("\nJOIN pg_database d ON d.oid = s.dbid")
		where = append(where, fmt.Sprintf("d.datname = $%d", len(args)+1))
		args = append(args, database)
	}
	where = append(where, fmt.Sprintf("s.calls > $%d", len(args)+1))
	args = append(args, minCalls)
	sb.WriteString("\nWHERE ")
	sb.WriteString(strings.Join(where, " AND "))
	sb.WriteString("\nORDER BY s.total_exec_time DESC")
	if limit > 0 {
		sb.WriteString(fmt.Sprintf("\nLIMIT %d", limit))
	}

	rows, err := conn.Query(ctx, sb.String(), args...)
	if err != nil {
		return fmt.Errorf("query pg_stat_statements (extension installed? PG 13+?): %w", err)
	}
	defer rows.Close()

	var queries []qshape.Query
	for rows.Next() {
		var (
			qid, calls, rowCount            int64
			raw                             string
			totalExec, meanExec, stddevExec float64
		)
		if err := rows.Scan(&qid, &calls, &raw, &totalExec, &meanExec, &stddevExec, &rowCount); err != nil {
			return fmt.Errorf("scan: %w", err)
		}
		queries = append(queries, qshape.Query{
			QueryID:          qid,
			Calls:            calls,
			Raw:              raw,
			TotalExecTimeMs:  totalExec,
			MeanExecTimeMs:   meanExec,
			StddevExecTimeMs: stddevExec,
			Rows:             rowCount,
		})
	}
	if err := rows.Err(); err != nil {
		return err
	}

	clusters, err := qshape.Group(queries)
	if err != nil {
		return err
	}

	fmt.Fprintf(os.Stderr, "captured %d queries → %d clusters\n", len(queries), len(clusters))

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	return enc.Encode(clustersDoc{SchemaVersion: currentSchemaVersion, Clusters: clusters})
}
