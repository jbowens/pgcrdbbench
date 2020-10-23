package main

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/csv"
	"flag"
	"fmt"
	"math/rand"
	"os"
	"sort"
	"strconv"
	"strings"
	"text/tabwriter"
	"time"

	_ "github.com/lib/pq"
)

const (
	runCount = 10
	rowCount = 1_000_000
)

var (
	rowSizes = []int{16, 32, 512, 1024, 4096}
	queries  = []benchmarkQuery{
		{
			q:     "SELECT SUM(LENGTH(payload)) FROM %s",
			label: "sum-length-payload",
		},
		{
			q:     "SELECT COUNT(number) FROM %s",
			label: "select-count",
		},
	}
)

type benchmarkQuery struct {
	q     string
	label string
}

func main() {
	flag.Parse()
	if flag.NArg() == 0 {
		fmt.Fprintln(os.Stderr, "Need 1 args")
		os.Exit(1)
	}

	cmd := flag.Arg(0)
	switch cmd {
	case "csvs":
		makeFiles()
	case "scan":
		label := flag.Arg(1)
		connURI := flag.Arg(2)
		runScan(label, connURI)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command %q", cmd)
		os.Exit(1)
	}

}

func tableName(sz int) string {
	return fmt.Sprintf("scan_%04d", sz)
}

func makeFiles() {
	rng := rand.New(rand.NewSource(1603296558158))
	for _, sz := range rowSizes {
		filename := tableName(sz) + ".csv"
		f, err := os.Create(filename)
		must(err)
		w := csv.NewWriter(f)
		v := make([]byte, base64.StdEncoding.DecodedLen(sz-16))
		for i := 0; i < rowCount; i++ {
			_, _ = rng.Read(v[:])
			rec := [3]string{
				strconv.Itoa(i),
				strconv.Itoa(rng.Intn(1000)),
				base64.StdEncoding.EncodeToString(v),
			}
			must(w.Write(rec[:]))
			must(err)
		}
		w.Flush()
		must(w.Error())
		info, err := f.Stat()
		must(err)
		must(f.Close())
		fmt.Printf("Wrote %s (%.2f MiB).\n", info.Name(), float64(info.Size())/1024.0/1024.0)
	}
}

func runScan(label, connURI string) {
	ctx := context.Background()
	db, err := sql.Open("postgres", connURI)
	must(err)
	for _, q := range queries {
		filename := fmt.Sprintf("results-%s-%s-rowcount%d.csv", label, q.label, rowCount)
		fmt.Printf("Database: %s, Query: %s\n", label, q.label)
		fmt.Printf("Query: %s\n", q.q)
		fmt.Printf("Saving results to %s.\n", filename)
		fmt.Println()

		results := make([][]time.Duration, len(rowSizes))
		for i, sz := range rowSizes {
			results[i] = make([]time.Duration, runCount)
			tbl := tableName(sz)
			for j := 0; j < runCount; j++ {
				start := time.Now()
				rows, err := db.QueryContext(ctx, fmt.Sprintf(q.q, tbl))
				results[i][j] = time.Now().Sub(start)
				must(err)
				must(rows.Close())
			}
		}

		tw := tabwriter.NewWriter(os.Stdout, 0, 8, 0, '\t', 0)
		f, err := os.Create(filename)
		must(err)
		w := csv.NewWriter(f)
		record := make([]string, len(rowSizes)+1)
		record[0] = "run"
		for i, sz := range rowSizes {
			record[i+1] = fmt.Sprintf("%d B", sz)
		}
		must(w.Write(record))
		_, err = tw.Write([]byte(strings.Join(record, "\t") + "\n"))
		must(err)
		for j := 0; j < runCount; j++ {
			record = record[:0]
			record = append(record, strconv.Itoa(j+1))
			for i := range rowSizes {
				record = append(record, strconv.FormatInt(results[i][j].Milliseconds(), 10))
			}
			must(w.Write(record))
			_, err = tw.Write([]byte(strings.Join(record, "\t") + "\n"))
			must(err)
		}

		// Include medians.
		record = record[:0]
		record = append(record, "median")
		for i := range rowSizes {
			record = append(record, strconv.FormatInt(median(results[i]).Milliseconds(), 10))
		}
		must(w.Write(record))
		_, err = tw.Write([]byte(strings.Join(record, "\t") + "\n"))
		must(err)
		w.Flush()
		must(w.Error())
		must(f.Close())
		must(tw.Flush())
		fmt.Println()
		fmt.Println()
	}
	must(db.Close())
}

func median(durs []time.Duration) time.Duration {
	sortedDurs := make([]int, len(durs))
	for i, d := range durs {
		sortedDurs[i] = int(d.Nanoseconds())
	}
	sort.Ints(sortedDurs)
	if len(sortedDurs)%2 == 1 {
		return time.Duration(sortedDurs[len(sortedDurs)/2])
	}
	m := (float64(sortedDurs[len(sortedDurs)/2]) + float64(sortedDurs[len(sortedDurs)/2+1])) / 2.0
	return time.Duration(m)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
