package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/arrow/go/arrow"
	"github.com/apache/arrow/go/arrow/array"
	"github.com/apache/arrow/go/arrow/memory"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	_ "github.com/mattn/go-sqlite3"
)

type Config struct {
	Threads        int
	BatchSize      int
	Query          string
	SDBSystem      string
	SDBName        string
	SDBUser        string
	SDBPassword    string
	TDBSystem      string
	TTable         string
	TDBName        string
	TDBUser        string
	TDBPassword    string
	ConnectionType string
	UnixSocketPath string
}

func createArrowSchemaAndData(rows *sql.Rows, mem memory.Allocator) (*arrow.Schema, []array.Interface, error) {
	columnTypes, err := rows.ColumnTypes()
	if err != nil {
		return nil, nil, err
	}

	fields := make([]arrow.Field, len(columnTypes))
	for i, ct := range columnTypes {
		arrowType, nullable := sqlTypeToArrowType(ct.DatabaseTypeName())
		fields[i] = arrow.Field{Name: ct.Name(), Type: arrowType, Nullable: nullable}
	}
	schema := arrow.NewSchema(fields, nil)

	builders := make([]array.Builder, len(fields))
	for i, field := range fields {
		builders[i] = array.NewBuilder(mem, field.Type)
		defer builders[i].Release()
	}

	columnVals := make([]interface{}, len(columnTypes))
	for i := range columnVals {
		columnVals[i] = new(interface{})
	}

	for rows.Next() {
		if err := rows.Scan(columnVals...); err != nil {
			return nil, nil, err
		}

		for i, columnVal := range columnVals {
			if columnVal == nil {
				builders[i].AppendNull()
				continue
			}

			switch val := columnVal.(type) {
			case int32:
				b := builders[i].(*array.Int32Builder)
				b.Append(val)
			case int64:
				b := builders[i].(*array.Int64Builder)
				b.Append(val)
			case float32:
				b := builders[i].(*array.Float32Builder)
				b.Append(val)
			case float64:
				b := builders[i].(*array.Float64Builder)
				b.Append(val)
			case string:
				b := builders[i].(*array.StringBuilder)
				b.Append(val)
			case bool:
				b := builders[i].(*array.BooleanBuilder)
				b.Append(val)
			default:
				return nil, nil, fmt.Errorf("unsupported type: %T", val)
			}
		}
	}

	arrays := make([]array.Interface, len(builders))
	for i, builder := range builders {
		arrays[i] = builder.NewArray()
		builder.Release()
	}

	return schema, arrays, nil
}

func sqlTypeToArrowType(sqlType string) (arrow.DataType, bool) {
	switch strings.ToUpper(sqlType) {
	case "INT":
		return arrow.PrimitiveTypes.Int32, true
	case "BIGINT":
		return arrow.PrimitiveTypes.Int64, true
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32, true
	case "DOUBLE":
		return arrow.PrimitiveTypes.Float64, true
	case "VARCHAR", "CHAR", "TEXT":
		return arrow.BinaryTypes.String, true
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean, true
	case "TIMESTAMP":
		return &arrow.TimestampType{Unit: arrow.Second}, true
	default:
		return nil, false
	}
}

func main() {

	// var config Config
	var rowsProcessed int64
	start := time.Now()

	// Command-line flags
	// flag.IntVar(&config.Threads, "Threads", 4, "The number of concurrent threads")
	// flag.IntVar(&config.BatchSize, "BatchSize", 10000, "The number of inserts to batch")
	// [Other flag definitions]
	// flag.Parse()

	config := Config{
		Threads:        1,
		BatchSize:      1000,
		Query:          "SELECT * FROM your_sqlite_table;",
		SDBSystem:      "sqlite",
		SDBName:        "your_sqlite_db.sqlite",
		SDBUser:        "",
		SDBPassword:    "",
		TDBSystem:      "postgres",
		TTable:         "your_postgres_table",
		TDBName:        "your_postgres_db",
		TDBUser:        "postgres",
		TDBPassword:    "postgres",
		ConnectionType: "unix",
		UnixSocketPath: "/cloudsql/your_project_id:your_region:your_instance_id",
	}

	type Output struct {
		StartTime     time.Time `json:"start_time"`
		RowsProcessed int       `json:"rows_processed"`
		RowsPerSecond float64   `json:"rows_per_second"`
		TotalTime     float64   `json:"total_time"`
	}

	sqliteDB, err := sql.Open("sqlite3", "your_sqlite_db.sqlite")
	if err != nil {
		log.Fatalf("Failed to open SQLite DB: %v", err)
	}
	defer sqliteDB.Close()

	query := "SELECT * FROM your_sqlite_table;" // Replace with your query
	// Execute the query
	rows, err := sqliteDB.Query(query)
	if err != nil {
		log.Fatalf("Failed to execute query: %v", err)
	}
	defer rows.Close()

	// Create Arrow memory allocator
	allocator := memory.NewGoAllocator()

	// Create Apache Arrow schema and data arrays
	schema, dataArrays, err := createArrowSchemaAndData(rows, allocator)

	columns := make([]string, len(schema.Fields()))

	var connectionString string

	if config.ConnectionType == "unix" {
		// Unix domain socket connection
		dbPassword := url.QueryEscape(config.TDBPassword)
		connectionString = fmt.Sprintf("postgresql://%s:%s@/cloudsql/%s/%s", config.TDBUser, dbPassword, config.UnixSocketPath, config.TDBName)
	} else {
		// TCP/IP connection
		dbPassword := url.QueryEscape(config.TDBPassword)
		connectionString = fmt.Sprintf("postgresql://%s:%s@localhost:5432/%s?timezone=UTC", config.TDBUser, dbPassword, config.TDBName)
	}

	pool, err := pgxpool.New(context.Background(), connectionString)
	if err != nil {
		log.Fatal("Failed to connect to Postgres:", err.Error())
	}
	defer pool.Close()

	// Create a channel to hold the batches
	rowsChannel := make(chan [][]interface{})

	// WaitGroup to wait for all processing goroutines to finish
	var wg sync.WaitGroup

	// Start multiple goroutines for processing batches
	for i := 0; i < config.Threads; i++ {
		wg.Add(1)
		go processBatch(pool, rowsChannel, &wg, &rowsProcessed, config.TTable, columns)
	}

	// Split data into batches and send to rowsChannel
	var batch [][]interface{}
	for _, array := range dataArrays {
		// [Convert array.Interface into [][]interface{} and send to rowsChannel]
		rowsChannel <- batch
		batch = nil

		// Release memory used by this array
		array.Release()
	}
	if len(batch) > 0 {
		rowsChannel <- batch
	}

	// Close channel and wait for all goroutines to finish
	close(rowsChannel)
	wg.Wait()

	// Output
	output := Output{
		StartTime:     start,
		RowsProcessed: int(atomic.LoadInt64(&rowsProcessed)),
		RowsPerSecond: float64(atomic.LoadInt64(&rowsProcessed)) / time.Since(start).Seconds(),
		TotalTime:     time.Since(start).Seconds(),
	}

	outputJSON, err := json.Marshal(output)
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(string(outputJSON))

}

func processBatch(pool *pgxpool.Pool, rowsChannel <-chan [][]interface{}, wg *sync.WaitGroup, rowsProcessed *int64, tableName string, columnNames []string) {
	defer wg.Done()

	var batchWG sync.WaitGroup

	for rows := range rowsChannel {
		batchWG.Add(1)

		go func(rows [][]interface{}) {
			defer batchWG.Done()

			copyData := make([][]interface{}, len(rows))

			for i, row := range rows {
				copyData[i] = make([]interface{}, len(row))
				for j, val := range row {
					copyData[i][j] = val
				}
			}

			tableIdentifier := pgx.Identifier{tableName}

			// Use pgx.CopyFrom to efficiently insert the batch into the database
			_, err := pool.CopyFrom(context.Background(), tableIdentifier, columnNames, pgx.CopyFromRows(copyData))
			if err != nil {
				log.Fatal(err)
			}
			atomic.AddInt64(rowsProcessed, int64(len(rows)))
		}(rows)
	}

	batchWG.Wait()
}
