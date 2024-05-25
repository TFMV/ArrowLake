package arrow

import (
	"context"
	"database/sql"
	"fmt"
	"log"

	"github.com/apache/arrow/go/v17/arrow"
	"github.com/apache/arrow/go/v17/arrow/array"
	"github.com/apache/arrow/go/v17/arrow/memory"
	_ "github.com/marcboeker/go-duckdb"
)

type Arrow struct {
	db *sql.DB
}

func NewArrow(db *sql.DB) *Arrow {
	return &Arrow{db: db}
}

func (a *Arrow) QueryArrow(ctx context.Context, query string, args ...interface{}) (arrow.Record, error) {
	rows, err := a.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("failed to execute query: %w", err)
	}
	defer rows.Close()

	columns, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("failed to get columns: %w", err)
	}

	pool := memory.NewGoAllocator()
	builders := make([]array.Builder, len(columns))
	for i, col := range columns {
		switch col.DatabaseTypeName() {
		case "INTEGER":
			builders[i] = array.NewInt32Builder(pool)
		case "DOUBLE":
			builders[i] = array.NewFloat64Builder(pool)
		case "VARCHAR":
			builders[i] = array.NewStringBuilder(pool)
		default:
			return nil, fmt.Errorf("unsupported column type: %s", col.DatabaseTypeName())
		}
	}

	for rows.Next() {
		values := make([]interface{}, len(columns))
		for i := range values {
			switch columns[i].DatabaseTypeName() {
			case "INTEGER":
				var v sql.NullInt32
				values[i] = &v
			case "DOUBLE":
				var v sql.NullFloat64
				values[i] = &v
			case "VARCHAR":
				var v sql.NullString
				values[i] = &v
			default:
				return nil, fmt.Errorf("unsupported column type: %s", columns[i].DatabaseTypeName())
			}
		}

		if err := rows.Scan(values...); err != nil {
			return nil, fmt.Errorf("failed to scan row: %w", err)
		}

		for i, val := range values {
			switch v := val.(type) {
			case *sql.NullInt32:
				builder := builders[i].(*array.Int32Builder)
				if v.Valid {
					builder.Append(v.Int32)
				} else {
					builder.AppendNull()
				}
			case *sql.NullFloat64:
				builder := builders[i].(*array.Float64Builder)
				if v.Valid {
					builder.Append(v.Float64)
				} else {
					builder.AppendNull()
				}
			case *sql.NullString:
				builder := builders[i].(*array.StringBuilder)
				if v.Valid {
					builder.Append(v.String)
				} else {
					builder.AppendNull()
				}
			}
		}
	}

	fieldTypes := make([]arrow.Field, len(columns))
	arrs := make([]arrow.Array, len(columns))
	for i, col := range columns {
		arr := builders[i].NewArray()
		defer arr.Release()
		fieldTypes[i] = arrow.Field{Name: col.Name(), Type: arr.DataType()}
		arrs[i] = arr
	}

	schema := arrow.NewSchema(fieldTypes, nil)
	record := array.NewRecord(schema, arrs, int64(arrs[0].Len()))

	return record, nil
}

func main() {
	// Example usage
	connStr := "host=localhost port=5432 user=postgres password=password dbname=tfmv sslmode=disable"
	db, err := sql.Open("duckdb", connStr)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer db.Close()

	arrowInstance := NewArrow(db)
	ctx := context.Background()
	query := "SELECT id, name, value FROM test_table WHERE id = ?"

	record, err := arrowInstance.QueryArrow(ctx, query, 1)
	if err != nil {
		log.Fatalf("Failed to query Arrow: %v", err)
	}
	defer record.Release()

	for _, col := range record.Columns() {
		fmt.Println(col)
	}
}
