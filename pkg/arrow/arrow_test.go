// --------------------------------------------------------------------------------
// Author: Thomas F McGeehan V
//
// This file is part of a software project developed by Thomas F McGeehan V.
//
// Permission is hereby granted, free of charge, to any person obtaining a copy
// of this software and associated documentation files (the "Software"), to deal
// in the Software without restriction, including without limitation the rights
// to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
// copies of the Software, and to permit persons to whom the Software is
// furnished to do so, subject to the following conditions:
//
// The above copyright notice and this permission notice shall be included in all
// copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
// IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
// FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
// AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
// LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
// OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
// SOFTWARE.
//
// For more information about the MIT License, please visit:
// https://opensource.org/licenses/MIT
//
// Acknowledgment appreciated but not required.
// --------------------------------------------------------------------------------

package arrow

import (
	"context"
	"database/sql"
	"testing"

	"github.com/apache/arrow/go/v17/arrow/array"
	_ "github.com/marcboeker/go-duckdb"
	"github.com/stretchr/testify/require"
)

func setupDuckDBWithArrow() (*sql.DB, *Arrow, error) {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return nil, nil, err
	}

	_, err = db.Exec(`
		CREATE TABLE test_table (
			id INTEGER,
			name VARCHAR,
			value DOUBLE
		);
		INSERT INTO test_table VALUES (1, 'Alice', 10.5), (2, 'Bob', 20.75), (3, 'Charlie', 30.0);
	`)
	if err != nil {
		return nil, nil, err
	}

	arrowInstance := NewArrow(db)
	return db, arrowInstance, nil
}

func TestQueryArrow(t *testing.T) {
	db, arrowInstance, err := setupDuckDBWithArrow()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	query := "SELECT id, name, value FROM test_table WHERE id = ?"

	record, err := arrowInstance.QueryArrow(ctx, query, 1)
	require.NoError(t, err)
	defer record.Release()

	require.Equal(t, int64(1), record.NumRows())
	require.Equal(t, int64(3), record.NumCols())

	col1 := record.Column(0).(*array.Int32)
	col2 := record.Column(1).(*array.String)
	col3 := record.Column(2).(*array.Float64)

	require.Equal(t, int32(1), col1.Value(0))
	require.Equal(t, "Alice", col2.Value(0))
	require.Equal(t, 10.5, col3.Value(0))
}

func TestQueryArrowWithMultipleRows(t *testing.T) {
	db, arrowInstance, err := setupDuckDBWithArrow()
	require.NoError(t, err)
	defer db.Close()

	ctx := context.Background()
	query := "SELECT id, name, value FROM test_table WHERE id IN (?, ?)"

	record, err := arrowInstance.QueryArrow(ctx, query, 1, 2)
	require.NoError(t, err)
	defer record.Release()

	require.Equal(t, int64(2), record.NumRows())
	require.Equal(t, int64(3), record.NumCols())

	col1 := record.Column(0).(*array.Int32)
	col2 := record.Column(1).(*array.String)
	col3 := record.Column(2).(*array.Float64)

	require.Contains(t, []int32{1, 2}, col1.Value(0))
	require.Contains(t, []int32{1, 2}, col1.Value(1))
	require.Contains(t, []string{"Alice", "Bob"}, col2.Value(0))
	require.Contains(t, []string{"Alice", "Bob"}, col2.Value(1))
	require.Contains(t, []float64{10.5, 20.75}, col3.Value(0))
	require.Contains(t, []float64{10.5, 20.75}, col3.Value(1))
}
