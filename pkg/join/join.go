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

package join

import (
	"context"
	"database/sql"
	"fmt"
	"strings"

	"os"

	_ "github.com/marcboeker/go-duckdb"
	"gopkg.in/yaml.v2"
)

type DataSource struct {
	Type             string `yaml:"type"`
	TableName        string `yaml:"table_name"`
	FilePath         string `yaml:"file_path,omitempty"`
	ConnectionString string `yaml:"connection_string,omitempty"`
}

type QueryConfig struct {
	JoinColumns   []JoinColumn `yaml:"join_columns"`
	SelectColumns []string     `yaml:"select_columns"`
	SQL           string       `yaml:"sql"`
}

type JoinColumn struct {
	Source string `yaml:"source"`
	Column string `yaml:"column"`
}

type Config struct {
	Sources []DataSource `yaml:"sources"`
	Query   QueryConfig  `yaml:"query"`
}

func LoadConfig(configPath string) (*Config, error) {
	data, err := os.ReadFile(configPath)
	if err != nil {
		return nil, fmt.Errorf("unable to read config file: %v", err)
	}

	var config Config
	err = yaml.Unmarshal(data, &config)
	if err != nil {
		return nil, fmt.Errorf("unable to unmarshal config file: %v", err)
	}

	return &config, nil
}

func JoinDataSources(ctx context.Context, config *Config) error {
	db, err := sql.Open("duckdb", "")
	if err != nil {
		return fmt.Errorf("failed to open DuckDB: %w", err)
	}
	defer db.Close()

	for _, source := range config.Sources {
		switch source.Type {
		case "parquet":
			_, err = db.Exec(fmt.Sprintf(`CREATE TABLE %s AS SELECT * FROM read_parquet('%s')`, source.TableName, source.FilePath))
			if err != nil {
				return fmt.Errorf("failed to create Parquet table: %w", err)
			}
		case "postgres":
			_, err = db.Exec(`INSTALL postgres; LOAD postgres;`)
			if err != nil {
				return fmt.Errorf("failed to install and load PostgreSQL extension: %w", err)
			}

			attachCmd := fmt.Sprintf(`ATTACH '%s' AS %s (TYPE POSTGRES);`, source.ConnectionString, source.TableName)
			_, err = db.Exec(attachCmd)
			if err != nil {
				return fmt.Errorf("failed to attach PostgreSQL database: %w", err)
			}
		}
	}

	joinColumns := make([]string, len(config.Query.JoinColumns))
	for i, col := range config.Query.JoinColumns {
		joinColumns[i] = fmt.Sprintf("%s.%s", col.Source, col.Column)
	}

	query := config.Query.SQL
	query = strings.Replace(query, "{select_columns}", strings.Join(config.Query.SelectColumns, ", "), -1)
	for _, col := range config.Query.JoinColumns {
		placeholder := fmt.Sprintf("{%s.%s}", col.Source, col.Column)
		query = strings.Replace(query, placeholder, fmt.Sprintf("%s.%s", col.Source, col.Column), -1)
	}

	rows, err := db.Query(query)
	if err != nil {
		return fmt.Errorf("failed to execute join query: %w", err)
	}
	defer rows.Close()

	// Process the results
	var count int
	if rows.Next() {
		err := rows.Scan(&count)
		if err != nil {
			return fmt.Errorf("failed to scan row: %w", err)
		}
	}

	fmt.Printf("Count of rows: %d\n", count)

	return nil
}
