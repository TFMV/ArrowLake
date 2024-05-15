package pg

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/tmc/langchaingo/chains"
	"github.com/tmc/langchaingo/llms/openai"
)

const (
	dsn = "postgres://username:password@localhost:5432/mydb?sslmode=disable"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func makeSample(ctx context.Context, pool *pgxpool.Pool) {
	conn, err := pool.Acquire(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer conn.Release()

	sqlStmt := `
	CREATE EXTENSION IF NOT EXISTS pgvector;
	CREATE TABLE IF NOT EXISTS foo (id INTEGER NOT NULL PRIMARY KEY, name TEXT, embedding VECTOR(100));
	DELETE FROM foo;
	`
	_, err = conn.Exec(ctx, sqlStmt)
	if err != nil {
		log.Fatal(err)
	}

	tx, err := conn.Begin(ctx)
	if err != nil {
		log.Fatal(err)
	}

	for i := 0; i < 100; i++ {
		vector := make([]float32, 100)

		_, err = tx.Exec(ctx, "INSERT INTO foo (id, name, embedding) VALUES ($1, $2, $3)", i, fmt.Sprintf("Foo %03d", i), vector)
		if err != nil {
			log.Fatal(err)
		}
	}

	err = tx.Commit(ctx)
	if err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx := context.Background()

	llm, err := openai.New()
	if err != nil {
		return err
	}

	config, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		return err
	}

	pool, err := pgxpool.NewWithConfig(ctx, config)
	if err != nil {
		return err
	}
	defer pool.Close()

	makeSample(ctx, pool)

	sqlDatabaseChain := chains.NewSQLDatabaseChain(llm, 100, pool)
	out, err := chains.Run(ctx, sqlDatabaseChain, "Your query here")
	if err != nil {
		return err
	}
	fmt.Println(out)

	return nil
}
