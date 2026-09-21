package main

import (
	"context"
	"database/sql"
	"fmt"
	_ "github.com/jackc/pgx/v5/stdlib"
	"github.com/pressly/goose/v3"
	"os"
	"os/signal"
	"syscall"
	"time"
)

func run() error {
	if len(os.Args) != 2 {
		return fmt.Errorf("usage: migrate up|down|status")
	}
	if os.Args[1] != "up" && os.Args[1] != "down" && os.Args[1] != "status" {
		return fmt.Errorf("unsupported migration command")
	}
	address := os.Getenv("DATABASE_URL")
	if address == "" {
		return fmt.Errorf("DATABASE_URL is required")
	}
	dir := os.Getenv("MIGRATIONS_DIR")
	if dir == "" {
		dir = "migrations"
	}
	db, err := sql.Open("pgx", address)
	if err != nil {
		return err
	}
	defer db.Close()
	db.SetMaxOpenConns(1)
	ctx, cancel := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer cancel()
	ctx, timeout := context.WithTimeout(ctx, 2*time.Minute)
	defer timeout()
	if err = goose.SetDialect("postgres"); err != nil {
		return err
	}
	return goose.RunContext(ctx, os.Args[1], db, dir)
}
func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}
