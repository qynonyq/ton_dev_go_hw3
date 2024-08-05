package main

import (
	"log"

	"github.com/qynonyq/ton_dev_go_hw3/internal/app"
	"github.com/qynonyq/ton_dev_go_hw3/internal/storage"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	if _, err := app.InitApp(); err != nil {
		return err
	}

	dbTx := app.DB.Begin()
	if err := dbTx.AutoMigrate(&storage.Block{}); err != nil {
		dbTx.Rollback()
		return err
	}
	if err := dbTx.Commit().Error; err != nil {
		return err
	}

	return nil
}
