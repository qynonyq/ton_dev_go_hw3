package app

import (
	"fmt"

	"gorm.io/driver/postgres"
	"gorm.io/gorm"
)

var DB *gorm.DB

func initDatabase(cfg Postgres) error {
	dsn := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s TimeZone=%s",
		cfg.Host, cfg.Port, cfg.User, cfg.Password, cfg.DbName, cfg.SslMode, cfg.Timezone)
	var err error

	DB, err = gorm.Open(postgres.Open(dsn))
	if err != nil {
		return err
	}

	return nil
}
