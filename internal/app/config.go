package app

import (
	"os"
	"strings"

	"github.com/joho/godotenv"
	"github.com/xssnick/tonutils-go/liteclient"
)

const (
	MainnetCfgURL = "https://ton-blockchain.github.io/global.config.json"
	TestnetCfgURL = "https://ton-blockchain.github.io/testnet-global.config.json"
)

type (
	Cfg struct {
		LogLevel  string
		Postgres  Postgres
		NetConfig *liteclient.GlobalConfig
		Wallet    Wallet
	}

	Wallet struct {
		Seed []string
	}

	Postgres struct {
		Host     string
		Port     string
		User     string
		Password string
		DbName   string
		SslMode  string
		Timezone string
	}
)

func initConfig() (*Cfg, error) {
	if err := godotenv.Load(); err != nil {
		return nil, err
	}

	cfg := Cfg{
		LogLevel: os.Getenv("LOG_LEVEL"),
		Wallet: Wallet{
			Seed: strings.Split(os.Getenv("SEED"), " "),
		},
		Postgres: Postgres{
			Host:     os.Getenv("POSTGRES_HOST"),
			Port:     os.Getenv("POSTGRES_PORT"),
			User:     os.Getenv("POSTGRES_USER"),
			Password: os.Getenv("POSTGRES_PASSWORD"),
			DbName:   os.Getenv("POSTGRES_DB_NAME"),
			SslMode:  os.Getenv("POSTGRES_SSLMODE"),
			Timezone: os.Getenv("POSTGRES_TIMEZONE"),
		},
	}

	return &cfg, nil
}
