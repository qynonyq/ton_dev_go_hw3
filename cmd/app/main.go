package main

import (
	"context"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/sirupsen/logrus"

	"github.com/qynonyq/ton_dev_go_hw3/internal/app"
	"github.com/qynonyq/ton_dev_go_hw3/internal/scanner"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	a, err := app.InitApp()
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sc, err := scanner.NewScanner(ctx, a.Cfg.NetConfig)
	if err != nil {
		return err
	}
	go sc.Listen(ctx)

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	sig := <-sigCh
	logrus.Infof("received %q, shutting down gracefully", sig)

	stopped := make(chan struct{})
	go func() {
		sc.Stop()
		cancel()
		stopped <- struct{}{}
	}()

	select {
	case <-time.After(5 * time.Second):
		logrus.Info("shutdown timeout expired, scanner stopped")
	case <-stopped:
		logrus.Info("scanner gracefully stopped")
	}

	return nil
}
