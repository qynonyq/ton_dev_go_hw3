package app

import (
	"fmt"
	"os"
	"path/filepath"
	"runtime"

	"github.com/sirupsen/logrus"
)

func initLogger(cfgLevel string) error {
	logrus.SetReportCaller(true)
	lvl, err := logrus.ParseLevel(cfgLevel)
	if err != nil {
		return err
	}

	formatter := &logrus.TextFormatter{
		TimestampFormat:        "02-01-2006 15:04:05",
		FullTimestamp:          true,
		DisableLevelTruncation: true,
		ForceColors:            true,
		CallerPrettyfier: func(frame *runtime.Frame) (function string, file string) {
			return "", fmt.Sprintf(" %s:%d", filepath.Base(frame.File), frame.Line)
		},
	}

	logrus.SetLevel(lvl)
	logrus.SetOutput(os.Stdout)
	logrus.SetFormatter(formatter)

	return nil
}
