// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/telemetry"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func runInteractive() error {
	configFlag := flag.String("config", "", "Path to a supervisor configuration file")
	flag.Parse()

	cfg, err := config.Load(*configFlag)
	if err != nil {
		return fmt.Errorf("failed to load config: %w", err)
	}

	logger, err := telemetry.NewLogger(cfg.Telemetry.Logs)
	if err != nil {
		return fmt.Errorf("failed to create logger: %w", err)
	}

	supervisorWrapper, err := supervisor.NewSupervisorWrapper(logger.Named("supervisor"), cfg)
	if err != nil {
		return fmt.Errorf("failed to create supervisor: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())

	err = supervisorWrapper.Run(ctx)
	if err != nil {
		return fmt.Errorf("failed to start supervisor: %w", err)
	}

	interrupt := make(chan os.Signal, 1)
	signal.Notify(interrupt, os.Interrupt)
	<-interrupt
	cancel()
	err = supervisorWrapper.Shutdown(ctx)
	if err != nil {
		return fmt.Errorf("failed to shutdown supervisor gracefully: %w", err)
	}

	return nil
}
