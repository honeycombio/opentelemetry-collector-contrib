// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"fmt"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

type Wrapper struct {
	supervisor *Supervisor
	config     config.Supervisor
	logger     *zap.Logger
}

func NewSupervisorWrapper(logger *zap.Logger, cfg config.Supervisor) (*Wrapper, error) {
	wrapper := &Wrapper{
		config: cfg,
		logger: logger,
	}

	return wrapper, nil
}

func (w *Wrapper) Run(ctx context.Context) error {
	err := w.startSupervisor(ctx)
	if err != nil {
		return err
	}

	for {
		select {
		case newTelemetryConfig := <-w.supervisor.Watch():
			err := w.reloadSupervisorWithTelemetry(ctx, newTelemetryConfig)
			if err != nil {
				return err
			}
		case <-ctx.Done():
			w.logger.Info("Context done, terminating watch", zap.Error(ctx.Err()))
			return nil
		}
	}
}

func (w *Wrapper) startSupervisor(ctx context.Context) error {
	supervisor, err := NewSupervisor(ctx, w.logger, w.config)
	if err != nil {
		return fmt.Errorf("failed to create supervisor: %w", err)
	}

	w.supervisor = supervisor

	if err := w.supervisor.Start(ctx); err != nil {
		return fmt.Errorf("failed to start supervisor: %w", err)
	}
	return nil
}

func (w *Wrapper) reloadSupervisorWithTelemetry(ctx context.Context, newConfig config.Supervisor) error {
	w.logger.Warn("Telemetry config updated, restart supervisor")
	w.config = newConfig
	w.supervisor.Shutdown()
	return w.startSupervisor(ctx)
}

// Shutdown gracefully shuts down the supervisor wrapper.
func (w *Wrapper) Shutdown(_ context.Context) error {
	// Shutdown the current supervisor
	if w.supervisor != nil {
		w.supervisor.Shutdown()
	}

	return nil
}
