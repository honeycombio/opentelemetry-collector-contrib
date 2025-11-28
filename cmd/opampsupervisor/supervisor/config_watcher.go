// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"os"
	"time"

	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

// configWatcher polls a configuration file for changes and triggers reloads.
// This implementation follows the OpenTelemetry Collector's polling approach
// rather than using OS notifications (fsnotify), which are unreliable for
// Kubernetes ConfigMaps and other virtual filesystems.
type configWatcher struct {
	configFilePath string
	logger         *zap.Logger
	onChange       func(config.Supervisor) error

	// File state tracking
	lastModTime time.Time
	lastSize    int64
}

const (
	// pollInterval is how often we check the file for changes (matching collector)
	pollInterval = 1 * time.Second

	// gracePeriod is the delay before triggering a reload after detecting a change
	// This allows multiple rapid writes to complete before reloading (matching collector)
	gracePeriod = 2 * time.Second
)

// newConfigWatcher creates a new polling-based configuration watcher
func newConfigWatcher(configFilePath string, logger *zap.Logger, onChange func(config.Supervisor) error) (*configWatcher, error) {
	// Get initial file state
	info, err := os.Stat(configFilePath)
	if err != nil {
		return nil, err
	}

	return &configWatcher{
		configFilePath: configFilePath,
		logger:         logger,
		onChange:       onChange,
		lastModTime:    info.ModTime(),
		lastSize:       info.Size(),
	}, nil
}

// Start begins polling the configuration file for changes
func (w *configWatcher) Start(ctx context.Context) {
	ticker := time.NewTicker(pollInterval)
	defer ticker.Stop()

	// Timer for grace period after detecting a change
	// Initialize with a stopped timer to avoid nil channel
	graceTimer := time.NewTimer(0)
	graceTimer.Stop()
	changeDetected := false

	w.logger.Debug("Config file watcher started",
		zap.String("file", w.configFilePath),
		zap.Duration("poll_interval", pollInterval),
		zap.Duration("grace_period", gracePeriod))

	for {
		select {
		case <-ticker.C:
			if w.hasFileChanged() && !changeDetected {
				w.logger.Debug("Config file change detected, starting grace period",
					zap.String("file", w.configFilePath))

				changeDetected = true

				// Stop and drain the timer before resetting it
				if !graceTimer.Stop() {
					select {
					case <-graceTimer.C:
					default:
					}
				}
				graceTimer.Reset(gracePeriod)
			}

		case <-graceTimer.C:
			if changeDetected {
				w.logger.Info("Grace period elapsed, triggering config reload")
				w.handleConfigChange()
				changeDetected = false
			}

		case <-ctx.Done():
			if !graceTimer.Stop() {
				select {
				case <-graceTimer.C:
				default:
				}
			}
			w.logger.Debug("Config file watcher stopped")
			return
		}
	}
}

// hasFileChanged checks if the file has been modified since last check
func (w *configWatcher) hasFileChanged() bool {
	info, err := os.Stat(w.configFilePath)
	if err != nil {
		w.logger.Error("Failed to stat config file", zap.Error(err))
		return false
	}

	modTime := info.ModTime()
	size := info.Size()

	// Check if either modification time or size changed
	changed := !modTime.Equal(w.lastModTime) || size != w.lastSize

	if changed {
		w.logger.Debug("File metadata changed",
			zap.String("file", w.configFilePath),
			zap.Time("old_mod_time", w.lastModTime),
			zap.Time("new_mod_time", modTime),
			zap.Int64("old_size", w.lastSize),
			zap.Int64("new_size", size))

		// Update tracked state
		w.lastModTime = modTime
		w.lastSize = size
	}

	return changed
}

// handleConfigChange loads and applies the new configuration
func (w *configWatcher) handleConfigChange() {
	w.logger.Info("Reloading configuration from file", zap.String("file", w.configFilePath))

	newCfg, err := config.Load(w.configFilePath)
	if err != nil {
		w.logger.Error("Failed to load new config", zap.Error(err))
		return
	}

	if err := w.onChange(newCfg); err != nil {
		w.logger.Error("Failed to apply new config", zap.Error(err))
	} else {
		w.logger.Info("Configuration reloaded successfully")
	}
}

// Stop stops the config watcher (nothing to clean up in polling implementation)
func (w *configWatcher) Stop() error {
	// No resources to clean up with polling approach
	return nil
}
