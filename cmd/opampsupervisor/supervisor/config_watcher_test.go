// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package supervisor

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap"

	"github.com/open-telemetry/opentelemetry-collector-contrib/cmd/opampsupervisor/supervisor/config"
)

func TestConfigWatcher_DetectsChanges(t *testing.T) {
	// Create temporary config file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "supervisor.yaml")

	initialConfig := `
server:
  endpoint: ws://localhost:4320/v1/opamp
agent:
  executable: /usr/bin/true
storage:
  directory: /tmp/opamp
`
	err := os.WriteFile(configFile, []byte(initialConfig), 0600)
	require.NoError(t, err)

	// Wait to ensure file system has settled
	time.Sleep(100 * time.Millisecond)

	// Track onChange calls
	changeCount := 0
	changeChan := make(chan struct{}, 1)
	onChange := func(cfg config.Supervisor) error {
		changeCount++
		select {
		case changeChan <- struct{}{}:
		default:
		}
		return nil
	}

	// Create and start watcher with a logger for debugging
	logger, _ := zap.NewDevelopment()
	watcher, err := newConfigWatcher(configFile, logger, onChange)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watcher.Start(ctx)

	// Wait for watcher to start polling
	time.Sleep(1500 * time.Millisecond)

	// Modify config file
	updatedConfig := `
server:
  endpoint: ws://localhost:4321/v1/opamp
agent:
  executable: /usr/bin/true
storage:
  directory: /tmp/opamp
`
	err = os.WriteFile(configFile, []byte(updatedConfig), 0600)
	require.NoError(t, err)

	// Wait for change detection + grace period
	select {
	case <-changeChan:
		// Success - change detected
	case <-time.After(5 * time.Second):
		t.Fatal("Config change not detected within timeout")
	}

	assert.Equal(t, 1, changeCount, "onChange should be called exactly once")
}

func TestConfigWatcher_GracePeriod(t *testing.T) {
	// This test verifies that rapid file changes are debounced with a grace period
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "supervisor.yaml")

	initialConfig := `
server:
  endpoint: ws://localhost:4320/v1/opamp
agent:
  executable: /usr/bin/true
storage:
  directory: /tmp/opamp
`
	err := os.WriteFile(configFile, []byte(initialConfig), 0600)
	require.NoError(t, err)

	// Wait to ensure file system has settled
	time.Sleep(100 * time.Millisecond)

	// Track onChange calls - should only be called once despite multiple changes
	callCount := 0
	onChange := func(cfg config.Supervisor) error {
		callCount++
		return nil
	}

	logger := zap.NewNop()
	watcher, err := newConfigWatcher(configFile, logger, onChange)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watcher.Start(ctx)
	// Wait for watcher to start polling
	time.Sleep(1500 * time.Millisecond)

	// Make multiple rapid changes
	for i := 1; i <= 3; i++ {
		updatedConfig := initialConfig + fmt.Sprintf("# change %d\n", i)
		err = os.WriteFile(configFile, []byte(updatedConfig), 0600)
		require.NoError(t, err)
		time.Sleep(300 * time.Millisecond)
	}

	// Wait for grace period to fully expire
	time.Sleep(3 * time.Second)

	// The onChange should be called exactly once despite multiple file changes
	// This verifies the debouncing/grace period logic
	assert.Equal(t, 1, callCount, "onChange should be called exactly once despite multiple rapid changes")
}

func TestConfigWatcher_InvalidConfig(t *testing.T) {
	// Create temporary config file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "supervisor.yaml")

	initialConfig := `
server:
  endpoint: ws://localhost:4320/v1/opamp
agent:
  executable: /usr/bin/true
storage:
  directory: /tmp/opamp
`
	err := os.WriteFile(configFile, []byte(initialConfig), 0600)
	require.NoError(t, err)

	// Track onChange calls
	changeCount := 0
	onChange := func(cfg config.Supervisor) error {
		changeCount++
		return nil
	}

	logger := zap.NewNop()
	watcher, err := newConfigWatcher(configFile, logger, onChange)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go watcher.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Write invalid config (should not trigger onChange due to load error)
	invalidConfig := `
this is not valid yaml: {
`
	err = os.WriteFile(configFile, []byte(invalidConfig), 0600)
	require.NoError(t, err)

	// Wait to see if onChange is called (it shouldn't be)
	time.Sleep(4 * time.Second)

	assert.Equal(t, 0, changeCount, "onChange should not be called for invalid config")
}

func TestConfigWatcher_Stop(t *testing.T) {
	// Create temporary config file
	tmpDir := t.TempDir()
	configFile := filepath.Join(tmpDir, "supervisor.yaml")

	initialConfig := `
server:
  endpoint: ws://localhost:4320/v1/opamp
agent:
  executable: /usr/bin/true
storage:
  directory: /tmp/opamp
`
	err := os.WriteFile(configFile, []byte(initialConfig), 0600)
	require.NoError(t, err)

	onChange := func(cfg config.Supervisor) error {
		return nil
	}

	logger := zap.NewNop()
	watcher, err := newConfigWatcher(configFile, logger, onChange)
	require.NoError(t, err)

	ctx, cancel := context.WithCancel(context.Background())

	go watcher.Start(ctx)
	time.Sleep(100 * time.Millisecond)

	// Stop the watcher
	cancel()
	time.Sleep(100 * time.Millisecond)

	// Stop should not error
	err = watcher.Stop()
	assert.NoError(t, err)
}

func TestConfigWatcher_NonExistentFile(t *testing.T) {
	tmpDir := t.TempDir()
	nonExistentFile := filepath.Join(tmpDir, "does_not_exist.yaml")

	onChange := func(cfg config.Supervisor) error {
		return nil
	}

	logger := zap.NewNop()
	_, err := newConfigWatcher(nonExistentFile, logger, onChange)
	assert.Error(t, err, "Should error when file does not exist")
}
