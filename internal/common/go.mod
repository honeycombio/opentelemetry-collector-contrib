module github.com/open-telemetry/opentelemetry-collector-contrib/internal/common

go 1.18

require (
	github.com/stretchr/testify v1.8.1
	go.opentelemetry.io/collector/featuregate v0.66.1-0.20221128222955-4ff1ff379b90
	go.uber.org/zap v1.25.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/multierr v1.10.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

retract v0.65.0
