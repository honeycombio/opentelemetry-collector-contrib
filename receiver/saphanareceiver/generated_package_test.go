// Code generated by mdatagen. DO NOT EDIT.

package saphanareceiver

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m, goleak.IgnoreTopFunction("github.com/SAP/go-hdb/driver.(*metrics).collect"))
}
