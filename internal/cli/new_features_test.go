package cli

import (
	"encoding/json"
	"strings"
	"testing"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
)

func TestOperationsCommandNDJSON(t *testing.T) {
	envelope := cliUploadEnvelope(t, []byte{0, 97, 115, 109})
	srv := fakerpc.NewTransactionServer([]protocol.TransactionInfo{
		cliTransaction(9900, "tx", protocol.TransactionStatusSuccess, envelope, ""),
	})
	withFake(t, srv)

	stdout, _, err := run(t, "operations", "--from", "9900", "--type", "invoke_host_function", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	lines := nonEmptyLines(stdout)
	if len(lines) != 1 {
		t.Fatalf("got %d operation lines, want 1\n%s", len(lines), stdout)
	}
	var record struct {
		Kind       string          `json:"kind"`
		ID         string          `json:"id"`
		Type       string          `json:"type"`
		Parameters json.RawMessage `json:"parameters"`
		ParamsXDR  string          `json:"parametersXdr"`
	}
	if err := json.Unmarshal([]byte(lines[0]), &record); err != nil {
		t.Fatalf("invalid operation JSON: %v", err)
	}
	if record.Kind != "operation" || record.Type != "invoke_host_function" || record.ID == "" || len(record.Parameters) == 0 && record.ParamsXDR == "" {
		t.Errorf("operation record = %+v, want demuxed metadata", record)
	}
}

func TestSimulateCommandNDJSON(t *testing.T) {
	srv := fakerpc.NewLedgerServer(fakerpc.MakeLedgers(9950, 1))
	srv.SetSimulations([]protocol.SimulateTransactionResponse{{MinResourceFee: 17}})
	withFake(t, srv)

	stdout, _, err := run(t, "simulate", "--tx-envelope", "AAA=", "--json")
	if err != nil {
		t.Fatalf("Execute() error = %v", err)
	}
	if !strings.Contains(stdout, `"kind":"simulation"`) || !strings.Contains(stdout, `"minResourceFee":17`) {
		t.Errorf("simulation output = %s", stdout)
	}
}

func TestSimulateCommandRequiresEnvelope(t *testing.T) {
	if _, _, err := run(t, "simulate"); err == nil || !strings.Contains(err.Error(), "--tx-envelope") {
		t.Fatalf("Execute() error = %v, want missing envelope validation", err)
	}
}
