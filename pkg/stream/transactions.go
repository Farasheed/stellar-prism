package stream

import (
	"context"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"time"

	protocol "github.com/stellar/go-stellar-sdk/protocols/rpc"
	"github.com/stellar/go-stellar-sdk/xdr"

	"github.com/stellar-optics/stellar-prism/pkg/rpc"
)

// DefaultTransactionPageLimit is the page size requested for transaction streams.
const DefaultTransactionPageLimit = 100

// TransactionQuery describes which transactions to stream.
type TransactionQuery struct {
	// StartLedger is where a fresh stream begins. Zero means start from the
	// server's newest ledger.
	StartLedger uint32
	// PageLimit is the page size to request.
	PageLimit uint
}

// TransactionSource streams transactions from RPC.
type TransactionSource struct {
	client rpc.Client
	query  TransactionQuery
}

// NewTransactionSource returns a Source over the given RPC client.
func NewTransactionSource(client rpc.Client, q TransactionQuery) *TransactionSource {
	if q.PageLimit == 0 {
		q.PageLimit = DefaultTransactionPageLimit
	}
	return &TransactionSource{client: client, query: q}
}

// Name implements Source.
func (s *TransactionSource) Name() string { return "transactions" }

// Rewind implements Rewinder, repositioning a fresh fetch at ledger.
func (s *TransactionSource) Rewind(ledger uint32) { s.query.StartLedger = ledger }

// CursorLedger implements Source.
func (s *TransactionSource) CursorLedger(cursor string) (uint32, bool) {
	if cursor == "" {
		return 0, false
	}
	// The transaction cursor is just a decimal ledger sequence in some RPC versions,
	// but according to protocol it might be identical to LedgerPaginationOptions cursor.
	// We'll parse it out. Since the events cursor is a compound string like "12345-1",
	// let's try to parse a ledger from it. If it fails, fallback to simple decimal.
	var ledger uint32
	if _, err := fmt.Sscanf(cursor, "%d-", &ledger); err == nil {
		return ledger, true
	}
	n, err := strconv.ParseUint(cursor, 10, 32)
	if err != nil {
		return 0, false
	}
	return uint32(n), true
}

// Fetch implements Source.
func (s *TransactionSource) Fetch(ctx context.Context, cursor string) (Batch, error) {
	req := protocol.GetTransactionsRequest{
		Pagination: &protocol.LedgerPaginationOptions{
			Limit: s.query.PageLimit,
		},
	}
	if cursor == "" {
		req.StartLedger = s.query.StartLedger
	} else {
		req.Pagination.Cursor = cursor
	}

	resp, err := s.client.GetTransactions(ctx, req)
	if err != nil {
		return Batch{}, err
	}

	batch := Batch{
		Records:    make([]Record, 0, len(resp.Transactions)),
		NextCursor: resp.Cursor,
		Latest:     resp.LatestLedger,
		Oldest:     resp.OldestLedger,
		Full:       uint(len(resp.Transactions)) >= s.query.PageLimit,
	}
	for _, tx := range resp.Transactions {
		batch.Records = append(batch.Records, transactionRecord(tx))
	}
	return batch, nil
}

// transactionRecord converts an RPC transaction into a neutral Record.
func transactionRecord(tx protocol.TransactionInfo) Record {
	transaction := &Transaction{
		Hash:          tx.TransactionHash,
		Index:         uint32(tx.ApplicationOrder),
		Successful:    tx.Status == protocol.TransactionStatusSuccess,
		EnvelopeXDR:   tx.EnvelopeXDR,
		ResultXDR:     tx.ResultXDR,
		ResultMetaXDR: tx.ResultMetaXDR,
	}
	if !transaction.Successful {
		transaction.ErrorCode, transaction.FailureReason, transaction.FailedOpIndex, transaction.FailedOpType = failureSummary(tx.ResultXDR)
	}
	return Record{
		ID:          fmt.Sprintf("%010d-%04d", tx.Ledger, tx.ApplicationOrder),
		Sequence:    tx.Ledger,
		Time:        time.Unix(tx.LedgerCloseTime, 0).UTC(),
		Kind:        KindTransaction,
		Transaction: transaction,
	}
}

// failureSummary extracts the failed transaction or operation code without
// making malformed optional result XDR fatal to the stream.
func failureSummary(resultXDR string) (code, detail string, opIndex *uint32, opType string) {
	if resultXDR == "" {
		return
	}
	var result xdr.TransactionResult
	if err := xdr.SafeUnmarshalBase64(resultXDR, &result); err != nil {
		return
	}
	resultCode := normalizedCode(result.Result.Code.String())
	var operationResults *[]xdr.OperationResult
	if result.Result.Results != nil {
		operationResults = result.Result.Results
	}
	if result.Result.Code == xdr.TransactionResultCodeTxFeeBumpInnerFailed && result.Result.InnerResultPair != nil {
		inner := result.Result.InnerResultPair.Result
		resultCode = normalizedCode(inner.Result.Code.String())
		operationResults = inner.Result.Results
	}
	code, detail = resultCode, humanizeCode(resultCode)
	if operationResults == nil {
		return
	}
	for i, operation := range *operationResults {
		if operation.Code != xdr.OperationResultCodeOpInner || operation.Tr == nil {
			if operation.Code != xdr.OperationResultCodeOpInner {
				n := uint32(i)
				opIndex = &n
				code = normalizedCode(operation.Code.String())
				detail = humanizeCode(code)
			}
			if opIndex != nil {
				return
			}
			continue
		}
		tr := operation.Tr
		raw := operationErrorCode(tr)
		if raw != "" && !strings.HasSuffix(raw, "Success") {
			n := uint32(i)
			opIndex = &n
			opType = strings.TrimPrefix(strings.ToLower(tr.Type.String()), "operationtype")
			code = normalizedCode(raw)
			detail = humanizeCode(code)
			return
		}
	}
	return
}

func operationErrorCode(tr *xdr.OperationResultTr) string {
	if tr == nil {
		return ""
	}
	name, ok := tr.ArmForSwitch(int32(tr.Type))
	if !ok || name == "" {
		return ""
	}
	value := reflect.ValueOf(tr).Elem().FieldByName(name)
	if !value.IsValid() || value.IsNil() {
		return ""
	}
	code := value.Elem().FieldByName("Code")
	if !code.IsValid() || !code.CanInterface() {
		return ""
	}
	stringer, ok := code.Interface().(fmt.Stringer)
	if !ok {
		return ""
	}
	return stringer.String()
}

func normalizedCode(raw string) string {
	prefix := ""
	if i := strings.Index(raw, "ResultCode"); i >= 0 {
		enumName := raw[:i]
		raw = raw[i+len("ResultCode"):]
		switch enumName {
		case "Transaction":
			prefix = "tx_"
		case "Operation":
			prefix = "op_"
		default:
			prefix = camelSnake(enumName) + "_"
		}
		raw = strings.TrimPrefix(raw, enumName)
		if enumName == "Transaction" {
			raw = strings.TrimPrefix(raw, "Tx")
		}
		if enumName == "Operation" {
			raw = strings.TrimPrefix(raw, "Op")
		}
	}
	return prefix + camelSnake(raw)
}

func camelSnake(raw string) string {
	var b strings.Builder
	for i, r := range raw {
		if i > 0 && r >= 'A' && r <= 'Z' {
			b.WriteByte('_')
		}
		b.WriteRune(r)
	}
	return strings.ToLower(b.String())
}

func humanizeCode(code string) string { return strings.ReplaceAll(code, "_", " ") }
