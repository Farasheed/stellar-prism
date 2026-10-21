package stream

import (
	"testing"
	"time"

	"github.com/stellar-optics/stellar-prism/internal/fakerpc"
	"github.com/stellar/go-stellar-sdk/xdr"
)

func TestExtractPathPaymentTradeUsesExecutedAmounts(t *testing.T) {
	native := xdr.Asset{Type: xdr.AssetTypeAssetTypeNative}
	destination := xdr.MuxedAccount{Type: xdr.CryptoKeyTypeKeyTypeEd25519, Ed25519: &xdr.Uint256{}}
	operation := xdr.Operation{Body: xdr.OperationBody{Type: xdr.OperationTypePathPaymentStrictSend, PathPaymentStrictSendOp: &xdr.PathPaymentStrictSendOp{SendAsset: native, SendAmount: 20_000_000, Destination: destination, DestAsset: native, DestMin: 19_000_000}}}
	envelope := xdr.TransactionEnvelope{Type: xdr.EnvelopeTypeEnvelopeTypeTx, V1: &xdr.TransactionV1Envelope{Tx: xdr.Transaction{SourceAccount: destination, Operations: []xdr.Operation{operation}}}}
	envelopeXDR, err := xdr.MarshalBase64(&envelope)
	if err != nil {
		t.Fatal(err)
	}
	resultAccount := xdr.AccountId(xdr.PublicKey{Type: xdr.PublicKeyTypePublicKeyTypeEd25519, Ed25519: &xdr.Uint256{}})
	pathResult := xdr.PathPaymentStrictSendResult{Code: xdr.PathPaymentStrictSendResultCodePathPaymentStrictSendSuccess, Success: &xdr.PathPaymentStrictSendResultSuccess{Last: xdr.SimplePaymentResult{Destination: resultAccount, Asset: native, Amount: 19_500_000}}}
	operationResult := xdr.OperationResultTr{Type: xdr.OperationTypePathPaymentStrictSend, PathPaymentStrictSendResult: &pathResult}
	resultEntry, err := xdr.NewOperationResult(xdr.OperationResultCodeOpInner, operationResult)
	if err != nil {
		t.Fatal(err)
	}
	resultUnion, err := xdr.NewTransactionResultResult(xdr.TransactionResultCodeTxSuccess, []xdr.OperationResult{resultEntry})
	if err != nil {
		t.Fatal(err)
	}
	resultXDR, err := xdr.MarshalBase64(&xdr.TransactionResult{Result: resultUnion})
	if err != nil {
		t.Fatal(err)
	}
	got, err := extractPathPaymentTrades(&Transaction{Hash: "abc", Index: 2, Successful: true, EnvelopeXDR: envelopeXDR, ResultXDR: resultXDR}, 42, time.Unix(1, 0).UTC())
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 {
		t.Fatalf("trades = %d, want 1", len(got))
	}
	trade := got[0].Trade
	if trade.Protocol != "classic" || trade.AssetIn != "XLM" || trade.AssetOut != "XLM" || trade.AmountIn != "2.0000000" || trade.AmountOut != "1.9500000" || trade.Price != "0.97500000" {
		t.Fatalf("trade = %+v", trade)
	}
}

func TestTradeSourcePairValidation(t *testing.T) {
	if _, err := NewTradeSource(nil, TradeQuery{}); err == nil {
		t.Fatal("nil client accepted")
	}
	if _, err := NewTradeSource(fakerpc.NewTransactionServer(nil), TradeQuery{Pair: "XLM"}); err == nil {
		t.Fatal("malformed pair accepted")
	}
}
