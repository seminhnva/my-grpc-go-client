package port

import (
	"context"

	"github.com/seminhnva/my-grpc-proto/protogen/go/bank"
	"google.golang.org/grpc"
)

type BankClienPort interface {
	GetCurrentBalance(ctx context.Context, req *bank.CurrentBalanceRequest, opts ...grpc.CallOption) (*bank.CurrrentBalanceResponse, error)
	FetchExchangeRates(ctx context.Context, req *bank.ExchangeRateRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[bank.ExchangeRateResponse], error)
	SummarizeTransactions(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[bank.Transaction, bank.TransactionSummary], error)
	TransferMultiple(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[bank.TransferRequest, bank.TransferResponse], error)
	CreateAccount(ctx context.Context, req *bank.CreateAccountRequest, opts ...grpc.CallOption) (*bank.CreateAccountResponse, error)
}
