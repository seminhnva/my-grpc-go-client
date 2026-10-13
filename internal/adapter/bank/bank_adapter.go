package bank

import (
	"context"
	"io"
	"log"

	dbank "github.com/seminhnva/my-grpc-client/internal/application/domain/bank"
	"github.com/seminhnva/my-grpc-client/internal/port"
	"github.com/seminhnva/my-grpc-proto/protogen/go/bank"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc"
	"google.golang.org/grpc/status"
)

type BankAdapter struct {
	bankClient port.BankClienPort
}

func NewBankAdapter(conn *grpc.ClientConn) (*BankAdapter, error) {
	client := bank.NewBankServiceClient(conn)
	return &BankAdapter{
		bankClient: client,
	}, nil
}

func (a *BankAdapter) GetCurrentBalance(ctx context.Context, acct string) (*bank.CurrrentBalanceResponse, error) {
	bankRequest := &bank.CurrentBalanceRequest{
		AccountNumber: acct,
	}

	bal, err := a.bankClient.GetCurrentBalance(ctx, bankRequest)

	if err != nil {
		st, _ := status.FromError(err)
		log.Fatal("[FATAL] Error on GetCurrentBalance: ", st)
	}
	return bal, nil
}

func (a *BankAdapter) FetchExchangeRates(ctx context.Context, fromCur string, toCur string) {
	rateRequest := &bank.ExchangeRateRequest{
		FromCurrency: fromCur,
		ToCurrency:   toCur,
	}
	exchangeRateStream, err := a.bankClient.FetchExchangeRates(ctx, rateRequest)
	if err != nil {
		st, _ := status.FromError(err)
		log.Fatal("[FATAL] Error on FetchExchangeRates: ", st)
	}
	for {
		rate, err := exchangeRateStream.Recv()

		if err == io.EOF {
			break
		}
		if err != nil {
			st := status.Convert(err)
			log.Fatalf("[FATAL] Error on FetchExchangeRates (%s): %s", st.Code(), st.Message())
		}
		timestamp := "unavailable"
		if rate.TransactionTimestamp != nil {
			timestamp = rate.TransactionTimestamp.AsTime().Local().Format("02-01-2006 15:04:05")
		}
		log.Printf("Rate at %s from %s to %s is %v\n", timestamp, rate.FromCurrency, rate.ToCurrency, rate.Rate)
	}
}

func (a *BankAdapter) SummarizeTransactions(ctx context.Context, acct string, tx []dbank.Transaction) {
	txStream, err := a.bankClient.SummarizeTransactions(ctx)
	if err != nil {
		log.Fatal("[FATAL] Error on SummarizeTransactions: ", err)

	}

	for _, t := range tx {
		ttype := bank.TransactionType_TRANSACTION_TYPE_UNSPECIFIED
		switch t.TransactionType {
		case dbank.TransactionTypeIn:
			ttype = bank.TransactionType_TRANSACTION_TYPE_IN
		case dbank.TransactionTypeOut:
			ttype = bank.TransactionType_TRANSACTION_TYPE_OUT
		default:
			ttype = bank.TransactionType_TRANSACTION_TYPE_UNSPECIFIED

		}

		bankRequest := &bank.Transaction{
			AccountNumber: acct,
			Type:          ttype,
			Amount:        t.Amount,
			Notes:         t.Notes,
		}

		txStream.Send(bankRequest)
	}
	summary, err := txStream.CloseAndRecv()

	if err != nil {
		st, _ := status.FromError(err)
		log.Fatal("[FATAL] Error on SummarizeTransactions: ", st)
	}
	log.Println(summary)
}

func (a *BankAdapter) TransferMultiple(ctx context.Context, trf []dbank.TransferTransaction) {
	trfStream, err := a.bankClient.TransferMultiple(ctx)
	if err != nil {
		log.Fatalln("[FATAL] Error on TranserMultiple", err)
	}

	trfChan := make(chan struct{})

	go func() {
		for _, tt := range trf {
			req := &bank.TransferRequest{
				FromAccountNumber: tt.FromAccountNumber,
				ToAccountNumber:   tt.ToAccountNumber,
				Currency:          tt.Currency,
				Amount:            tt.Amount,
			}
			trfStream.Send(req)
		}
		trfStream.CloseSend()
	}()

	go func() {
		for {
			res, err := trfStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				handleTransferErrorGrpc(err)
				break
			} else {
				log.Printf("Tranfer status %v on %v \n", res.Status, res.Timestamp)
			}
		}
		close(trfChan)
	}()
	<-trfChan
}

func handleTransferErrorGrpc(err error) {
	st := status.Convert(err)
	log.Printf("Error %v on TransferMultiple:% v", st.Code(), st.Message())

	for _, detail := range st.Details() {
		switch t := detail.(type) {
		case *errdetails.PreconditionFailure:
			for _, violation := range t.GetViolations() {
				log.Println("[VIOLATION]", violation)
			}
		case *errdetails.ErrorInfo:
			log.Printf("Error on: %v, with reason %v\n", t.Domain, t.Reason)
			for k, v := range t.GetMetadata() {
				log.Printf(" %v : %v \n", k, v)
			}
		}

	}
}
