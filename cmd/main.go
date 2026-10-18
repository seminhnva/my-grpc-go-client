package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/seminhnva/my-grpc-client/internal/adapter/bank"
	resiliencty "github.com/seminhnva/my-grpc-client/internal/adapter/resiliency"
	dbank "github.com/seminhnva/my-grpc-client/internal/application/domain/bank"
	resl "github.com/seminhnva/my-grpc-client/internal/application/domain/resiliency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {
	log.SetFlags(0)
	log.SetOutput(log.Writer())

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	conn, err := grpc.NewClient("localhost:9090", opts...)
	if err != nil {
		log.Fatalf("cant connect grpc Server :%v", err)
	}
	defer conn.Close()

	// helloAdapter := hello.NewHelloAdapter(conn)
	// bankAdapter, err := bank.NewBankAdapter(conn)

	// if err != nil {
	// 	log.Fatalf("Can not create bank adapter %v", err)
	// }

	// runGetCurrentBalance(bankAdapter, "7835697001")
	// runFetchExchangeRate(bankAdapter, "USD", "VND")
	// runSumarizeTransaction(bankAdapter, "783597001", 10)
	// runTransferMultiple(bankAdapter, "7835697001", "7835697002", 320)

	resiliencyAdapter, err := resiliencty.NewResiliencyAdapter(conn)

	if err != nil {
		log.Fatalln("Cant not create ResiliencyAdapter:", err)
	}

	// runUnaryResiliencyWithTimeout(resiliencyAdapter, 2, 8, []uint32{resl.OK}, 5*time.Second)
	// runServerStreamResiliency(resiliencyAdapter, 0, 3, []uint32{resl.OK}, 15*time.Second)
	// runClientStreamResiliency(resiliencyAdapter, 0, 3, []uint32{resl.OK}, 10, 5*time.Second)
	runBiDirectionResiliency(resiliencyAdapter, 0, 3, []uint32{resl.OK}, 10, 10*time.Second)
}

// func RunSayHello(adapter *hello.HelloAdapter, name string) {
// 	res, err := adapter.SayHello(context.Background(), &hellopb.HelloRequest{
// 		Name: name,
// 	})
// 	if err != nil {
// 		log.Fatalf("No hello %v", err)
// 	}
// 	log.Println(res.Greet)
// }

// func RunSayManyHello(adapter *hello.HelloAdapter, name string) {
// 	adapter.SayManyHello(context.Background(), name)
// }

// func SayHelloToEveryone(adapter *hello.HelloAdapter, names []string) {
// 	adapter.SayHelloToEveryone(context.Background(), names)
// }

// func SayHelloContinuous(adapter *hello.HelloAdapter, names []string) {
// 	adapter.SayHelloToEveryone(context.Background(), names)
// }

func runGetCurrentBalance(adapter *bank.BankAdapter, acct string) {
	bal, err := adapter.GetCurrentBalance(context.Background(), acct)
	if err != nil {
		log.Fatalln("Fail to call GetCurrentBalance :", err)
	}
	log.Println(bal)
}

func runFetchExchangeRate(adapter *bank.BankAdapter, fromCur, toCur string) {
	adapter.FetchExchangeRates(context.Background(), fromCur, toCur)
}

func runSumarizeTransaction(adapter *bank.BankAdapter, acct string, numDummyTransactions int) {
	var tx []dbank.Transaction
	for i := 1; i <= numDummyTransactions; i++ {
		ttype := dbank.TransactionTypeIn

		if i%3 == 0 {
			ttype = dbank.TransactionTypeOut
		}
		t := dbank.Transaction{
			Amount:          float64(rand.Intn(500) + 10),
			TransactionType: ttype,
			Notes:           fmt.Sprint("Dummy transaction %v", i),
		}
		tx = append(tx, t)
	}
	adapter.SummarizeTransactions(context.Background(), acct, tx)
}

func runTransferMultiple(adapter *bank.BankAdapter, fromAcct, toAcct string, numDummyTransaction int) {
	var trf []dbank.TransferTransaction
	for i := 1; i <= numDummyTransaction; i++ {
		tr := dbank.TransferTransaction{
			FromAccountNumber: fromAcct,
			ToAccountNumber:   toAcct,
			Currency:          "USD",
			Amount:            float64(rand.Intn(200) + 5),
		}
		trf = append(trf, tr)
	}
	adapter.TransferMultiple(context.Background(), trf)
}

func runUnaryResiliencyWithTimeout(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	res, err := adapter.UnaryResiliency(ctx, minDelaySecond, maxDelaySecond, statusCodes)

	if err != nil {
		log.Fatalln("Failted to call UnaryResponse")
	}

	log.Println(res.DummyString)
}
func runServerStreamResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	adapter.ServerStreamResiliency(ctx, minDelaySecond, maxDelaySecond, statusCodes)

}
func runClientStreamResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32, count int, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	adapter.ClientStreamResiliency(ctx, minDelaySecond, maxDelaySecond, statusCodes, count)

}
func runBiDirectionResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32, count int, timeout time.Duration) {
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	adapter.BiDirectionResiliency(ctx, minDelaySecond, maxDelaySecond, statusCodes, count)
}
