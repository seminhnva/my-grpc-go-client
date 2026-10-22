package main

import (
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	grpc_retry "github.com/grpc-ecosystem/go-grpc-middleware/retry"
	"github.com/seminhnva/my-grpc-client/internal/adapter/bank"
	resiliencty "github.com/seminhnva/my-grpc-client/internal/adapter/resiliency"
	dbank "github.com/seminhnva/my-grpc-client/internal/application/domain/bank"
	resl "github.com/seminhnva/my-grpc-client/internal/application/domain/resiliency"
	"github.com/seminhnva/my-grpc-proto/protogen/go/resiliency"
	"github.com/sony/gobreaker"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
)

var cbreaker *gobreaker.CircuitBreaker

func init() {
	mybreaker := gobreaker.Settings{
		Name: "my-circuit-breaker",
		ReadyToTrip: func(counts gobreaker.Counts) bool {
			failureRatio := float64(counts.TotalFailures) / float64(counts.Requests)

			log.Printf("Circuit breaker failure is %v, request is %v, means failutre ratio %v", counts.TotalFailures, counts.Requests, failureRatio)
			return counts.Requests >= 3 && failureRatio >= 0.6
		},
		Timeout:     4,
		MaxRequests: 3,
		OnStateChange: func(name string, from, to gobreaker.State) {
			log.Printf("Circuit breaker %v changed state, form %v to %v \n\n", name, from, to)
		},
	}
	cbreaker = gobreaker.NewCircuitBreaker(mybreaker)

}
func main() {
	log.SetFlags(0)
	log.SetOutput(log.Writer())

	var opts []grpc.DialOption
	opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	opts = append(opts, grpc.WithUnaryInterceptor(
		grpc_retry.UnaryClientInterceptor(
			grpc_retry.WithCodes(codes.Unknown, codes.NotFound),
			grpc_retry.WithMax(4),
			grpc_retry.WithBackoff(grpc_retry.BackoffExponential(2*time.Second)),
		),
	))
	opts = append(opts, grpc.WithStreamInterceptor(
		grpc_retry.StreamClientInterceptor(
			grpc_retry.WithCodes(codes.Unknown, codes.NotFound),
			grpc_retry.WithMax(4),
			grpc_retry.WithBackoff(grpc_retry.BackoffLinear(1*time.Second)),
		),
	))
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
	// runBiDirectionResiliency(resiliencyAdapter, 0, 3, []uint32{resl.OK}, 10, 10*time.Second)

	// runUnaryResiliency(resiliencyAdapter, 0, 3, []uint32{resl.UNKNOWN, resl.OK})
	// runServerStreamResiliency(resiliencyAdapter, 0, 1, []uint32{resl.UNKNOWN, resl.NOT_FOUND, resl.OK}, 1*time.Second)
	// runClientStreamResiliency(resiliencyAdapter, 0, 1, []uint32{resl.UNKNOWN, resl.NOT_FOUND, resl.OK}, 2)

	// Repeated entries are weights: 8 UNKNOWN + 2 OK gives an 80/20 distribution.
	statusCodeMix := []uint32{
		resl.UNKNOWN, resl.UNKNOWN, resl.UNKNOWN, resl.UNKNOWN,
		resl.UNKNOWN, resl.UNKNOWN, resl.UNKNOWN, resl.UNKNOWN,
		resl.OK, resl.OK,
	}
	for i := 0; i < 300; i++ {
		runUnaryResiliencyWithCircuit(resiliencyAdapter, 0, 3, statusCodeMix)
		time.Sleep(time.Second)
	}
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
			Notes:           fmt.Sprintf("Dummy transaction %v", i),
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

func runUnaryResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32) {

	res, err := adapter.UnaryResiliency(context.Background(), minDelaySecond, maxDelaySecond, statusCodes)

	if err != nil {
		log.Fatalln("Failted to call UnaryResponse")
	}

	log.Println(res.DummyString)
}
func runServerStreamResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32) {

	adapter.ServerStreamResiliency(context.Background(), minDelaySecond, maxDelaySecond, statusCodes)

}
func runClientStreamResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32, count int) {

	adapter.ClientStreamResiliency(context.Background(), minDelaySecond, maxDelaySecond, statusCodes, count)

}
func runBiDirectionResiliency(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32, count int) {

	adapter.BiDirectionResiliency(context.Background(), minDelaySecond, maxDelaySecond, statusCodes, count)
}
func runUnaryResiliencyWithCircuit(adapter *resiliencty.ResiliencyAdapter, minDelaySecond, maxDelaySecond int32, statusCodes []uint32) {

	cbreakerRes, cbreakerErr := cbreaker.Execute(
		func() (interface{}, error) {
			return adapter.UnaryResiliency(context.Background(), minDelaySecond, maxDelaySecond, statusCodes)
		},
	)

	if cbreakerErr != nil {
		log.Println("Failed to call Unary Resiliency:", cbreakerErr)
	} else {
		log.Println(cbreakerRes.(*resiliency.ResiliencyResponse).DummyString)
	}

}
