package main

import (
	"context"
	"log"

	"github.com/seminhnva/my-grpc-client/internal/adapter/hello"
	hellopb "github.com/seminhnva/my-grpc-proto/protogen/go/hello"
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

	helloAdapter := hello.NewHelloAdapter(conn)
	// RunSayHello(helloAdapter, "Minbeo")
	// RunSayManyHello(helloAdapter, "Minbeo")
	// SayHelloToEveryone(helloAdapter, []string{"superman", "thor", "batman"})
	SayHelloContinuous(helloAdapter, []string{"superman", "thor", "batman"})

}

func RunSayHello(adapter *hello.HelloAdapter, name string) {
	res, err := adapter.SayHello(context.Background(), &hellopb.HelloRequest{
		Name: name,
	})
	if err != nil {
		log.Fatalf("No hello %v", err)
	}
	log.Println(res.Greet)
}

func RunSayManyHello(adapter *hello.HelloAdapter, name string) {
	adapter.SayManyHello(context.Background(), name)
}

func SayHelloToEveryone(adapter *hello.HelloAdapter, names []string) {
	adapter.SayHelloToEveryone(context.Background(), names)
}

func SayHelloContinuous(adapter *hello.HelloAdapter, names []string) {
	adapter.SayHelloToEveryone(context.Background(), names)
}
