package hello

import (
	"context"
	"io"
	"log"

	"github.com/seminhnva/my-grpc-client/internal/port"
	"github.com/seminhnva/my-grpc-proto/protogen/go/hello"
	hellopb "github.com/seminhnva/my-grpc-proto/protogen/go/hello"
	"google.golang.org/grpc"
)

type HelloAdapter struct {
	helloClient port.HelloClientPort
}

func NewHelloAdapter(conn *grpc.ClientConn) *HelloAdapter {
	client := hellopb.NewHelloServiceClient(conn)
	return &HelloAdapter{
		helloClient: client,
	}
}

func (a *HelloAdapter) SayHello(ctx context.Context, req *hellopb.HelloRequest) (*hellopb.HelloResponse, error) {
	return a.helloClient.SayHello(ctx, req)
}

func (a *HelloAdapter) SayManyHello(ctx context.Context, name string) {

	helloReq := &hello.HelloRequest{
		Name: name,
	}
	stream, err := a.helloClient.SayManyHello(ctx, helloReq)
	if err != nil {
		log.Fatalf("Err on saymanyHellos :%v", err)
	}
	for {
		greet, err := stream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatalln("Error on sayManyHellos", err)
		}
		log.Println(greet.Greet)
	}
}
