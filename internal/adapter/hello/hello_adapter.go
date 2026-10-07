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

func (a *HelloAdapter) SayHelloToEveryone(ctx context.Context, names []string) {
	greetStream, err := a.helloClient.SayHelloToEveryone(ctx)
	if err != nil {
		log.Fatalln("Err while stream SayHelloToEveyrone", err)
	}
	for _, name := range names {
		if err := greetStream.Send(&hellopb.HelloRequest{
			Name: name,
		}); err != nil {
			log.Fatalln("Err while stream SayHelloToEveyrone", err)

		}
	}
	res, err := greetStream.CloseAndRecv()
	if err != nil {
		log.Fatalln("Err while stream SayHelloToEveyrone", err)
	}
	log.Println(res)
}

func (a *HelloAdapter) SayHelloContinuous(ctx context.Context, names []string) {
	greetStream, err := a.helloClient.SayHelloContinuous(ctx)

	if err != nil {
		log.Fatalln("Err while stream SayHelloContinuous", err)
	}
	greetCh := make(chan any)
	go func() {
		for _, name := range names {
			greetStream.Send(&hellopb.HelloRequest{
				Name: name,
			})
		}
	}()

	go func() {
		res, err := greetStream.Recv()
		if err == io.EOF {
			close(greetCh)
		}
		if err != nil {
			log.Fatalln("Err while stream SayHelloContinuous", err)
		}
		log.Println(res.Greet)
	}()
	<-greetCh
}
