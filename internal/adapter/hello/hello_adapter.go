package hello

import (
	"context"

	"github.com/seminhnva/my-grpc-client/internal/port"
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

func (a *HelloAdapter) SayHello(ctx context.Context, req *hellopb.HelloRequest, opts ...grpc.CallOption) (*hellopb.HelloResponse, error) {
	return a.helloClient.SayHello(ctx, req, opts...)
}
