package port

import (
	"context"

	"github.com/seminhnva/my-grpc-proto/protogen/go/resiliency"
	"google.golang.org/grpc"
)

type ResiliencyClientPort interface {
	UnaryResiliency(ctx context.Context, in *resiliency.ResiliencyRequest, opts ...grpc.CallOption) (*resiliency.ResiliencyResponse, error)
	ServerStreamResiliency(ctx context.Context, in *resiliency.ResiliencyRequest, opts ...grpc.CallOption) (grpc.ServerStreamingClient[resiliency.ResiliencyResponse], error)
	ClientStreamResiliency(ctx context.Context, opts ...grpc.CallOption) (grpc.ClientStreamingClient[resiliency.ResiliencyRequest, resiliency.ResiliencyResponse], error)
	BiDirectionResiliency(ctx context.Context, opts ...grpc.CallOption) (grpc.BidiStreamingClient[resiliency.ResiliencyRequest, resiliency.ResiliencyResponse], error)
}
