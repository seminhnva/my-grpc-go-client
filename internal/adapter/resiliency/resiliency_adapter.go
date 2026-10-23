package resiliencty

import (
	"context"
	"io"
	"log"

	"github.com/seminhnva/my-grpc-client/internal/port"
	"github.com/seminhnva/my-grpc-proto/protogen/go/resiliency"
	resl "github.com/seminhnva/my-grpc-proto/protogen/go/resiliency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func isStreamCancellation(ctx context.Context, err error) bool {
	if ctx.Err() != nil {
		return true
	}
	code := status.Code(err)
	return code == codes.Canceled || code == codes.DeadlineExceeded
}

type ResiliencyAdapter struct {
	resiliencyClient             port.ResiliencyClientPort
	resiliencyClientWithMetadata port.ResiliencyWithMetadataClientPort
}

func NewResiliencyAdapter(conn *grpc.ClientConn) (*ResiliencyAdapter, error) {
	client := resl.NewResilienceServiceClient(conn)
	clientWithMetadata := resiliency.NewResilienceWithMetadataServiceClient(conn)
	return &ResiliencyAdapter{
		resiliencyClient:             client,
		resiliencyClientWithMetadata: clientWithMetadata,
	}, nil
}
func (a *ResiliencyAdapter) UnaryResiliency(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32) (*resiliency.ResiliencyResponse, error) {
	resiliencyRequest := &resl.ResiliencyRequest{
		MinDelaySecond: minDelaySecond,
		MaxDelaySecond: maxDelaySecond,
		StatusCodes:    statusCodes,
	}

	res, err := a.resiliencyClient.UnaryResiliency(ctx, resiliencyRequest)
	if err != nil {
		log.Println("Error on UnaryResiliency: ", err)
		return nil, err
	}
	return res, nil
}
func (a *ResiliencyAdapter) ServerStreamResiliency(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32) {
	resiliencyRequest := &resl.ResiliencyRequest{
		MinDelaySecond: minDelaySecond,
		MaxDelaySecond: maxDelaySecond,
		StatusCodes:    statusCodes,
	}
	reslStream, err := a.resiliencyClient.ServerStreamResiliency(ctx, resiliencyRequest)
	if err != nil {
		if isStreamCancellation(ctx, err) {
			log.Printf("ServerStreamResiliency stopped: %v", err)
			return
		}
		log.Fatalln("Error on ServerStreamResiliency: ", err)
	}
	for {
		res, err := reslStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isStreamCancellation(ctx, err) {
				log.Printf("ServerStreamResiliency stopped: %v", err)
				return
			}
			log.Fatalln("Error on ServerStreamResiliency: ", err)
		}
		log.Println(res.DummyString)
	}
}

func (a *ResiliencyAdapter) ClientStreamResiliency(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32, count int) {
	resslStream, err := a.resiliencyClient.ClientStreamResiliency(ctx)
	if err != nil {
		log.Fatalln("Error on ClientStreamResiliency", err)
	}

	for i := 1; i <= count; i++ {
		resiliencyRequest := &resl.ResiliencyRequest{
			MinDelaySecond: minDelaySecond,
			MaxDelaySecond: maxDelaySecond,
			StatusCodes:    statusCodes,
		}
		resslStream.Send(resiliencyRequest)
	}

	res, err := resslStream.CloseAndRecv()
	if err != nil {
		log.Fatalln("Error on ClientStreamResiliency", err)
	}
	log.Println(res.DummyString)

}

func (a *ResiliencyAdapter) BiDirectionResiliency(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32, count int) {
	reslStream, err := a.resiliencyClient.BiDirectionResiliency(ctx)

	if err != nil {
		log.Fatalln("Error on BiDirectionResiliency")
	}

	reaslChan := make(chan struct{})
	go func() {
		for i := 1; i <= count; i++ {
			resiliencyRequest := &resl.ResiliencyRequest{
				MinDelaySecond: minDelaySecond,
				MaxDelaySecond: maxDelaySecond,
				StatusCodes:    statusCodes,
			}

			if err := reslStream.Send(resiliencyRequest); err != nil {
				log.Fatalln("Error on ClientStreamResiliency")

			}
		}
		reslStream.CloseSend()
	}()

	go func() {
		for {
			res, err := reslStream.Recv()
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln("Error on BiDirectionResiliency:", err)
			}
			log.Println(res.DummyString)
		}
		close(reaslChan)
	}()

	<-reaslChan

}
