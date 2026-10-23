package resiliencty

import (
	"context"
	"fmt"
	"io"
	"log"
	"runtime"
	"time"

	"github.com/google/uuid"

	"github.com/seminhnva/my-grpc-proto/protogen/go/resiliency"
	resl "github.com/seminhnva/my-grpc-proto/protogen/go/resiliency"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

func sampleRequestMetadata() metadata.MD {
	md := map[string]string{
		"grpc-client-time":  fmt.Sprint(time.Now().Format("15:04:05")),
		"grpc-client-os":    runtime.GOOS,
		"grpc-request-uuid": uuid.New().String(),
	}
	return metadata.New(md)
}
func sampleResponseMetadata(md metadata.MD) {
	if md.Len() == 0 {
		log.Println("Response metadata not found")
	} else {
		log.Println("Response meta data:")
		for k, v := range md {
			log.Printf(" %v : %v\n", k, v)
		}
	}
}

func (a *ResiliencyAdapter) UnaryResiliencyWithMetadata(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32) (*resiliency.ResiliencyResponse, error) {
	ctx = metadata.NewOutgoingContext(ctx, sampleRequestMetadata())

	resiliencyRequest := &resl.ResiliencyRequest{
		MinDelaySecond: minDelaySecond,
		MaxDelaySecond: maxDelaySecond,
		StatusCodes:    statusCodes,
	}

	var responseMetadata metadata.MD
	res, err := a.resiliencyClientWithMetadata.UnaryResiliencyWithMetadata(ctx, resiliencyRequest, grpc.Header(&responseMetadata))
	if err != nil {
		log.Println("Error on UnaryResiliency: ", err)
		return nil, err
	}

	sampleResponseMetadata(responseMetadata)
	return res, nil
}
func (a *ResiliencyAdapter) ServerStreamResiliencyWithMetadata(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32) {
	ctx = metadata.NewOutgoingContext(ctx, sampleRequestMetadata())

	resiliencyRequest := &resl.ResiliencyRequest{
		MinDelaySecond: minDelaySecond,
		MaxDelaySecond: maxDelaySecond,
		StatusCodes:    statusCodes,
	}
	reslStream, err := a.resiliencyClientWithMetadata.ServerStreamResiliencyWithMetadata(ctx, resiliencyRequest)
	if err != nil {
		if isStreamCancellation(ctx, err) {
			log.Printf("ServerStreamResiliencyWithMetadata stopped: %v", err)
			return
		}
		log.Fatalln("Error on ServerStreamResiliencyWithMetadata: ", err)
	}

	if responseMetadata, err := reslStream.Header(); err == nil {
		sampleResponseMetadata(responseMetadata)
	}

	for {
		res, err := reslStream.Recv()
		if err == io.EOF {
			break
		}
		if err != nil {
			if isStreamCancellation(ctx, err) {
				log.Printf("ServerStreamResiliencyWithMetadata stopped: %v", err)
				return
			}
			log.Fatalln("Error on ServerStreamResiliencyWithMetadata: ", err)
		}
		log.Println(res.DummyString)
	}
}

func (a *ResiliencyAdapter) ClientStreamResiliencyWithMetadata(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32, count int) {

	resslStream, err := a.resiliencyClientWithMetadata.ClientStreamResiliencyWithMetadata(ctx)
	if err != nil {
		log.Fatalln("Error on ClientStreamResiliencyWithMetadata", err)
	}

	for i := 1; i <= count; i++ {
		ctx = metadata.NewOutgoingContext(ctx, sampleRequestMetadata())

		resiliencyRequest := &resl.ResiliencyRequest{
			MinDelaySecond: minDelaySecond,
			MaxDelaySecond: maxDelaySecond,
			StatusCodes:    statusCodes,
		}
		resslStream.Send(resiliencyRequest)
	}

	res, err := resslStream.CloseAndRecv()
	if err != nil {
		log.Fatalln("Error on ClientStreamResiliencyWithMetadata", err)
	}
	log.Println(res.DummyString)
	if responseMetadata, err := resslStream.Header(); err == nil {
		sampleResponseMetadata(responseMetadata)
	}
}

func (a *ResiliencyAdapter) BiDirectionResiliencyWithMetadata(ctx context.Context, minDelaySecond int32,
	maxDelaySecond int32, statusCodes []uint32, count int) {

	reslStream, err := a.resiliencyClientWithMetadata.BiDirectionResiliencyWithMetadata(ctx)

	if err != nil {
		log.Fatalln("Error on BiDirectionResiliencyWithMetadata")
	}
	if responseMetadata, err := reslStream.Header(); err == nil {
		sampleResponseMetadata(responseMetadata)
	}

	reaslChan := make(chan struct{})
	go func() {
		ctx = metadata.NewOutgoingContext(ctx, sampleRequestMetadata())
		for i := 1; i <= count; i++ {
			resiliencyRequest := &resl.ResiliencyRequest{
				MinDelaySecond: minDelaySecond,
				MaxDelaySecond: maxDelaySecond,
				StatusCodes:    statusCodes,
			}

			if err := reslStream.Send(resiliencyRequest); err != nil {
				log.Fatalln("Error on BiDirectionResiliencyWithMetadata")

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
