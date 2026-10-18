package resiliencty

import (
	"context"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestIsStreamCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	tests := []struct {
		name string
		ctx  context.Context
		err  error
		want bool
	}{
		{name: "context canceled", ctx: ctx, err: status.Error(codes.Unknown, "stream error"), want: true},
		{name: "deadline exceeded", ctx: context.Background(), err: status.Error(codes.DeadlineExceeded, "deadline"), want: true},
		{name: "rpc canceled", ctx: context.Background(), err: status.Error(codes.Canceled, "canceled"), want: true},
		{name: "other error", ctx: context.Background(), err: status.Error(codes.Internal, "internal"), want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isStreamCancellation(tt.ctx, tt.err); got != tt.want {
				t.Errorf("isStreamCancellation() = %v, want %v", got, tt.want)
			}
		})
	}
}
