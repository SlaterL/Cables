package cables

import (
	pb "cables/generated"
	"context"
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"
)

type CablesClient struct {
	client pb.CablesServiceClient
}

func NewClient(client pb.CablesServiceClient) *CablesClient {
	return &CablesClient{
		client: client,
	}
}

func (c *CablesClient) Hook() error {
	opts := []grpc.CallOption{}
	stream, err := c.client.Hook(context.Background(), opts...)
	if err != nil {
		return nil
	}
	waitc := make(chan struct{})
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				close(waitc)
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive a note : %v", err)
			}
			log.Printf("Got message %s", in.Message)
		}
	}()

	for i := range 5 {
		m := &pb.Message{
			Qos:     1,
			Message: []byte(fmt.Sprintf("Here is a message: %d", i)),
		}
		stream.Send(m)
	}
	err = stream.CloseSend()
	if err != nil {
		return err
	}
	<-waitc

	return nil

}
