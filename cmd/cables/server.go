package main

import (
	pb "cables/generated"
	"fmt"
	"io"
)

type cablesServer struct {
	pb.UnimplementedCablesServiceServer
}

func (s *cablesServer) Hook(stream pb.CablesService_HookServer) error {
	fmt.Println("Connection Established")
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		fmt.Println(string(in.Message))
		if in.Qos > 0 {
			retMessage := pb.Message{
				Message: []byte("Ack"),
				Qos:     0,
			}
			sendErr := stream.Send(&retMessage)
			if sendErr != nil {
				return sendErr
			}
		}
	}
}

func NewCablesServer() *cablesServer {
	return &cablesServer{}

}
