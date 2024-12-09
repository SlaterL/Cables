package main

import (
	"cables"
	pb "cables/generated"
	"log"

	"google.golang.org/grpc"
)

func main() {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.NewClient("localhost:8082", opts...)
	if err != nil {
		log.Fatal("Conn failure: %v", err)
	}
	defer conn.Close()

	client := cables.NewClient(pb.NewCablesServiceClient(conn))

	client.Hook()

}
