package main

import (
	"fmt"
	"log"
	"net"

	"google.golang.org/grpc"

	pb "cables/generated"
)

const port = 8082

func main() {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)
	pb.RegisterCablesServiceServer(grpcServer, NewCablesServer())
	fmt.Println("Server Started")
	grpcServer.Serve(lis)

}
