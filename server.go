package cables

import (
	pb "cables/generated"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"

	"cables/internal/consumer"

	"google.golang.org/grpc"
)

var ackMessage = &pb.Message{Message: []byte("ACK")}

type cablesServer struct {
	pb.UnimplementedCablesServiceServer
	consumerGroups map[string]*consumer.ConsumerGroup
	grpcServer     *grpc.Server
	netListener    net.Listener
}

func (s *cablesServer) allConsumers() []*consumer.Consumer {
	consumers := []*consumer.Consumer{}
	for _, group := range s.consumerGroups {
		consumers = append(consumers, group.All()...)
	}

	return consumers
}

func (s *cablesServer) processClientConfig(config *CablesClientConfig, stream pb.CablesService_HookServer) error {
	if config.CanConsume {
		newConsumer := &consumer.Consumer{
			Name:          config.ClientName,
			ConsumerGroup: config.ConsumerGroup,
			Topics:        config.ConsumeTopics,
			ReturnStream:  stream,
		}

		if _, ok := s.consumerGroups[config.ConsumerGroup]; !ok {
			s.consumerGroups[config.ConsumerGroup] = consumer.NewConsumerGroup(config.ConsumerGroup, config.ConsumeTopics)
		}
		return s.consumerGroups[config.ConsumerGroup].Add(newConsumer)
	}
	return nil
}

func (s *cablesServer) Cleanup() {
	for _, con := range s.allConsumers() {
		if con.ReturnStream.Context().Err() != nil {
			s.consumerGroups[con.ConsumerGroup].Remove(con)
			fmt.Printf("Consumer Removed (%v) from group: %v\n", con.Name, con.ConsumerGroup)
		}
	}
}

func (s *cablesServer) Serve() error {
	return s.grpcServer.Serve(s.netListener)
}

func (s *cablesServer) Hook(stream pb.CablesService_HookServer) error {
	defer s.Cleanup()
	configMessage, errConfig := stream.Recv()
	if errConfig != nil {
		return errConfig
	}

	var config CablesClientConfig
	errUnmarshal := json.Unmarshal(configMessage.GetMessage(), &config)
	if errUnmarshal != nil {
		return errUnmarshal
	}

	configErr := s.processClientConfig(&config, stream)
	if configErr != nil {
		return configErr
	}
	stream.Send(ackMessage)

	fmt.Printf("Connection Established: %v\n", config.ClientName)
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		// TODO: Respect QoS definitions
		switch in.GetQos() {
		case 0:
		case 1:
		case 2:
		case 3:
			s.ProcessPublished(in)
		default:
			return fmt.Errorf("Invalid QoS")
		}
	}
}

func (s *cablesServer) ProcessPublished(message *pb.Message) error {
	for _, group := range s.consumerGroups {
		err := group.ProcessMessage(message)
		if err != nil {
			return err
		}
	}
	return nil
}

func NewCablesServer(port int) *cablesServer {
	lis, err := net.Listen("tcp", fmt.Sprintf("localhost:%d", port))
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}
	var opts []grpc.ServerOption
	grpcServer := grpc.NewServer(opts...)

	s := &cablesServer{
		netListener:    lis,
		grpcServer:     grpcServer,
		consumerGroups: map[string]*consumer.ConsumerGroup{},
	}
	pb.RegisterCablesServiceServer(grpcServer, s)

	return s
}
