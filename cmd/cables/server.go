package main

import (
	"cables"
	pb "cables/generated"
	"encoding/json"
	"fmt"
	"io"

	"cables/internal/consumer"
)

var ackMessage = &pb.Message{Message: []byte("ACK")}

type ConsumerGrouper interface {
	Add(*consumer.Consumer) error
	Remove(*consumer.Consumer) error
	ProcessMessage(*pb.Message) error
	All() []*consumer.Consumer
}

type cablesServer struct {
	pb.UnimplementedCablesServiceServer
	consumerGroups map[string]ConsumerGrouper
}

func (s *cablesServer) allConsumers() []*consumer.Consumer {
	consumers := []*consumer.Consumer{}
	for _, group := range s.consumerGroups {
		consumers = append(consumers, group.All()...)
	}

	return consumers
}

func (s *cablesServer) processClientConfig(config *cables.CablesClientConfig, stream pb.CablesService_HookServer) error {
	if config.CanConsume {
		newConsumer := &consumer.Consumer{
			Name:          config.ClientName,
			ConsumerGroup: config.ConsumerGroup,
			Topics:        config.ConsumeTopics,
			ReturnStream:  stream,
		}

		if s.consumerGroups[config.ConsumerGroup] == nil {
			switch config.ConsumerGroupType {
			case 0:
				s.consumerGroups[config.ConsumerGroup] = consumer.NewConsumerGroupClassic(config.ConsumerGroup, config.ConsumeTopics)
			case 1:
				s.consumerGroups[config.ConsumerGroup] = consumer.NewConsumerGroupQueue(config.ConsumerGroup)
			}
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

func (s *cablesServer) Hook(stream pb.CablesService_HookServer) error {
	defer s.Cleanup()
	configMessage, errConfig := stream.Recv()
	if errConfig != nil {
		return errConfig
	}

	var config cables.CablesClientConfig
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

func NewCablesServer() *cablesServer {
	return &cablesServer{
		consumerGroups: map[string]ConsumerGrouper{},
	}
}
