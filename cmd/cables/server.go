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

type cablesServer struct {
	pb.UnimplementedCablesServiceServer
	consumerGroups map[string]*consumer.ConsumerGroup
}

func (s *cablesServer) allConsumers() []*consumer.Consumer {
	consumers := []*consumer.Consumer{}
	for _, group := range s.consumerGroups {
		consumers = append(consumers, group.AllConsumers()...)
	}

	return consumers
}

func (s *cablesServer) processClientConfig(config *cables.CablesClientConfig, stream pb.CablesService_HookServer) {
	if config.CanConsume {
		newConsumer := &consumer.Consumer{
			Name:          config.ClientName,
			ConsumerGroup: config.ConsumerGroup,
			Topics:        config.ConsumeTopics,
			ReturnStream:  stream,
		}

		if s.consumerGroups[config.ConsumerGroup] == nil {
			s.consumerGroups[config.ConsumerGroup] = consumer.NewConsumerGroup(config.ConsumerGroup)
		}
		s.consumerGroups[config.ConsumerGroup].AddConsumer(newConsumer)
	}
}

func (s *cablesServer) Cleanup() {
	for _, con := range s.allConsumers() {
		if con.ReturnStream.Context().Err() != nil {
			s.consumerGroups[con.ConsumerGroup].RemoveConsumer(con)
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

	s.processClientConfig(&config, stream)
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
		s.ProcessPublished(in)
	}
}

func (s *cablesServer) ProcessPublished(message *pb.Message) error {
	for _, group := range s.consumerGroups {
		con := group.ConsumerOfTopic(message.GetTopic())
		if con == nil {
			return nil
		}
		errConsume := con.Consume(message)
		if errConsume != nil {
			return errConsume
		}
	}
	return nil
}

func NewCablesServer() *cablesServer {
	return &cablesServer{
		consumerGroups: map[string]*consumer.ConsumerGroup{},
	}
}
