package main

import (
	"cables"
	pb "cables/generated"
	"encoding/json"
	"fmt"
	"io"
)

type cablesServer struct {
	pb.UnimplementedCablesServiceServer
	consumers []*Consumer
}

type Consumer struct {
	Name          string
	ReturnStream  pb.CablesService_HookServer
	ConsumerGroup string
	Topics        []string
}

func (c *Consumer) Consume(message *pb.Message) error {
	return c.ReturnStream.Send(message)
}

func (s *cablesServer) processClientConfig(config *cables.CablesClientConfig, stream pb.CablesService_HookServer) {
	if config.CanConsume {
		newConsumer := &Consumer{
			Name:          config.ClientName,
			ConsumerGroup: config.ConsumerGroup,
			Topics:        config.ConsumeTopics,
			ReturnStream:  stream,
		}
		s.consumers = append(s.consumers, newConsumer)
	}
}

func (s *cablesServer) Cleanup() {
	var aliveConsumers []*Consumer
	for _, consumer := range s.consumers {
		if consumer.ReturnStream.Context().Err() == nil {
			aliveConsumers = append(aliveConsumers, consumer)
		}
	}
	s.consumers = aliveConsumers
}

func (s *cablesServer) Hook(stream pb.CablesService_HookServer) error {
	fmt.Println("Connection Established")
	configMessage, errConfig := stream.Recv()
	var config cables.CablesClientConfig
	json.Unmarshal(configMessage.GetMessage(), &config)
	s.processClientConfig(&config, stream)
	if errConfig != nil {
		return errConfig
	}
	for {
		in, err := stream.Recv()
		if err == io.EOF {
			// TODO: resolve client remove bug
			s.Cleanup()
			return nil
		}
		if err != nil {
			return err
		}
		s.ProcessPublished(in)
	}
}

func StringInSlice(s string, list []string) bool {
	for _, item := range list {
		if item == s {
			return true
		}
	}
	return false
}

func (s *cablesServer) ProcessPublished(message *pb.Message) error {
	for _, consumer := range s.consumers {
		if StringInSlice(message.GetTopic(), consumer.Topics) {
			errConsume := consumer.Consume(message)
			if errConsume != nil {
				return errConsume
			}
		}
	}
	return nil
}

func NewCablesServer() *cablesServer {
	return &cablesServer{}
}
