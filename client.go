package cables

import (
	pb "cables/generated"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"

	"google.golang.org/grpc"
)

type CablesClient struct {
	client         pb.CablesServiceClient
	PublishChannel chan *pb.Message
	quitCh         chan bool
	handleFunc     func(*pb.Message) error
	conn           *grpc.ClientConn
	config         *CablesClientConfig
}

type CablesClientConfig struct {
	ClientName    string   `json:"client_name,omitempty"`
	ConsumerGroup string   `json:"consumer_group,omitempty"`
	ConsumeTopics []string `json:"consume_topics,omitempty"`
	CanConsume    bool     `json:"can_consume,omitempty"`
}

func NewClient(config *CablesClientConfig, handleFunc func(*pb.Message) error) *CablesClient {
	opts := []grpc.DialOption{
		grpc.WithInsecure(),
	}
	conn, err := grpc.NewClient("localhost:8082", opts...)
	if err != nil {
		log.Fatalf("Conn failure: %v", err)
	}

	return &CablesClient{
		client:         pb.NewCablesServiceClient(conn),
		PublishChannel: make(chan *pb.Message, 100),
		quitCh:         make(chan bool),
		handleFunc:     handleFunc,
		conn:           conn,
		config:         config,
	}
}

func (c *CablesClient) Close() {
	if c.conn != nil {
		c.conn.Close()
	}
	close(c.quitCh)
	close(c.PublishChannel)
}

func (c *CablesClient) Publish(message *pb.Message) error {
	c.PublishChannel <- message
	return nil
}

func (c *CablesClient) Hook() error {
	opts := []grpc.CallOption{}
	stream, err := c.client.Hook(context.Background(), opts...)
	if err != nil {
		return err
	}
	configBytes, errConfig := json.Marshal(c.config)
	if errConfig != nil {
		return errConfig
	}

	stream.Send(&pb.Message{
		Message: configBytes,
		Qos:     1,
	})
	// Publish
	go func() {
		for message := range c.PublishChannel {
			stream.Send(message)
		}
	}()

	// Consume
	go func() {
		for {
			in, err := stream.Recv()
			if err == io.EOF {
				// read done.
				c.Close()
				return
			}
			if err != nil {
				log.Fatalf("Failed to receive a note: %v", err)
			}
			errHandle := c.handleFunc(in)
			if errHandle != nil {
				log.Fatalf("Error in message processing: %v", errHandle)
			}
		}
	}()

	<-c.quitCh
	fmt.Println("Exiting Hook")
	err = stream.CloseSend()
	if err != nil {
		return err
	}

	return nil
}
