package consumer

import pb "cables/generated"

type Consumer struct {
	Name          string
	ReturnStream  pb.CablesService_HookServer
	ConsumerGroup string
	Topics        []string
}

func (c *Consumer) Consume(message *pb.Message) error {
	return c.ReturnStream.Send(message)
}
