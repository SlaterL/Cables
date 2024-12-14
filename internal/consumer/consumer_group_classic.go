package consumer

import (
	pb "cables/generated"
	"fmt"
	"reflect"
)

type ConsumerGroupClassic struct {
	Name      string
	Consumers []*Consumer
	Offset    int
	Topics    []string
}

// Returns a new ConsumerGroupClassic. Only one consumer per
// group will recieve a copy of a published message.
// This allows for multiple instances of a handler to
// run without duplication of work.
// Classic version only allows for consumers with the same
// topic specifications.
func NewConsumerGroupClassic(name string, topics []string) *ConsumerGroupClassic {
	return &ConsumerGroupClassic{
		Name:      name,
		Topics:    topics,
		Offset:    0,
		Consumers: []*Consumer{},
	}
}

func listEq(t, c []string) bool {
	if len(t) != len(c) {
		return false
	}
	return reflect.DeepEqual(t, c)
}

func (c *ConsumerGroupClassic) Add(con *Consumer) error {
	if !listEq(c.Topics, con.Topics) {
		return fmt.Errorf("Consumer rejected due to topic mismatch. Expected: %v", c.Topics)
	}
	c.Consumers = append(c.Consumers, con)
	return nil
}

func (c *ConsumerGroupClassic) Remove(con *Consumer) error {
	remaining := []*Consumer{}
	removed := false
	for _, elm := range c.Consumers {
		if elm != con {
			remaining = append(remaining, elm)
		} else {
			removed = true
		}
	}
	if !removed {
		return fmt.Errorf("No element to remove from consumer group")
	}
	c.Consumers = remaining

	return nil
}

func (c *ConsumerGroupClassic) ProcessMessage(m *pb.Message) error {
	errConsume := c.Consumers[c.Offset].Consume(m)
	if errConsume != nil {
		return errConsume
	}
	c.Offset = (c.Offset + 1) % len(c.Consumers)

	return nil
}

func (c *ConsumerGroupClassic) All() []*Consumer {
	return c.Consumers
}
