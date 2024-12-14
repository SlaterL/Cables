package consumer

import pb "cables/generated"

type ConsumerGroupQueue struct {
	Name      string
	Consumers ConsumerQueue
}

// Returns a new ConsumerGroupQueue. Only one consumer per
// group will recieve a copy of a published message.
// This allows for multiple instances of a handler to
// run without duplication of work.
func NewConsumerGroupQueue(name string) *ConsumerGroupQueue {
	return &ConsumerGroupQueue{
		Name: name,
	}
}

func (c *ConsumerGroupQueue) All() []*Consumer {
	consumers := []*Consumer{}
	cur := c.Consumers.head

	for cur != nil {
		consumers = append(consumers, cur.Value)
		cur = cur.Next
	}

	return consumers
}

func (c *ConsumerGroupQueue) Add(con *Consumer) error {
	c.Consumers.Add(con)
	return nil
}

func (c *ConsumerGroupQueue) Remove(con *Consumer) error {
	prevNode := c.Consumers.head
	if prevNode == nil {
		return nil
	}
	if prevNode.Value == con {
		c.Consumers.PopFront()
	}
	curNode := prevNode.Next
	for curNode != nil {
		if curNode.Value == con {
			prevNode.Next = curNode.Next
			return nil
		}
		prevNode = curNode
		curNode = prevNode.Next
	}
	return nil
}

func (c *ConsumerGroupQueue) ProcessMessage(m *pb.Message) error {
	// TODO: Bug trying to publish to removed consumers,
	// likely not cleaning up pointers on removal
	cur := c.Consumers.head
	var prev *Node
	for cur != nil {
		if containsString(cur.Value.Topics, m.Topic) {
			val := cur.Value
			if prev != nil {
				prev.Next = cur.Next
				c.Consumers.tail = cur
				cur.Next = nil
			} else if cur.Next != nil {
				c.Consumers.head = cur.Next
				c.Consumers.tail.Next = cur
				c.Consumers.tail = cur
				cur.Next = nil
			}
			return val.Consume(m)
		}
		prev = cur
		cur = cur.Next
	}
	return nil
}

func containsString(l []string, t string) bool {
	for _, i := range l {
		if i == t {
			return true
		}
	}
	return false
}
