package consumer

type ConsumerGroup struct {
	Name      string
	Consumers ConsumerQueue
}

// Returns a new ConsumerGroup. Only one consumer per
// group will recieve a copy of a published message.
// This allows for multiple instances of a handler to
// run without duplication of work.
func NewConsumerGroup(name string) *ConsumerGroup {
	return &ConsumerGroup{
		Name: name,
	}
}

func (c *ConsumerGroup) AllConsumers() []*Consumer {
	consumers := []*Consumer{}
	cur := c.Consumers.head

	for cur != nil {
		consumers = append(consumers, cur.Value)
		cur = cur.Next
	}

	return consumers
}

func (c *ConsumerGroup) AddConsumer(con *Consumer) {
	c.Consumers.Add(con)
}

func (c *ConsumerGroup) RemoveConsumer(con *Consumer) {
	prevNode := c.Consumers.head
	if prevNode == nil {
		return
	}
	if prevNode.Value == con {
		c.Consumers.PopFront()
	}
	curNode := prevNode.Next
	for curNode != nil {
		if curNode.Value == con {
			prevNode.Next = curNode.Next
			return
		}
		prevNode = curNode
		curNode = prevNode.Next
	}
}

func (c *ConsumerGroup) ConsumerOfTopic(topic string) *Consumer {
	cur := c.Consumers.head
	var prev *Node
	for cur != nil {
		if containsString(cur.Value.Topics, topic) {
			val := cur.Value
			if prev != nil {
				prev.Next = cur.Next
				c.Consumers.tail = cur
			} else if cur.Next != nil {
				c.Consumers.head = cur.Next
				c.Consumers.tail.Next = cur
				c.Consumers.tail = cur
			}
			return val
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
