package consumer

type Node struct {
	Value *Consumer
	Next  *Node
}

type ConsumerQueue struct {
	head *Node
	tail *Node
}

func NewConsumerQueue(val *Consumer) *ConsumerQueue {
	n := &Node{Value: val}

	return &ConsumerQueue{
		head: n,
		tail: n,
	}
}

func (q *ConsumerQueue) Length() int {
	if q.head == nil {
		return 0
	}
	l := 1

	cur := q.head
	for cur.Next != nil {
		l += 1
		cur = cur.Next
	}
	return l
}

func (q *ConsumerQueue) Add(val *Consumer) {
	n := &Node{Value: val}
	if q.tail == nil {
		q.head = n
		q.tail = n
		return
	}
	q.tail.Next = n
	q.tail = n
}

func (q *ConsumerQueue) PopFront() *Consumer {
	if q.head == q.tail {
		q.tail = nil
	}
	val := q.head.Value
	q.head = q.head.Next

	return val
}
