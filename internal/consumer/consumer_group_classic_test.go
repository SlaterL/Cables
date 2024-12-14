package consumer

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestListEq(t *testing.T) {
	t.Parallel()

	l1 := []string{
		"first",
		"second",
		"third",
	}
	l2 := []string{
		"first",
		"second",
		"third",
	}
	assert.Equal(t, l1, l2)
}

func TestListEq_Fail(t *testing.T) {
	t.Parallel()

	l1 := []string{
		"first",
		"second",
		"third",
	}
	l2 := []string{
		"first",
		"second",
	}
	assert.NotEqual(t, l1, l2)
}

func TestConsumerClassic_Add(t *testing.T) {
	t.Parallel()

	cg := NewConsumerGroupClassic("testCG", []string{"t1", "t2"})
	cPass := &Consumer{
		Name:   "testCPass",
		Topics: []string{"t1", "t2"},
	}
	cFail := &Consumer{
		Name:   "testCFail",
		Topics: []string{"t3", "t4"},
	}
	errAdd := cg.Add(cPass)
	require.NoError(t, errAdd)

	errAdd = cg.Add(cFail)
	require.Error(t, errAdd)
}

func TestConsumerClassic_Remove(t *testing.T) {
	t.Parallel()

	cg := NewConsumerGroupClassic("testCG", []string{"t1", "t2"})
	c1 := &Consumer{
		Name:   "testC1",
		Topics: []string{"t1", "t2"},
	}
	c2 := &Consumer{
		Name:   "testC2",
		Topics: []string{"t1", "t2"},
	}
	errAdd := cg.Add(c1)
	require.NoError(t, errAdd)
	errAdd = cg.Add(c2)
	require.NoError(t, errAdd)

	errRemove := cg.Remove(c1)
	require.NoError(t, errRemove)
	assert.Equal(t, "testC2", cg.Consumers[0].Name)

	errRemove = cg.Remove(c2)
	require.NoError(t, errRemove)
	assert.Empty(t, cg.Consumers)

	errRemove = cg.Remove(c2)
	require.Error(t, errRemove)
}
