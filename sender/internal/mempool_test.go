package internal_test

import (
	"testing"

	"github.com/anjankow/message-sender/sender/internal"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestMemPool(t *testing.T) {
	p := internal.NewMemPool()
	require.NotNil(t, p)

	// Get 2 empty buffers from the pool
	b1 := p.GetBuffer()
	require.NotNil(t, b1)
	assert.Zero(t, b1.Len())
	assert.Zero(t, b1.Cap())

	b2 := p.GetBuffer()
	require.NotNil(t, b2)
	assert.Zero(t, b2.Len())
	assert.Zero(t, b2.Cap())

	// Write to b1 and verify b2 is still empty
	b1.WriteString("test")
	assert.Equal(t, 4, b1.Len())
	assert.Zero(t, b2.Len())

	// Return b1 to the pool
	p.ReleaseBuffer(b1)
	assert.Zero(t, b1.Len())

	// Get a new buffer from the pool and verify it's empty
	b3 := p.GetBuffer()
	assert.Zero(t, b3.Len())

	// Verify b3 is reused after b1
	b3.WriteString("test again")
	assert.Equal(t, b1, b3)
}
