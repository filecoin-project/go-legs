package legs

import (
	"testing"

	"github.com/ipfs/go-cid"
	cidlink "github.com/ipld/go-ipld-prime/linking/cid"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	"github.com/stretchr/testify/require"
)

func TestGetStopNode(t *testing.T) {
	c, err := cid.V0Builder{}.Sum([]byte("hi"))
	require.NoError(t, err)
	stopNode := cidlink.Link{Cid: c}
	sel := ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), nil, stopNode)
	actualStopNode, err := getStopNode(sel)
	require.NoError(t, err)
	require.Equal(t, stopNode, actualStopNode)
}

func TestGetStopNodeWhenNil(t *testing.T) {
	sel := ExploreRecursiveWithStopNode(selector.RecursionLimitNone(), nil, nil)
	_, err := getStopNode(sel)
	require.Error(t, err, "We shouldn't get a stop node out if none was set")
}
