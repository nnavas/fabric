package gossip

import (
	"testing"

	pg "github.com/hyperledger/fabric-protos-go-apiv2/gossip"
	"github.com/hyperledger/fabric/gossip/comm"
	"github.com/stretchr/testify/require"
)

func TestSpanningTreeStateAdoptsBestParent(t *testing.T) {
	state := newSpanningTreeState()

	first := &pg.GossipMessage{
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId: []byte("root"),
				RootIncNum: 1,
				RootSeqNum: 1,
				Distance:  1,
				SenderPkiId: []byte("peer-a"),
				PathCost: 10,
			},
		},
	}

	require.True(t, state.handle(first, []byte("peer-a")))
	require.Equal(t, []byte("peer-a"), state.parent)
	require.Equal(t, uint32(10), state.bestPathCost)

	better := &pg.GossipMessage{
		Content: &pg.GossipMessage_SpanningTree{
			SpanningTree: &pg.SpanningTreeMsg{
				RootPkiId: []byte("root"),
				RootIncNum: 1,
				RootSeqNum: 1,
				Distance:  0,
				SenderPkiId: []byte("peer-b"),
				PathCost: 1,
			},
		},
	}

	require.True(t, state.handle(better, []byte("peer-b")))
	require.Equal(t, []byte("peer-b"), state.parent)
	require.Equal(t, uint32(1), state.bestPathCost)
}

func TestSpanningTreeStateSelectsChildPeersForData(t *testing.T) {
	state := newSpanningTreeState()
	state.children["peer-a"] = struct{}{}
	state.children["peer-b"] = struct{}{}

	peers := []*comm.RemotePeer{
		{PKIID: []byte("peer-a")},
		{PKIID: []byte("peer-c")},
		{PKIID: []byte("peer-b")},
	}

	selected := state.selectPeers(peers)
	require.Len(t, selected, 2)
	var selectedPKIIDs [][]byte
	for _, peer := range selected {
		selectedPKIIDs = append(selectedPKIIDs, peer.PKIID)
	}
	require.ElementsMatch(t, [][]byte{[]byte("peer-a"), []byte("peer-b")}, selectedPKIIDs)
}
