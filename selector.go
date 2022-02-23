package legs

import (
	"github.com/ipld/go-ipld-prime"
	"github.com/ipld/go-ipld-prime/datamodel"
	"github.com/ipld/go-ipld-prime/fluent"
	basicnode "github.com/ipld/go-ipld-prime/node/basic"
	"github.com/ipld/go-ipld-prime/traversal/selector"
	selectorbuilder "github.com/ipld/go-ipld-prime/traversal/selector/builder"
)

// ExploreRecursiveWithStop builds a selector that recursively syncs a DAG
// until the link stopLnk is seen. It prevents from having to sync DAGs from
// scratch with every update.
func ExploreRecursiveWithStop(limit selector.RecursionLimit, sequence selectorbuilder.SelectorSpec, stopLnk ipld.Link) ipld.Node {
	return ExploreRecursiveWithStopNode(limit, sequence.Node(), stopLnk)
}

// ExploreRecursiveWithStopNode builds a selector that recursively syncs a DAG
// until the link stopLnk is seen. It prevents from having to sync DAGs from
// scratch with every update.
func ExploreRecursiveWithStopNode(limit selector.RecursionLimit, sequence ipld.Node, stopLnk ipld.Link) ipld.Node {
	if sequence == nil {
		log.Debug("No selector sequence specified; using default explore all with recursive edge.")
		np := basicnode.Prototype__Any{}
		ssb := selectorbuilder.NewSelectorSpecBuilder(np)
		sequence = ssb.ExploreAll(ssb.ExploreRecursiveEdge()).Node()
	}
	np := basicnode.Prototype__Map{}
	return fluent.MustBuildMap(np, 1, func(na fluent.MapAssembler) {
		// RecursionLimit
		na.AssembleEntry(selector.SelectorKey_ExploreRecursive).CreateMap(3, func(na fluent.MapAssembler) {
			na.AssembleEntry(selector.SelectorKey_Limit).CreateMap(1, func(na fluent.MapAssembler) {
				switch limit.Mode() {
				case selector.RecursionLimit_Depth:
					na.AssembleEntry(selector.SelectorKey_LimitDepth).AssignInt(limit.Depth())
				case selector.RecursionLimit_None:
					na.AssembleEntry(selector.SelectorKey_LimitNone).CreateMap(0, func(na fluent.MapAssembler) {})
				default:
					panic("Unsupported recursion limit type")
				}
			})
			// Sequence
			na.AssembleEntry(selector.SelectorKey_Sequence).AssignNode(sequence)

			// Stop condition
			if stopLnk != nil {
				cond := fluent.MustBuildMap(basicnode.Prototype__Map{}, 1, func(na fluent.MapAssembler) {
					na.AssembleEntry(string(selector.ConditionMode_Link)).AssignLink(stopLnk)
				})
				na.AssembleEntry(selector.SelectorKey_StopAt).AssignNode(cond)
			}
		})
	})
}

// getStopNode will try to return the stop node from a recursive selector.
func getStopNode(selNode datamodel.Node) (datamodel.Link, bool) {
	if selNode == nil {
		return nil, false
	}
	selNode, err := selNode.LookupByString(selector.SelectorKey_ExploreRecursive)
	if err != nil {
		return nil, false
	}
	selNode, err = selNode.LookupByString(selector.SelectorKey_StopAt)
	if err != nil {
		return nil, false
	}
	selNode, err = selNode.LookupByString(string(selector.ConditionMode_Link))
	if err != nil {
		return nil, false
	}
	stopNodeLink, err := selNode.AsLink()
	return stopNodeLink, err == nil
}

// LegSelector is a convenient function that returns the selector
// used by leg subscribers
//
// LegSelector is a "recurse all" selector that provides conditions
// to stop the traversal at a specific link (stopAt).
func LegSelector(limit selector.RecursionLimit, stopLnk ipld.Link) ipld.Node {
	np := basicnode.Prototype__Any{}
	ssb := selectorbuilder.NewSelectorSpecBuilder(np)
	return ExploreRecursiveWithStop(
		limit,
		ssb.ExploreAll(ssb.ExploreRecursiveEdge()),
		stopLnk)
}
