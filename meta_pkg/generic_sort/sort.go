package generic_sort

import (
	"slices"

	"golang.org/x/exp/constraints"
)

func Sort[S ~[]E, E constraints.Ordered](x S, k int) {
	k = min(k, len(x))
	if k > 0 {
		floydRivest(x, 0, len(x)-1, k-1) // 0-indexed
		slices.Sort(x[:k-1])
	}
}

func SortFunc[S ~[]E, E any](x S, k int, cmp func(E, E) int) {
	k = min(k, len(x))
	if k > 0 {
		floydRivestFunc(x, 0, len(x)-1, k-1, cmp)
		slices.SortFunc(x[:k-1], cmp)
	}
}
