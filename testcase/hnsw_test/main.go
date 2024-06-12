package main

import (
	"fmt"
	"kuentra-official/metamorphosis/meta_index/hnsw"
)

func main() {
	metaGraph := hnsw.NewMetaGraph[hnsw.MetaVector]()

	metaGraph.Add(
		hnsw.MakeVector("a", []float32{1, 0, -5}),
		hnsw.MakeVector("b", []float32{2, 3, -5}),
		hnsw.MakeVector("c", []float32{-4, 2, -5}),
		hnsw.MakeVector("d", []float32{3, 9, -5}),
		hnsw.MakeVector("e", []float32{1, 1, -5}),
	)
	fmt.Printf("Nearest: %v\n", metaGraph.MetaSearch(
		[]float32{1, 0, -4},
		3,
	))
}
