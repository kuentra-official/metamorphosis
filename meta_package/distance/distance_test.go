package distance

import (
	"fmt"
	"kuentra-official/metamorphosis/meta_package/distance/asm"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/require"
)

var vectorTable = []struct {
	name          string
	x             []float32
	y             []float32
	wantDot       float32
	wantEuclidean float32
}{
	{"Zero", []float32{0, 0, 0}, []float32{0, 0, 0}, 0, 0},
	{"One", []float32{1, 1}, []float32{1, 1}, 2, 0},
	{"Two", []float32{1, 2, 3}, []float32{4, 5, 6}, 32, 27},
	{"Negative", []float32{-1, -2, -3}, []float32{-4, -5, -6}, 32, 27},
	{"Mixed", []float32{-1, 2, 3}, []float32{4, -5, 6}, 4, 83},
}

func TestPureDotProduct(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := dotProductPureGo(tt.x, tt.y)
			require.Equal(t, tt.wantDot, got)
		})
	}
}

func TestASMdotProduct(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := asm.Dot(tt.x, tt.y)
			require.Equal(t, tt.wantDot, got)
		})
	}
}

func TestPureSquaredEuclidean(t *testing.T) {
	for _, tt := range vectorTable {
		t.Run(tt.name, func(t *testing.T) {
			got := squaredEuclideanDistancePureGo(tt.x, tt.y)
			require.Equal(t, tt.wantEuclidean, got)
		})
	}
}

func TestASMSquaredEuclidean(t *testing.T) {
	x := []float32{1, 2, 3}
	y := []float32{4, 5, 6}
	got := asm.SquaredEuclideanDistance(x, y)
	want := float32(27)
	require.Equal(t, want, got)
}

func TestHammingDistance(t *testing.T) {
	x := []uint64{0b1001, 0b1}
	y := []uint64{0b1101, 0b0}
	dist := hammingDistance(x, y)
	require.Equal(t, float32(2), dist)
}

func TestJaccardDistance(t *testing.T) {
	x := []uint64{0b1001, 0b1}
	y := []uint64{0b1101, 0b0}
	dist := jaccardDistance(x, y)
	require.Equal(t, float32(0.5), dist)
	x = []uint64{0b0, 0b0}
	y = []uint64{0b0, 0b0}
	dist = jaccardDistance(x, y)
	require.Equal(t, float32(0.0), dist)
}

func TestHaversineDistance(t *testing.T) {
	// Airport example from
	// https://scikit-learn.org/stable/modules/generated/sklearn.metrics.pairwise.haversine_distances.html
	x := []float32{-34.83333, -58.5166646}
	y := []float32{49.0083899664, 2.53844117956}
	dist := haversineDistance(x, y)
	dist /= 1000 // in km
	require.InDelta(t, 11099.54, dist, 0.01)
}

// ---------------------------

var benchTable = []struct {
	name string
	fn   func([]float32, []float32) float32
}{
	{"PureDotProduct", dotProductPureGo},
	{"ASMDotProduct", asm.Dot},
	{"PureSquaredEuclidean", squaredEuclideanDistancePureGo},
	{"ASMSquaredEuclidean", asm.SquaredEuclideanDistance},
}

var bechSizes = []int{768, 1536}

func randVector(size int) []float32 {
	vector := make([]float32, size)
	for i := 0; i < size; i++ {
		vector[i] = rand.Float32()
	}
	return vector
}

func BenchmarkDist(b *testing.B) {
	for _, size := range bechSizes {
		for _, bench := range benchTable {
			x := randVector(size)
			y := randVector(size)
			runName := fmt.Sprintf("%s-%d", bench.name, size)
			b.Run(runName, func(b *testing.B) {
				for i := 0; i < b.N; i++ {
					bench.fn(x, y)
				}
			})
		}
	}
}
