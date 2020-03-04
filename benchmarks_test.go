package tada

import (
	"math/rand"
	"testing"
)

// func Benchmark_ReadCSV(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		ImportCSV("test_files/big.csv", nil)
// 	}
// }

// func Benchmark_ConvertToString(b *testing.B) {
// 	n := 100000
// 	f := make([]float64, n)
// 	for i := range f {
// 		f[i] = rand.Float64()
// 	}
// 	nulls := make([]bool, n)
// 	for i := range nulls {
// 		nulls[i] = false
// 	}
// 	vc := &valueContainer{slice: f, isNull: nulls}
// 	for i := 0; i < b.N; i++ {
// 		vc.string()
// 	}
// }

// func Benchmark_GroupBy(b *testing.B) {
// 	n := 10000
// 	f := make([]float64, n)
// 	for i := range f {
// 		f[i] = rand.Float64()
// 	}
// 	s := make([]string, n)
// 	for i := range f {
// 		s[i] = "foo"
// 	}
// 	df := NewDataFrame(nil, s, f).SetLabelNames([]string{"foo", "bar"})
// 	for i := 0; i < b.N; i++ {
// 		df.GroupBy("foo", "bar")
// 	}
// }

func Benchmark_concatenateStringLabels(b *testing.B) {
	n := 10000
	f := make([]float64, n)
	for i := range f {
		f[i] = rand.Float64()
	}
	s := make([]int, n)
	for i := range f {
		s[i] = rand.Int()
	}
	vcs := []*valueContainer{
		&valueContainer{slice: f},
		&valueContainer{slice: s},
	}
	for i := 0; i < b.N; i++ {
		concatenateLabelsToStrings(vcs)
	}
}
