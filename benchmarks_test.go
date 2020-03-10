package tada

import (
	"math/rand"
	"testing"
	"time"
)

func makeBenchmarkDF() *DataFrame {
	n := 100000
	s := make([]string, n)
	for i := range s {
		if rand.Float64() > .5 {
			s[i] = ""
		} else {
			s[i] = "foo"
		}
	}
	return NewDataFrame([]interface{}{s})
}

var benchmarkDF = makeBenchmarkDF()

// func Benchmark_DropNull(b *testing.B) {
// 	for i := 0; i < b.N; i++ {
// 		benchmarkDF.DropNull()
// 	}
// }

func Benchmark_ReadCSV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ImportCSV("test_files/big.csv", nil)
	}
}

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

// func Benchmark_string(b *testing.B) {
// 	n := 10000
// 	f := make([]float64, n)
// 	for i := range f {
// 		f[i] = rand.Float64()
// 	}
// 	i := make([]int, n)
// 	for k := range i {
// 		i[k] = rand.Int()
// 	}
// 	d := make([]time.Time, n)
// 	for i := range d {
// 		d[i] = time.Date(rand.Int(), 1, 1, 0, 0, 0, 0, time.UTC)
// 	}
// 	// vcs := []*valueContainer{
// 	// &valueContainer{slice: f},
// 	// &valueContainer{slice: s},
// 	// 	&valueContainer{slice: d},
// 	// }
// 	// vc := &valueContainer{slice: d}
// 	// vc := &valueContainer{slice: i}
// 	vc := &valueContainer{slice: f}
// 	for i := 0; i < b.N; i++ {
// 		vc.string()
// 	}

func Benchmark_concatenateStringLabelsBytes(b *testing.B) {
	n := 1000000
	f := make([]float64, n)
	for i := range f {
		f[i] = rand.Float64()
	}
	i := make([]int, n)
	for a := range i {
		i[a] = rand.Int()
	}
	d := make([]time.Time, n)
	for i := range d {
		d[i] = time.Date(rand.Int(), 1, 1, 0, 0, 0, 0, time.UTC)
	}
	s := make([]string, n)
	for i := range s {
		s[i] = "foo"
	}

	vc := &valueContainer{slice: d}
	vc.resetCache()
	vcs := []*valueContainer{
		vc,
	}
	for i := 0; i < b.N; i++ {
		concatenateLabelsToStringsBytes(vcs)
	}
}
