package tada

import (
	"fmt"
	"testing"
)

// func TestUsage(t *testing.T) {
// 	df := NewDataFrame(
// 		[]interface{}{[]float64{1, 2, 3}, []float64{4, 5, 6}}, []string{"foo", "foo", "baz"}).
// 		SetCols([]string{"qux", "quux"})
// 	g := df.GroupBy().Sum()
// 	fmt.Print(g.values[1])
// }

// func TestMockCSV(t *testing.T) {
// 	c := [][]string{{"qux", "corge", "waldo"},
// 		{"1", "dog", "2/15/20"},
// 		{"1", "dog", "2/15/20"}}
// 	var b strings.Builder
// 	WriteMockCSV(c, &b, nil)
// }

func TestAlign(t *testing.T) {
	s := NewSeries([]float64{1, 2, 3}, []string{"foo", "foo", "bar"})
	fmt.Println(s.GroupBy().Align().Sum())
}
