package tada

import (
	"fmt"
	"testing"
)

func TestUsage(t *testing.T) {
	df := NewDataFrame(
		[]interface{}{[]float64{1, 2, 3}, []float64{4, 5, 6}}, []string{"foo", "foo", "baz"}).
		SetCols([]string{"qux", "quux"})
	g := df.GroupBy().Sum()
	fmt.Print(g.values[1])
}
