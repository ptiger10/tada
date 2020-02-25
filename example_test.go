package tada

import (
	"fmt"
	"math"
)

func ExampleDataFrame() {
	df := NewDataFrame([]interface{}{[]float64{1, 2}, []string{"foo", "bar"}}).
		SetColNames([]string{"a", "b"}).
		SetLabelNames([]string{"baz"}).
		SetName("qux")
	fmt.Println(df)
	// Output:
	// +-------+---+-----+
	// | -BAZ- | A |  B  |
	// +-------+---+-----+
	// |     0 | 1 | foo |
	// |     1 | 2 | bar |
	// +-------+---+-----+
	// name: qux
}

func ExampleDataFrame_excess_rows() {
	df := NewDataFrame([]interface{}{
		[]float64{1, 2, 3, 4, 5, 6, 7, 8}}).SetColNames([]string{"A"})
	archive := optionMaxRows
	SetOptionMaxRows(6)
	fmt.Println(df)
	SetOptionMaxRows(archive)
	// Output:
	// +-----+-----+
	// | --  |  A  |
	// +-----+-----+
	// |   0 |   1 |
	// |   1 |   2 |
	// |   2 |   3 |
	// | ... | ... |
	// |   5 |   6 |
	// |   6 |   7 |
	// |   7 |   8 |
	// +-----+-----+
}

func ExampleDataFrame_excess_cols() {
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []float64{3, 4}, []float64{5, 6},
		[]float64{3, 4}, []float64{5, 6},
	}).SetColNames([]string{"A", "B", "C", "D", "E"})
	archive := optionMaxColumns
	SetOptionMaxColumns(2)
	fmt.Println(df)
	SetOptionMaxColumns(archive)
	// Output:
	// +----+---+-----+---+
	// | -- | A |  .  | E |
	// +----+---+-----+---+
	// |  0 | 1 | ... | 5 |
	// |  1 | 2 |     | 6 |
	// +----+---+-----+---+
}

func ExampleDataFrame_null() {
	df := NewDataFrame([]interface{}{[]float64{math.NaN(), 2}, []string{"foo", ""}}).SetColNames([]string{"a", "b"}).SetName("qux")
	fmt.Println(df)
	// Output:
	// +----+-----+-----+
	// | -- |  A  |  B  |
	// +----+-----+-----+
	// |  0 | n/a | foo |
	// |  1 |   2 | n/a |
	// +----+-----+-----+
	// name: qux
}

func ExampleSeries_null() {
	s := NewSeries([]string{"foo", ""})
	fmt.Println(s)
	// Output:
	// +----+-----+-----+
	// | -- |  A  |  B  |
	// +----+-----+-----+
	// |  0 | n/a | foo |
	// |  1 |   2 | n/a |
	// +----+-----+-----+
}

func ExampleSeries() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "foo"}).SetName("A")
	fmt.Println(s)
	// Output:
	// +----+-----+
	// | -- |  0  |
	// +----+-----+
	// |  0 | foo |
	// |  1 | n/a |
	// +----+-----+
}

func ExampleGroupedSeries() {
	g := NewSeries([]float64{1, 2, 3, 4}, []string{"foo", "foo", "bar", "bar"}).GroupBy("*0")
	fmt.Println(g)
	// Output:
	// Groups: foo,bar
}
func ExampleGroupedSeries_multiple() {
	g := NewSeries([]float64{1, 2}, []string{"foo", "foo"}, []string{"bar", "bar"}).GroupBy("*0", "*1")
	fmt.Println(g)
	// Output:
	// Groups: foo|bar
}
