package tada

import (
	"errors"
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
	// +-----++---+-----+
	// | baz || a |  b  |
	// |-----||---|-----|
	// |   0 || 1 | foo |
	// |   1 || 2 | bar |
	// +-----++---+-----+
	// name: qux
}

func ExampleSeries_nested() {
	s := NewSeries([][]string{{"foo", "bar"}, {"baz"}, {}}).
		SetName("a")
	fmt.Println(s)
	// Output:
	// +---++-----------+
	// | - ||     a     |
	// |---||-----------|
	// | 0 || [foo bar] |
	// | 1 ||     [baz] |
	// | 2 ||       n/a |
	// +---++-----------+
}

func ExampleDataFrame_excess_rows() {
	df := NewDataFrame([]interface{}{
		[]float64{1, 2, 3, 4, 5, 6, 7, 8}}).SetColNames([]string{"A"})
	archive := optionMaxRows
	PrintOptionMaxRows(6)
	fmt.Println(df)
	PrintOptionMaxRows(archive)
	// Output:
	// +-----++-----+
	// |  -  ||  A  |
	// |-----||-----|
	// |   0 ||   1 |
	// |   1 ||   2 |
	// |   2 ||   3 |
	// | ... || ... |
	// |   5 ||   6 |
	// |   6 ||   7 |
	// |   7 ||   8 |
	// +-----++-----+
}

func ExampleDataFrame_excess_cols() {
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []float64{3, 4}, []float64{5, 6},
		[]float64{3, 4}, []float64{5, 6},
	}).SetColNames([]string{"A", "B", "C", "D", "E"})
	archive := optionMaxColumns
	PrintOptionMaxColumns(2)
	fmt.Println(df)
	PrintOptionMaxColumns(archive)
	// Output:
	// +---++---+-----+---+
	// | - || A | ... | E |
	// |---||---|-----|---|
	// | 0 || 1 | ... | 5 |
	// | 1 || 2 |     | 6 |
	// +---++---+-----+---+
}

func ExampleDataFrame_null() {
	df := NewDataFrame([]interface{}{[]float64{math.NaN(), 2}, []string{"foo", ""}}).SetColNames([]string{"a", "b"}).SetName("qux")
	fmt.Println(df)
	// Output:
	// +---++-----+-----+
	// | - ||  a  |  b  |
	// |---||-----|-----|
	// | 0 || n/a | foo |
	// | 1 ||   2 | n/a |
	// +---++-----+-----+
	// name: qux
}

func ExampleSeries_withError() {
	s := &Series{err: errors.New("foo")}
	fmt.Println(s)
	// Output:
	// Error: foo
}

func ExampleDataFrame_withError() {
	df := &DataFrame{err: errors.New("foo")}
	fmt.Println(df)
	// Output:
	// Error: foo
}

func ExampleSeries_null() {
	s := NewSeries([]string{"foo", ""})
	fmt.Println(s)
	// Output:
	// +---++-----+
	// | - ||  0  |
	// |---||-----|
	// | 0 || foo |
	// | 1 || n/a |
	// +---++-----+
}

func ExampleSeries() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "foo"}).SetName("A")
	fmt.Println(s)
	// Output:
	// +-----++---+
	// |  -  || A |
	// |-----||---|
	// | foo || 1 |
	// |     || 2 |
	// +-----++---+
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
