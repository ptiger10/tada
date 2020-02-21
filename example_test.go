package tada

import (
	"fmt"
	"math"
)

func ExampleDataFrame() {
	df := NewDataFrame([]interface{}{[]float64{1, 2}, []string{"foo", "bar"}}).SetCols([]string{"a", "b"}).SetName("qux")
	fmt.Println(df)
	// Output:
	// +---+---+-----+
	// |   | A |  B  |
	// +---+---+-----+
	// | 0 | 1 | foo |
	// | 1 | 2 | bar |
	// +---+---+-----+
	// name: qux
}

func ExampleDataFrame_null() {
	df := NewDataFrame([]interface{}{[]float64{math.NaN(), 2}, []string{"foo", ""}}).SetCols([]string{"a", "b"}).SetName("qux")
	fmt.Println(df)
	// Output:
	// +---+-----+-----+
	// |   |  A  |  B  |
	// +---+-----+-----+
	// | 0 | n/a | foo |
	// | 1 |   2 | n/a |
	// +---+-----+-----+
	// name: qux
}

func ExampleSeries_null() {
	s := NewSeries([]string{"foo", ""})
	fmt.Println(s)
	// Output:
	// +---+-----+
	// |   |  0  |
	// +---+-----+
	// | 0 | foo |
	// | 1 | n/a |
	// +---+-----+
}

func ExampleSeries() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "foo"}).SetName("A")
	fmt.Println(s)
	// Output:
	// +-----+---+
	// |     | A |
	// +-----+---+
	// | foo | 1 |
	// |     | 2 |
	// +-----+---+
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
