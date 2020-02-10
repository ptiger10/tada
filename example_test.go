package tada

import "fmt"

func ExampleDataFrame() {
	df := NewDataFrame([]interface{}{[]float64{1, 2}, []string{"foo", "bar"}}).SetCols([]string{"a", "b"}).SetName("qux")
	fmt.Println(df)
	// Output:
	// +---+---+-----+
	// | 0 | A |  B  |
	// +---+---+-----+
	// | 0 | 1 | foo |
	// | 1 | 2 | bar |
	// +---+---+-----+
	// name: qux
}

func ExampleSeries() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "foo"}).SetName("A")
	fmt.Println(s)
	// Output:
	// +-----+---+
	// |  0  | A |
	// +-----+---+
	// | foo | 1 |
	// |     | 2 |
	// +-----+---+
}
