package tada

import (
	"errors"
	"fmt"
)

func ExampleSeries() {
	s := NewSeries([]float64{1, 2}).SetName("foo")
	fmt.Println(s)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
}

func ExampleSeries_withError() {
	s := &Series{err: errors.New("foo")}
	fmt.Println(s)
	// Output:
	// Error: foo
}

func ExampleSeries_withNullValues() {
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

func ExampleSeries_nestedSlice() {
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

func ExampleGroupedSeries() {
	s := NewSeries([]float64{1, 2, 3, 4}, []string{"foo", "foo", "bar", "bar"})
	fmt.Println(s.GroupBy("*0"))
	// Output:
	// Groups: foo,bar
}
func ExampleGroupedSeries_compoundGroup() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "foo"}, []string{"bar", "bar"})
	fmt.Println(s.GroupBy("*0", "*1"))
	// Output:
	// Groups: foo|bar
}
