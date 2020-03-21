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

func ExampleSeries_Lookup() {
	s := NewSeries([]float64{1, 2}, []int{0, 1}).SetName("foo")
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := NewSeries([]float64{4, 5}, []int{0, 10})
	fmt.Println("--Series to lookup--")
	fmt.Println(s2)

	fmt.Println("--result--")
	fmt.Println(s.Lookup(s2))
	// Output:
	// --original Series--
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// --Series to lookup--
	// +----++---+
	// | -  || 0 |
	// |----||---|
	// |  0 || 4 |
	// | 10 || 5 |
	// +----++---+
	//
	// --result--
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   4 |
	// | 1 || n/a |
	// +---++-----+
}

func ExampleSeries_LookupAdvanced() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "bar"}, []int{0, 1})
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := NewSeries([]float64{4, 5}, []int{0, 10}, []string{"baz", "bar"})
	fmt.Println("--Series to lookup--")
	fmt.Println(s2)

	fmt.Println("--result--")
	fmt.Println(s.LookupAdvanced(s2, "inner", []string{"*1"}, []string{"*0"}))
	// Output:
	// --original Series--
	// +-----+---++---+
	// |  -  | - || 0 |
	// |-----|---||---|
	// | foo | 0 || 1 |
	// | bar | 1 || 2 |
	// +-----+---++---+
	//
	// --Series to lookup--
	// +----+-----++---+
	// | -  |  -  || 0 |
	// |----|-----||---|
	// |  0 | baz || 4 |
	// | 10 | bar || 5 |
	// +----+-----++---+
	//
	// --result--
	// +-----+---++---+
	// |  -  | - || 0 |
	// |-----|---||---|
	// | foo | 0 || 4 |
	// +-----+---++---+
}

func ExampleSeries_Merge() {
	s := NewSeries([]float64{1, 2}, []int{0, 1}).SetName("foo")
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := NewSeries([]float64{4, 5}, []int{0, 10}).SetName("bar")
	fmt.Println("--Series to merge--")
	fmt.Println(s2)

	fmt.Println("--result--")
	fmt.Println(s.Merge(s2))
	// Output:
	// --original Series--
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// --Series to merge--
	// +----++-----+
	// | -  || bar |
	// |----||-----|
	// |  0 ||   4 |
	// | 10 ||   5 |
	// +----++-----+
	//
	// --result--
	// +---++-----+-----+
	// | - || foo | bar |
	// |---||-----|-----|
	// | 0 ||   1 |   4 |
	// | 1 ||   2 | n/a |
	// +---++-----+-----+
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
