package tada_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/ptiger10/tada"
)

func ExampleNewReader() {
	data := "foo, bar\n baz, qux\n corge, fred"
	df, _ := tada.NewReader(strings.NewReader(data)).ReadDF()
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleNewReader_noHeaders() {
	data := "foo, bar\n baz, qux\n corge, fred"
	df, _ := tada.NewReader(strings.NewReader(data)).ReadDF(tada.WithHeaders(0))
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||   0   |  1   |
	// |---||-------|------|
	// | 0 ||   foo |  bar |
	// | 1 ||   baz |  qux |
	// | 2 || corge | fred |
	// +---++-------+------+
}

func ExampleNewReader_multipleHeaders() {
	data := "foo, bar\n baz, qux\n corge, fred"
	df, _ := tada.NewReader(strings.NewReader(data)).ReadDF(tada.WithHeaders(2))
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// |   ||  foo  | bar  |
	// | - ||  baz  | qux  |
	// |---||-------|------|
	// | 0 || corge | fred |
	// +---++-------+------+
}

func ExampleNewReader_multipleHeadersWithLabels() {
	data := ", foo, bar\n labels, baz, qux\n 1, corge, fred"
	df, _ := tada.NewReader(strings.NewReader(data)).ReadDF(tada.WithHeaders(2), tada.WithLabels(1))
	fmt.Println(df)
	// Output:
	// +--------++-------+------+
	// |        ||  foo  | bar  |
	// | labels ||  baz  | qux  |
	// |--------||-------|------|
	// |      1 || corge | fred |
	// +--------++-------+------+
}

func ExampleNewReader_withLabels() {
	data := `foo, bar
	baz, qux
	corge, fred`
	df, _ := tada.NewReader(strings.NewReader(data)).ReadDF(tada.WithLabels(1))
	fmt.Println(df)
	// Output:
	// +-------++------+
	// |  foo  || bar  |
	// |-------||------|
	// |   baz ||  qux |
	// | corge || fred |
	// +-------++------+
}

func ExampleNewReader_delimiter() {
	data := `foo|bar
	baz|qux
	corge|fred`
	df, _ := tada.NewReader(strings.NewReader(data)).ReadDF(tada.WithDelimiter('|'))
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleNewCSVReader() {
	data := [][]string{
		{"foo", "bar"},
		{"baz", "qux"},
		{"corge", "fred"},
	}
	df, _ := tada.NewCSVReader(data).ReadDF()
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleNewCSVReader_colsAsMajorDimension() {
	data := [][]string{
		{"foo", "bar"},
		{"baz", "qux"},
		{"corge", "fred"},
	}
	df, _ := tada.NewCSVReader(data).ReadDF(tada.ByColumn())
	fmt.Println(df)
	// Output:
	// +---++-----+-----+-------+
	// | - || foo | baz | corge |
	// |---||-----|-----|-------|
	// | 0 || bar | qux |  fred |
	// +---++-----+-----+-------+
}

func ExampleDataFrame() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"baz", "qux"}},
	).SetName("foo")
	fmt.Println(df)
	// Output:
	// +---++---+-----+
	// | - || 0 |  1  |
	// |---||---|-----|
	// | 0 || 1 | baz |
	// | 1 || 2 | qux |
	// +---++---+-----+
	// name: foo
}

func ExampleDataFrame_SetColNames() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"baz", "qux"}},
	).
		SetColNames([]string{"foo", "bar"})
	fmt.Println(df)
	// Output:
	// +---++-----+-----+
	// | - || foo | bar |
	// |---||-----|-----|
	// | 0 ||   1 | baz |
	// | 1 ||   2 | qux |
	// +---++-----+-----+
}

func ExampleDataFrame_SetLabelNames() {
	df := tada.NewDataFrame([]interface{}{[]float64{1, 2}}).
		SetLabelNames([]string{"baz"})
	fmt.Println(df)
	// Output:
	// +-----++---+
	// | baz || 0 |
	// |-----||---|
	// |   0 || 1 |
	// |   1 || 2 |
	// +-----++---+
}

func ExampleDataFrame_SetLabelNames_multiple() {
	df := tada.NewDataFrame(
		[]interface{}{[]float64{1, 2}},
		[]int{0, 1}, []string{"foo", "bar"},
	).
		SetColNames([]string{"A"}).
		SetLabelNames([]string{"baz", "qux"})
	fmt.Println(df)
	// Output:
	// +-----+-----++---+
	// | baz | qux || A |
	// |-----|-----||---|
	// |   0 | foo || 1 |
	// |   1 | bar || 2 |
	// +-----+-----++---+
}

func ExampleDataFrameMutator_WithCol_rename() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}},
	).
		SetColNames([]string{"foo"})
	fmt.Println(df)

	df.InPlace().WithCol("foo", "qux")
	fmt.Println(df)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// +---++-----+
	// | - || qux |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
}

func ExampleDataFrame_WithCol_rename() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}},
	).
		SetColNames([]string{"foo"})
	fmt.Println(df)

	ret := df.WithCol("foo", "qux")
	fmt.Println(ret)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// +---++-----+
	// | - || qux |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
}

func ExampleDataFrame_WithCol_overwrite() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}},
	).
		SetColNames([]string{"foo"})
	fmt.Println(df)

	ret := df.WithCol("foo", []string{"baz", "qux"})
	fmt.Println(ret)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 || baz |
	// | 1 || qux |
	// +---++-----+
}

func ExampleDataFrame_WithCol_append() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}},
	).
		SetColNames([]string{"foo"})
	fmt.Println(df)

	ret := df.WithCol("bar", []bool{false, true})
	fmt.Println(ret)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// +---++-----+-------+
	// | - || foo |  bar  |
	// |---||-----|-------|
	// | 0 ||   1 | false |
	// | 1 ||   2 |  true |
	// +---++-----+-------+
}

func ExampleDataFrame_Sort() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{2, 2, 1}, []string{"b", "c", "a"}},
	).
		SetColNames([]string{"foo", "bar"})
	fmt.Println(df)

	// first sort by foo in ascending order, then sort by bar in descending order
	ret := df.Sort(
		// Float64 is the default sorting DType, and ascending is the default ordering
		tada.Sorter{Name: "foo"},
		tada.Sorter{Name: "bar", DType: tada.String, Descending: true},
	)
	fmt.Println(ret)
	// Output:
	// +---++-----+-----+
	// | - || foo | bar |
	// |---||-----|-----|
	// | 0 ||   2 |   b |
	// | 1 ||     |   c |
	// | 2 ||   1 |   a |
	// +---++-----+-----+
	//
	// +---++-----+-----+
	// | - || foo | bar |
	// |---||-----|-----|
	// | 2 ||   1 |   a |
	// | 1 ||   2 |   c |
	// | 0 ||     |   b |
	// +---++-----+-----+
}

func ExampleDataFrame_Filter() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2, 3}, []time.Time{dt1, dt2, dt1}},
	).
		SetColNames([]string{"foo", "bar"})
	fmt.Println(df)

	gt1 := func(val interface{}) bool { return val.(float64) > 1 }
	beforeDate := func(val interface{}) bool { return val.(time.Time).Before(dt2) }
	ret := df.Filter(map[string]tada.FilterFn{
		"foo": gt1,
		"bar": beforeDate,
	})
	fmt.Println(ret)
	// Output:
	// +---++-----+----------------------+
	// | - || foo |         bar          |
	// |---||-----|----------------------|
	// | 0 ||   1 | 2020-01-01T00:00:00Z |
	// | 1 ||   2 | 2020-01-02T00:00:00Z |
	// | 2 ||   3 | 2020-01-01T00:00:00Z |
	// +---++-----+----------------------+
	//
	// +---++-----+----------------------+
	// | - || foo |         bar          |
	// |---||-----|----------------------|
	// | 2 ||   3 | 2020-01-01T00:00:00Z |
	// +---++-----+----------------------+
}

func ExampleDataFrame_Where() {
	df := tada.NewDataFrame([]interface{}{
		[]int{1, 2}},
	).
		SetColNames([]string{"foo"})
	fmt.Println(df)

	gt1 := func(val interface{}) bool { return val.(int) > 1 }
	ret, _ := df.Where(map[string]tada.FilterFn{"foo": gt1}, true, false)
	fmt.Println(ret)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// +---++-------+
	// | - ||       |
	// |---||-------|
	// | 0 || false |
	// | 1 ||  true |
	// +---++-------+
}
func ExamplePrintOptionMaxRows() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2, 3, 4, 5, 6, 7, 8}}).SetColNames([]string{"A"})
	tada.PrintOptionMaxRows(6)
	fmt.Println(df)
	tada.PrintOptionMaxRows(50)
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

func ExamplePrintOptionMaxColumns() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2}, []float64{3, 4}, []float64{5, 6},
		[]float64{3, 4}, []float64{5, 6},
	}).SetColNames([]string{"A", "B", "C", "D", "E"})
	tada.PrintOptionMaxColumns(2)
	fmt.Println(df)
	tada.PrintOptionMaxColumns(20)
	// Output:
	// +---++---+-----+---+
	// | - || A | ... | E |
	// |---||---|-----|---|
	// | 0 || 1 | ... | 5 |
	// | 1 || 2 |     | 6 |
	// +---++---+-----+---+
}

func ExamplePrintOptionMaxCellWidth() {
	df := tada.NewDataFrame([]interface{}{
		[]string{"corgilius", "barrius", "foo"},
	}).SetColNames([]string{"waldonius"})
	tada.PrintOptionMaxCellWidth(5)
	fmt.Println(df)
	tada.PrintOptionMaxCellWidth(30)
	// Output:
	// +---++-------+
	// | - || wa... |
	// |---||-------|
	// | 0 || co... |
	// | 1 || ba... |
	// | 2 ||   foo |
	// +---++-------+
}

func ExampleDataFrame_GroupBy() {
	df := tada.NewDataFrame([]interface{}{
		[]float64{1, 2, 3, 4},
	},
		[]string{"foo", "bar", "foo", "bar"}).
		SetColNames([]string{"baz"})
	g := df.GroupBy()
	fmt.Println(g)
	// Output:
	// +-----++-----+
	// |  -  || baz |
	// |-----||-----|
	// | foo ||   1 |
	// |     ||   3 |
	// | bar ||   2 |
	// |     ||   4 |
	// +-----++-----+
}

func ExampleDataFrame_Struct() {
	df := tada.NewDataFrame(
		[]interface{}{
			[]float64{1, 2},
		},
		[]string{"baz", "qux"},
	).SetLabelNames([]string{"foo"}).
		SetColNames([]string{"bar"})
	type output struct {
		Foo []string  `tada:"foo"`
		Bar []float64 `tada:"bar"`
	}
	var out output
	w := tada.StructWriter{Struct: &out}
	df.Write(&w)
	fmt.Printf("%#v", out)
	// Output:
	// tada_test.output{Foo:[]string{"baz", "qux"}, Bar:[]float64{1, 2}}
}

func ExampleDataFrame_Struct_withNulls() {
	df := tada.NewDataFrame(
		[]interface{}{
			[]float64{1, 2},
		},
		[]string{"", "qux"},
	).SetLabelNames([]string{"foo"}).
		SetColNames([]string{"bar"})
	type output struct {
		Foo   []string  `tada:"foo"`
		Bar   []float64 `tada:"bar"`
		Nulls [][]bool  `tada:"isNull"`
	}
	var out output
	w := tada.StructWriter{Struct: &out}
	df.Write(&w)
	fmt.Printf("%#v", out)
	// Output:
	// tada_test.output{Foo:[]string{"", "qux"}, Bar:[]float64{1, 2}, Nulls:[][]bool{[]bool{true, false}, []bool{false, false}}}

}
