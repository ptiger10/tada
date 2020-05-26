package tada_test

import (
	"fmt"
	"strings"
	"time"

	"github.com/ptiger10/tada"
)

func ExampleNewReader() {
	data := `foo,bar
baz,qux
corge,fred`
	df, _ := tada.NewCSVReader(strings.NewReader(data)).Read()
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
	data := `foo,bar
baz,qux
corge,fred`

	r := tada.NewCSVReader(strings.NewReader(data))
	r.HeaderRows = 0
	df, _ := r.Read()
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
	data := `foo,bar
baz,qux
corge,fred`
	r := tada.NewCSVReader(strings.NewReader(data))
	r.HeaderRows = 2
	df, _ := r.Read()
	fmt.Println(df)
	// Output:
	// +----++-------+------+
	// | *0 ||  foo  | bar  |
	// |    ||  baz  | qux  |
	// |----||-------|------|
	// |  0 || corge | fred |
	// +----++-------+------+
}

func ExampleNewReader_multipleHeadersWithLabels() {
	data := `,foo,bar
labels,baz,qux
1,corge,fred`
	r := tada.NewCSVReader(strings.NewReader(data))
	r.HeaderRows = 2
	r.LabelLevels = 1
	df, _ := r.Read()
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
	data := `foo,bar
baz,qux
corge,fred`
	r := tada.NewCSVReader(strings.NewReader(data))
	r.LabelLevels = 1
	df, _ := r.Read()
	fmt.Println(df)
	// Output:
	// +-------++------+
	// |  foo  || bar  |
	// |-------||------|
	// |   baz ||  qux |
	// | corge || fred |
	// +-------++------+
}

func ExampleNewRecordReader() {
	data := [][]string{
		{"foo", "bar"},
		{"baz", "qux"},
		{"corge", "fred"},
	}
	df, _ := tada.NewRecordReader(data).Read()
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleNewRecordReader_byColumn() {
	data := [][]string{
		{"foo", "bar"},
		{"baz", "qux"},
		{"corge", "fred"},
	}
	r := tada.NewRecordReader(data)
	r.ByColumn = true
	df, _ := r.Read()
	fmt.Println(df)
	// Output:
	// +---++-----+-----+-------+
	// | - || foo | baz | corge |
	// |---||-----|-----|-------|
	// | 0 || bar | qux |  fred |
	// +---++-----+-----+-------+
}

func ExampleNewDataFrame() {
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
		[]string{"baz", "qux"},
	})

	df, _ := tada.NewDataFrame(r)
	fmt.Println(df)
	// Output:
	// +---++---+-----+
	// | - || 0 |  1  |
	// |---||---|-----|
	// | 0 || 1 | baz |
	// | 1 || 2 | qux |
	// +---++---+-----+
}

func ExampleDataFrame_withLabels() {
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.LabelSlices = []interface{}{
		[]string{"foo", "bar"},
	}
	df, _ := r.Read()
	fmt.Println(df)
	// Output:
	// +-----++---+
	// |  -  || 0 |
	// |-----||---|
	// | foo || 1 |
	// | bar || 2 |
	// +-----++---+
}

func ExampleDataFrame_setColNames() {
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
		[]string{"baz", "qux"},
	})
	r.ColNames = []string{"foo", "bar"}
	r.LabelNames = []string{"baz"}
	r.Name = "foo"
	df, _ := r.Read()
	fmt.Println(df)
	// Output:
	// +-----++-----+-----+
	// | baz || foo | bar |
	// |-----||-----|-----|
	// |   0 ||   1 | baz |
	// |   1 ||   2 | qux |
	// +-----++-----+-----+
	// name: foo
}

func ExampleDataFrameMutator_WithCol_rename() {
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.ColNames = []string{"foo"}
	df, _ := r.Read()
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.ColNames = []string{"foo"}
	df, _ := r.Read()
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.ColNames = []string{"foo"}
	df, _ := r.Read()
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.ColNames = []string{"foo"}
	df, _ := r.Read()
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{10, 2, 2, 1},
		[]string{"d", "b", "c", "a"},
	})
	r.ColNames = []string{"foo", "bar"}
	df, _ := r.Read()
	fmt.Println(df)

	// first sort by foo in ascending order, then sort by bar in descending order
	ret := df.Sort(
		// Float64 is the default sorting DType, and ascending is the default ordering
		tada.Sorter{Name: "foo", DType: tada.Float64},
		tada.Sorter{Name: "bar", DType: tada.String, Descending: true},
	)
	fmt.Println(ret)
	// Output:
	// +---++-----+-----+
	// | - || foo | bar |
	// |---||-----|-----|
	// | 0 ||  10 |   d |
	// | 1 ||   2 |   b |
	// | 2 ||     |   c |
	// | 3 ||   1 |   a |
	// +---++-----+-----+
	//
	// +---++-----+-----+
	// | - || foo | bar |
	// |---||-----|-----|
	// | 3 ||   1 |   a |
	// | 2 ||   2 |   c |
	// | 1 ||     |   b |
	// | 0 ||  10 |   d |
	// +---++-----+-----+
}

func ExampleDataFrame_Filter() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2, 3},
		[]time.Time{dt1, dt2, dt1},
	})
	r.ColNames = []string{"foo", "bar"}
	df, _ := r.Read()
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.ColNames = []string{"foo"}
	df, _ := r.Read()
	fmt.Println(df)

	gt1 := func(val interface{}) bool { return val.(float64) > 1 }
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2, 3, 4, 5, 6, 7, 8},
	})
	r.ColNames = []string{"foo"}
	df, _ := r.Read()
	tada.PrintOptionMaxRows(6)
	fmt.Println(df)
	tada.PrintOptionMaxRows(50)
	// Output:
	// +-----++-----+
	// |  -  ||  foo  |
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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
		[]float64{3, 4},
		[]float64{5, 6},
		[]float64{3, 4},
		[]float64{5, 6},
	})
	r.ColNames = []string{"A", "B", "C", "D", "E"}
	df, _ := r.Read()
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
	r := tada.NewSliceReader([]interface{}{
		[]string{"corgilius", "barrius", "foo"},
	})
	r.ColNames = []string{"waldonius"}
	df, _ := r.Read()

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
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2, 3, 4},
	})
	r.LabelSlices = []interface{}{
		[]string{"foo", "bar", "foo", "bar"},
	}
	r.ColNames = []string{"baz"}
	df, _ := r.Read()

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

func ExampleNewStructReader() {
	type input struct {
		Foo string `json:"foo"`
	}
	in := []input{
		{"foo"},
		{"--"},
		{"bar"},
	}

	r := tada.NewStructReader(in)
	r.IsNull = [][]bool{
		{false},
		{true},
		{false},
	}
	df, _ := r.Read()
	fmt.Println(df)
	// Output:
	// +---++--------+
	// | - ||  foo   |
	// |---||--------|
	// | 0 ||    foo |
	// | 1 || (null) |
	// | 2 ||    bar |
	// +---++--------+
}

func ExampleNewStructWriter() {
	r := tada.NewSliceReader([]interface{}{
		[]float64{1, 2},
	})
	r.ColNames = []string{"foo"}

	df, _ := r.Read()
	type output struct {
		Foo float64 `json:"foo"`
	}
	var out []output
	w := tada.NewStructWriter(&out)
	df.WriteTo(w)
	fmt.Printf("%+v", out)
	// Output:
	// [{Foo:1} {Foo:2}]
}
