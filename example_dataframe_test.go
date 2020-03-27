package tada

import (
	"errors"
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"
	"time"
)

func ExampleReadCSV() {
	data := "foo, bar\n baz, qux\n corge, fred"
	df, _ := ReadCSV(strings.NewReader(data))
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleReadCSV_noHeaders() {
	data := "foo, bar\n baz, qux\n corge, fred"
	df, _ := ReadCSV(strings.NewReader(data), ReadOptionHeaders(0))
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

func ExampleReadCSV_multipleHeaders() {
	data := "foo, bar\n baz, qux\n corge, fred"
	df, _ := ReadCSV(strings.NewReader(data), ReadOptionHeaders(2))
	fmt.Println(df)
	// Output:
	// +----++-------+------+
	// |    ||  foo  | bar  |
	// | *0 ||  baz  | qux  |
	// |----||-------|------|
	// |  0 || corge | fred |
	// +----++-------+------+
}

func ExampleReadCSV_withLabels() {
	data := `foo, bar
	baz, qux
	corge, fred`
	df, _ := ReadCSV(strings.NewReader(data), ReadOptionLabels(1))
	fmt.Println(df)
	// Output:
	// +-------++------+
	// |  foo  || bar  |
	// |-------||------|
	// |   baz ||  qux |
	// | corge || fred |
	// +-------++------+
}

func ExampleReadCSV_delimiter() {
	data := `foo|bar
	baz|qux
	corge|fred`
	df, _ := ReadCSV(strings.NewReader(data), ReadOptionDelimiter('|'))
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleReadCSVFromRecords() {
	data := [][]string{
		{"foo", "bar"},
		{"baz", "qux"},
		{"corge", "fred"},
	}
	df, _ := ReadCSVFromRecords(data)
	fmt.Println(df)
	// Output:
	// +---++-------+------+
	// | - ||  foo  | bar  |
	// |---||-------|------|
	// | 0 ||   baz |  qux |
	// | 1 || corge | fred |
	// +---++-------+------+
}

func ExampleReadCSVFromRecords_colsAsMajorDimension() {
	data := [][]string{
		{"foo", "bar"},
		{"baz", "qux"},
		{"corge", "fred"},
	}
	df, _ := ReadCSVFromRecords(data, ReadOptionSwitchDims())
	fmt.Println(df)
	// Output:
	// +---++-----+-----+-------+
	// | - || foo | baz | corge |
	// |---||-----|-----|-------|
	// | 0 || bar | qux |  fred |
	// +---++-----+-----+-------+
}

func ExampleDataFrame() {
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"baz", "qux"}},
	)
	fmt.Println(df)
	// Output:
	// +---++---+-----+
	// | - || 0 |  1  |
	// |---||---|-----|
	// | 0 || 1 | baz |
	// | 1 || 2 | qux |
	// +---++---+-----+
}

func ExampleDataFrame_withError() {
	df := &DataFrame{err: errors.New("foo")}
	fmt.Println(df)
	// Output:
	// Error: foo
}

func ExampleDataFrame_withNullValues() {
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

func ExampleDataFrame_SetColNames() {
	df := NewDataFrame([]interface{}{
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
	df := NewDataFrame([]interface{}{[]float64{1, 2}}).
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
	df := NewDataFrame(
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
	df := NewDataFrame([]interface{}{
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
	df := NewDataFrame([]interface{}{
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
	df := NewDataFrame([]interface{}{
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
	df := NewDataFrame([]interface{}{
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
	df := NewDataFrame([]interface{}{
		[]float64{2, 2, 1}, []string{"b", "c", "a"}},
	).
		SetColNames([]string{"foo", "bar"})
	fmt.Println(df)

	// first sort by foo in ascending order, then sort by bar in descending order
	ret := df.Sort(
		// Float64 is the default sorting DType, and ascending is the default ordering
		Sorter{Name: "foo"},
		Sorter{Name: "bar", DType: String, Descending: true},
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

func ExampleDataFrame_Filter_float64() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"corge", "fred"}, []time.Time{dt1, dt2}},
	).
		SetColNames([]string{"foo", "bar", "baz"})
	fmt.Println(df)

	gt1 := FilterFn{Float64: func(v float64) bool { return v > 1 }}
	ret := df.Filter(map[string]FilterFn{"foo": gt1})
	fmt.Println(ret)
	// Output:
	// +---++-----+-------+----------------------+
	// | - || foo |  bar  |         baz          |
	// |---||-----|-------|----------------------|
	// | 0 ||   1 | corge | 2020-01-01T00:00:00Z |
	// | 1 ||   2 |  fred | 2020-01-02T00:00:00Z |
	// +---++-----+-------+----------------------+
	//
	// +---++-----+------+----------------------+
	// | - || foo | bar  |         baz          |
	// |---||-----|------|----------------------|
	// | 1 ||   2 | fred | 2020-01-02T00:00:00Z |
	// +---++-----+------+----------------------+
}

func ExampleDataFrame_Filter_string() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"corge", "fred"}, []time.Time{dt1, dt2}},
	).
		SetColNames([]string{"foo", "bar", "baz"})
	fmt.Println(df)

	containsD := FilterFn{String: func(v string) bool { return strings.Contains(v, "d") }}
	ret := df.Filter(map[string]FilterFn{"bar": containsD})
	fmt.Println(ret)
	// Output:
	// +---++-----+-------+----------------------+
	// | - || foo |  bar  |         baz          |
	// |---||-----|-------|----------------------|
	// | 0 ||   1 | corge | 2020-01-01T00:00:00Z |
	// | 1 ||   2 |  fred | 2020-01-02T00:00:00Z |
	// +---++-----+-------+----------------------+
	//
	// +---++-----+------+----------------------+
	// | - || foo | bar  |         baz          |
	// |---||-----|------|----------------------|
	// | 1 ||   2 | fred | 2020-01-02T00:00:00Z |
	// +---++-----+------+----------------------+
}

func ExampleDataFrame_Filter_dateTime() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"corge", "fred"}, []time.Time{dt1, dt2}},
	).
		SetColNames([]string{"foo", "bar", "baz"})
	fmt.Println(df)

	afterDate := FilterFn{DateTime: func(v time.Time) bool { return v.After(dt1) }}
	ret := df.Filter(map[string]FilterFn{"baz": afterDate})
	fmt.Println(ret)
	// Output:
	// +---++-----+-------+----------------------+
	// | - || foo |  bar  |         baz          |
	// |---||-----|-------|----------------------|
	// | 0 ||   1 | corge | 2020-01-01T00:00:00Z |
	// | 1 ||   2 |  fred | 2020-01-02T00:00:00Z |
	// +---++-----+-------+----------------------+
	//
	// +---++-----+------+----------------------+
	// | - || foo | bar  |         baz          |
	// |---||-----|------|----------------------|
	// | 1 ||   2 | fred | 2020-01-02T00:00:00Z |
	// +---++-----+------+----------------------+
}

func ExampleDataFrame_Filter_interface() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	df := NewDataFrame([]interface{}{
		[]int{1, 2}, []string{"corge", "fred"}, []time.Time{dt1, dt2}},
	).
		SetColNames([]string{"foo", "bar", "baz"})
	fmt.Println(df)

	gt1 := FilterFn{Interface: func(v interface{}) bool {
		val, ok := v.(int)
		if !ok {
			log.Fatalf("Expecting type int, got %v", reflect.TypeOf(v))
		}
		return val > 1
	}}
	ret := df.Filter(map[string]FilterFn{"foo": gt1})
	fmt.Println(ret)
	// Output:
	// +---++-----+-------+----------------------+
	// | - || foo |  bar  |         baz          |
	// |---||-----|-------|----------------------|
	// | 0 ||   1 | corge | 2020-01-01T00:00:00Z |
	// | 1 ||   2 |  fred | 2020-01-02T00:00:00Z |
	// +---++-----+-------+----------------------+
	//
	// +---++-----+------+----------------------+
	// | - || foo | bar  |         baz          |
	// |---||-----|------|----------------------|
	// | 1 ||   2 | fred | 2020-01-02T00:00:00Z |
	// +---++-----+------+----------------------+
}

func ExampleDataFrame_Where() {
	df := NewDataFrame([]interface{}{
		[]int{1, 2}},
	).
		SetColNames([]string{"foo"})
	fmt.Println(df)

	gt1 := FilterFn{Float64: func(v float64) bool { return v > 1 }}
	ret, _ := df.Where(map[string]FilterFn{"foo": gt1}, true, false)
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

func ExamplePrintOptionMaxColumns() {
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
