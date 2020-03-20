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

func ExampleSeries_test_withError() {
	s := &Series{err: errors.New("foo")}
	fmt.Println(s)
	// Output:
	// Error: foo
}

func ExampleSeries_test_null() {
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

func ExampleSeries_test_nested() {
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

func ExampleDataFrame_WithCol_rename() {
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}},
	).
		SetColNames([]string{"foo"})
	ret := df.WithCol("foo", "qux")
	fmt.Println(ret)
	// Output:
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
	ret := df.WithCol("foo", []string{"baz", "qux"})
	fmt.Println(ret)
	// Output:
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
	ret := df.WithCol("bar", []bool{false, true})
	fmt.Println(ret)
	// Output:
	// +---++-----+-------+
	// | - || foo |  bar  |
	// |---||-----|-------|
	// | 0 ||   1 | false |
	// | 1 ||   2 |  true |
	// +---++-----+-------+
}

func ExampleDataFrame_Filter_float64() {
	dt1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	dt2 := dt1.AddDate(0, 0, 1)
	df := NewDataFrame([]interface{}{
		[]float64{1, 2}, []string{"corge", "fred"}, []time.Time{dt1, dt2}},
	).
		SetColNames([]string{"foo", "bar", "baz"})

	gt1 := FilterFn{Float64: func(v float64) bool { return v > 1 }}
	ret := df.Filter(map[string]FilterFn{"foo": gt1})
	fmt.Println(ret)
	// Output:
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

	containsD := FilterFn{String: func(v string) bool { return strings.Contains(v, "d") }}
	ret := df.Filter(map[string]FilterFn{"bar": containsD})
	fmt.Println(ret)
	// Output:
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

	afterDate := FilterFn{DateTime: func(v time.Time) bool { return v.After(dt1) }}
	ret := df.Filter(map[string]FilterFn{"baz": afterDate})
	fmt.Println(ret)
	// Output:
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
	// +---++-----+------+----------------------+
	// | - || foo | bar  |         baz          |
	// |---||-----|------|----------------------|
	// | 1 ||   2 | fred | 2020-01-02T00:00:00Z |
	// +---++-----+------+----------------------+
}
func ExampleDataFrame_test_excess_rows() {
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

func ExampleDataFrame_test_multi_labels() {
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

func ExampleDataFrame_test_excess_cols() {
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

func ExampleDataFrame_test_null() {
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

func ExampleDataFrame_test_withError() {
	df := &DataFrame{err: errors.New("foo")}
	fmt.Println(df)
	// Output:
	// Error: foo
}

func ExampleGroupedSeries_test_multiple() {
	g := NewSeries([]float64{1, 2, 3, 4}, []string{"foo", "foo", "bar", "bar"}).GroupBy("*0")
	fmt.Println(g)
	// Output:
	// Groups: foo,bar
}
func ExampleGroupedSeries_test_single() {
	g := NewSeries([]float64{1, 2}, []string{"foo", "foo"}, []string{"bar", "bar"}).GroupBy("*0", "*1")
	fmt.Println(g)
	// Output:
	// Groups: foo|bar
}
