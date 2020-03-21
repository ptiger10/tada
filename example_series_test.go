package tada

import (
	"errors"
	"fmt"
	"math"
	"strconv"
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

func ExampleSeries_Apply_float64() {
	s := NewSeries([]int{1, 2, 3}).SetName("foo")
	fmt.Println(s)

	// coerces to float64, applies func
	times2 := ApplyFn{Float64: func(v float64) float64 { return v * 2 }}
	fmt.Println(s.Apply(times2))

	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// | 2 ||   3 |
	// +---++-----+
	//
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   2 |
	// | 1 ||   4 |
	// | 2 ||   6 |
	// +---++-----+
}

func ExampleSeries_ApplyFormat_float64() {
	s := NewSeries([]float64{1, 2.5, 3.1415}).SetName("foo")
	fmt.Println(s)

	decimalFormat := ApplyFormatFn{Float64: func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }}
	fmt.Println(s.ApplyFormat(decimalFormat))

	// Output:
	// +---++--------+
	// | - ||  foo   |
	// |---||--------|
	// | 0 ||      1 |
	// | 1 ||    2.5 |
	// | 2 || 3.1415 |
	// +---++--------+
	//
	// +---++------+
	// | - || foo  |
	// |---||------|
	// | 0 || 1.00 |
	// | 1 || 2.50 |
	// | 2 || 3.14 |
	// +---++------+
}

func ExampleSeries_ApplyFormat_dateTime() {
	s := NewSeries([]time.Time{time.Date(2020, 1, 15, 0, 0, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	monthFormat := ApplyFormatFn{DateTime: func(v time.Time) string { return v.Format("2006-01") }}
	fmt.Println(s.ApplyFormat(monthFormat))

	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T00:00:00Z |
	// +---++----------------------+
	//
	// +---++---------+
	// | - ||   foo   |
	// |---||---------|
	// | 0 || 2020-01 |
	// +---++---------+
}

func ExampleSeries_Resample_byMonth() {
	s := NewSeries([]time.Time{time.Date(2020, 1, 15, 12, 30, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	byMonth := Resampler{ByMonth: true}
	fmt.Println(s.Resample(byMonth))
	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:30:00Z |
	// +---++----------------------+
	//
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-01T00:00:00Z |
	// +---++----------------------+
}

func ExampleSeries_Resample_byWeek() {
	s := NewSeries([]time.Time{time.Date(2020, 1, 15, 12, 30, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	byWeek := Resampler{ByWeek: true, StartOfWeek: time.Sunday}
	fmt.Println(s.Resample(byWeek))
	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:30:00Z |
	// +---++----------------------+
	//
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-12T00:00:00Z |
	// +---++----------------------+
}

func ExampleSeries_Resample_byHour() {
	s := NewSeries([]time.Time{time.Date(2020, 1, 15, 12, 30, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	byHour := Resampler{ByDuration: time.Hour}
	fmt.Println(s.Resample(byHour))
	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:30:00Z |
	// +---++----------------------+
	//
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:00:00Z |
	// +---++----------------------+
}

func ExampleSeries_Resample_byHalfHour() {
	s := NewSeries([]time.Time{
		time.Date(2020, 1, 15, 12, 15, 0, 0, time.UTC),
		time.Date(2020, 1, 15, 12, 45, 0, 0, time.UTC),
	}).SetName("foo")
	fmt.Println(s)

	byHalfHour := Resampler{ByDuration: 30 * time.Minute}
	fmt.Println(s.Resample(byHalfHour))
	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:15:00Z |
	// | 1 || 2020-01-15T12:45:00Z |
	// +---++----------------------+
	//
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:00:00Z |
	// | 1 || 2020-01-15T12:30:00Z |
	// +---++----------------------+
}

func ExampleSeries_GroupBy() {
	s := NewSeries([]float64{1, 2, 3, 4}, []string{"foo", "foo", "bar", "bar"})
	fmt.Println(s.GroupBy("*0"))
	// Output:
	// Groups: foo,bar
}
func ExampleSeries_GroupBy_compoundGroup() {
	s := NewSeries([]float64{1, 2}, []string{"foo", "foo"}, []string{"bar", "bar"})
	fmt.Println(s.GroupBy("*0", "*1"))
	// Output:
	// Groups: foo|bar
}

func ExampleGroupedSeries_Mean() {
	s := NewSeries([]float64{1, 2, 3, 4}, []int{0, 1, 0, 1}).
		SetName("foo").
		SetLabelNames([]string{"baz"})
	fmt.Println(s)

	// here, s.GroupBy("baz") is equivalent to s.GroupBy()
	g := s.GroupBy("baz")
	fmt.Println(g.Mean())

	// Output:
	// +-----++-----+
	// | baz || foo |
	// |-----||-----|
	// |   0 ||   1 |
	// |   1 ||   2 |
	// |   0 ||   3 |
	// |   1 ||   4 |
	// +-----++-----+
	//
	// +-----++----------+
	// | baz || mean_foo |
	// |-----||----------|
	// |   0 ||        2 |
	// |   1 ||        3 |
	// +-----++----------+
}

func ExampleGroupedSeries_Align_mean() {
	s := NewSeries([]float64{1, 2, 3, 4}, []int{0, 1, 0, 1}).
		SetName("foo").
		SetLabelNames([]string{"baz"})
	fmt.Println(s)

	// here, s.GroupBy("baz") is equivalent to s.GroupBy()
	g := s.GroupBy("baz")
	fmt.Println(g.Align().Mean())

	// Output:
	// +-----++-----+
	// | baz || foo |
	// |-----||-----|
	// |   0 ||   1 |
	// |   1 ||   2 |
	// |   0 ||   3 |
	// |   1 ||   4 |
	// +-----++-----+
	//
	// +-----++----------+
	// | baz || mean_foo |
	// |-----||----------|
	// |   0 ||        2 |
	// |   1 ||        3 |
	// |   0 ||        2 |
	// |   1 ||        3 |
	// +-----++----------+
}

func ExampleGroupedSeries_Reduce_float64() {
	s := NewSeries([]float64{1, 2, 3, 4, 5, 6}, []int{0, 0, 0, 1, 1, 1}).
		SetName("foo").
		SetLabelNames([]string{"baz"})
	fmt.Println(s)

	g := s.GroupBy("baz")
	maxOdd := GroupReduceFn{Float64: func(vals []float64) float64 {
		max := math.Inf(-1)
		for i := range vals {
			if int(vals[i])%2 == 1 && vals[i] > max {
				max = vals[i]
			}
		}
		return max
	}}
	fmt.Println(g.Reduce("max_odd", maxOdd))

	// Output:
	// +-----++-----+
	// | baz || foo |
	// |-----||-----|
	// |   0 ||   1 |
	// |     ||   2 |
	// |     ||   3 |
	// |   1 ||   4 |
	// |     ||   5 |
	// |     ||   6 |
	// +-----++-----+
	//
	// +-----++-------------+
	// | baz || max_odd_foo |
	// |-----||-------------|
	// |   0 ||           3 |
	// |   1 ||           5 |
	// +-----++-------------+
}

func ExampleSeries_zscore() {
	s := NewSeries([]float64{1, 2, 3, 4, 5}).SetName("foo")
	fmt.Println(s)

	vals := s.GetValuesFloat64()
	ret := make([]float64, s.Len())
	mean := s.Mean()
	std := s.Std()
	for i := range vals {
		ret[i] = (vals[i] - mean) / std
	}

	newS := NewSeries(ret, s.GetLabels()...).SetName("zscore_foo")
	decimalFormat := ApplyFormatFn{Float64: func(v float64) string { return strconv.FormatFloat(v, 'f', 2, 64) }}
	newS.InPlace().ApplyFormat(decimalFormat)
	fmt.Println(newS)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// | 2 ||   3 |
	// | 3 ||   4 |
	// | 4 ||   5 |
	// +---++-----+
	//
	// +---++------------+
	// | - || zscore_foo |
	// |---||------------|
	// | 0 ||      -1.41 |
	// | 1 ||      -0.71 |
	// | 2 ||       0.00 |
	// | 3 ||       0.71 |
	// | 4 ||       1.41 |
	// +---++------------+
}

func ExampleGroupedSeries_Transform_zscore() {
	s := NewSeries([]float64{1, 2, 3, 4}, []int{0, 0, 1, 1}).
		SetName("foo").
		SetLabelNames([]string{"baz"})
	fmt.Println(s)

	g := s.GroupBy()
	zScore := func(input interface{}) interface{} {
		// in normal usage, check the type assertion and handle an error
		vals, _ := input.([]float64)
		var sum float64
		for i := range vals {
			sum += vals[i]
		}
		mean := sum / float64(len(vals))

		var variance float64
		for i := range vals {
			variance += math.Pow((vals[i] - mean), 2)
		}
		std := math.Pow(variance/float64(len(vals)), 0.5)

		ret := make([]float64, len(vals))
		for i := range vals {
			ret[i] = (vals[i] - mean) / std
		}
		return ret
	}
	fmt.Println(g.Transform("z_score", zScore))

	// Output:
	// +-----++-----+
	// | baz || foo |
	// |-----||-----|
	// |   0 ||   1 |
	// |     ||   2 |
	// |   1 ||   3 |
	// |     ||   4 |
	// +-----++-----+
	//
	// +-----++---------+
	// | baz || z_score |
	// |-----||---------|
	// |   0 ||      -1 |
	// |     ||       1 |
	// |   1 ||      -1 |
	// |     ||       1 |
	// +-----++---------+
}

func ExampleGroupedSeries_HavingCount_sum() {
	s := NewSeries([]float64{1, 2, 3, 4}, []int{0, 1, 1, 1}).
		SetName("foo").
		SetLabelNames([]string{"baz"})
	fmt.Println(s)

	countOf3 := func(n int) bool { return n == 3 }
	g := s.GroupBy("baz")
	fmt.Println(g.HavingCount(countOf3).Sum())

	// Output:
	// +-----++-----+
	// | baz || foo |
	// |-----||-----|
	// |   0 ||   1 |
	// |   1 ||   2 |
	// |     ||   3 |
	// |     ||   4 |
	// +-----++-----+
	//
	// +-----++---------+
	// | baz || sum_foo |
	// |-----||---------|
	// |   1 ||       9 |
	// +-----++---------+
}
