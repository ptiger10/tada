package tada_test

import (
	"fmt"
	"math"
	"time"

	"github.com/ptiger10/tada"
)

func ExampleSeries() {
	s := tada.NewSeries([]float64{1, 2}).SetName("foo")
	fmt.Println(s)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
}

func ExampleSeries_setNaNStatus() {
	s := tada.NewSeries([]float64{0, math.NaN()})
	fmt.Println("isNull:", s.GetNulls())

	tada.SetOptionNaNStatus(false)
	s = tada.NewSeries([]float64{0, math.NaN()})
	fmt.Println("isNull:", s.GetNulls())

	tada.SetOptionNaNStatus(true)
	// Output:
	// isNull: [false true]
	// isNull: [false false]
}

func ExampleSeries_setEmptyStringAsNull() {
	s := tada.NewSeries([]string{"foo", "", "(null)"})
	fmt.Println("default sentinel null values\n isNull:", s.GetNulls())

	tada.SetOptionEmptyStringAsNull(true)
	s = tada.NewSeries([]string{"foo", "", "(null)"})
	fmt.Println("remove defaults\n isNull:", s.GetNulls())

	tada.SetOptionEmptyStringAsNull(false)
	// Output:
	// default sentinel null values
	//  isNull: [false false true]
	// remove defaults
	//  isNull: [false true true]
}

func ExampleSeries_nestedSlice() {
	s := tada.NewSeries([][]string{{"foo", "bar"}, {"baz"}, {}}).
		SetName("a")
	fmt.Println(s)
	// Output:
	// +---++-----------+
	// | - ||     a     |
	// |---||-----------|
	// | 0 || [foo bar] |
	// | 1 ||     [baz] |
	// | 2 ||    (null) |
	// +---++-----------+
}

func ExampleSeries_Bin() {
	s := tada.NewSeries([]float64{1, 3, 5}).SetName("foo")
	fmt.Println(s)

	binned, _ := s.Bin([]float64{0, 2, 4}, nil)
	fmt.Println(binned)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   3 |
	// | 2 ||   5 |
	// +---++-----+
	//
	// +---++--------+
	// | - ||  foo   |
	// |---||--------|
	// | 0 ||    0-2 |
	// | 1 ||    2-4 |
	// | 2 || (null) |
	// +---++--------+
}

func ExampleSeries_Bin_andMore() {
	s := tada.NewSeries([]float64{1, 3, 5}).SetName("foo")
	fmt.Println(s)

	binned, _ := s.Bin([]float64{0, 2, 4}, &tada.Binner{AndMore: true})
	fmt.Println(binned)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   3 |
	// | 2 ||   5 |
	// +---++-----+
	//
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 || 0-2 |
	// | 1 || 2-4 |
	// | 2 ||  >4 |
	// +---++-----+
}

func ExampleSeries_Bin_customLabels() {
	s := tada.NewSeries([]float64{1, 3}).SetName("foo")
	fmt.Println(s)

	binned, _ := s.Bin([]float64{0, 2, 4}, &tada.Binner{Labels: []string{"low", "high"}})
	fmt.Println(binned)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   3 |
	// +---++-----+
	//
	// +---++------+
	// | - || foo  |
	// |---||------|
	// | 0 ||  low |
	// | 1 || high |
	// +---++------+
}

func ExampleSeries_PercentileBin() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}).SetName("foo")
	fmt.Println(s)

	binned, _ := s.PercentileBin([]float64{0, .5, 1}, nil)
	fmt.Println(binned)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// | 2 ||   3 |
	// | 3 ||   4 |
	// +---++-----+
	//
	// +---++-------+
	// | - ||  foo  |
	// |---||-------|
	// | 0 || 0-0.5 |
	// | 1 ||       |
	// | 2 || 0.5-1 |
	// | 3 ||       |
	// +---++-------+
}

func ExampleSeries_PercentileBin_customLabels() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}).SetName("foo")
	fmt.Println(s)

	binned, _ := s.PercentileBin([]float64{0, .5, 1}, &tada.Binner{Labels: []string{"Bottom 50%", "Top 50%"}})
	fmt.Println(binned)
	// Output:
	// +---++-----+
	// | - || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// | 2 ||   3 |
	// | 3 ||   4 |
	// +---++-----+
	//
	// +---++------------+
	// | - ||    foo     |
	// |---||------------|
	// | 0 || Bottom 50% |
	// | 1 ||            |
	// | 2 ||    Top 50% |
	// | 3 ||            |
	// +---++------------+
}

func ExampleSeries_Lookup() {
	s := tada.NewSeries([]float64{1, 2}, []int{0, 1}).SetName("foo").SetLabelNames([]string{"a"})
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := tada.NewSeries([]float64{4, 5}, []int{0, 10}).SetLabelNames([]string{"a"})
	fmt.Println("--Series to lookup--")
	fmt.Println(s2)

	fmt.Println("--result--")
	lookup, _ := s.Lookup(s2)
	fmt.Println(lookup)
	// Output:
	// --original Series--
	// +---++-----+
	// | a || foo |
	// |---||-----|
	// | 0 ||   1 |
	// | 1 ||   2 |
	// +---++-----+
	//
	// --Series to lookup--
	// +----++---+
	// | a  || 0 |
	// |----||---|
	// |  0 || 4 |
	// | 10 || 5 |
	// +----++---+
	//
	// --result--
	// +---++--------+
	// | a ||  foo   |
	// |---||--------|
	// | 0 ||      4 |
	// | 1 || (null) |
	// +---++--------+
}

func ExampleSeries_Lookup_withOptions() {
	s := tada.NewSeries([]float64{1, 2}, []string{"foo", "bar"}, []int{0, 1}).SetLabelNames([]string{"a", "b"})
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := tada.NewSeries([]float64{4, 5}, []int{0, 10}, []string{"baz", "bar"}).SetLabelNames([]string{"a", "b"})
	fmt.Println("--Series to lookup--")
	fmt.Println(s2)

	fmt.Println("--result--")
	lookup, _ := s.Lookup(
		s2,
		tada.JoinOptionHow("inner"),
		tada.JoinOptionLeftOn([]string{"a"}),
		tada.JoinOptionRightOn([]string{"b"}),
	)
	fmt.Println(lookup)
	// Output:
	// --original Series--
	// +-----+---++---+
	// |  a  | b || 0 |
	// |-----|---||---|
	// | foo | 0 || 1 |
	// | bar | 1 || 2 |
	// +-----+---++---+
	//
	// --Series to lookup--
	// +----+-----++---+
	// | a  |  b  || 0 |
	// |----|-----||---|
	// |  0 | baz || 4 |
	// | 10 | bar || 5 |
	// +----+-----++---+
	//
	// --result--
	// +-----+---++---+
	// |  a  | b || 0 |
	// |-----|---||---|
	// | bar | 1 || 5 |
	// +-----+---++---+
}

func ExampleSeries_Merge() {
	s := tada.NewSeries([]float64{1, 2}, []int{0, 1}).SetName("foo")
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := tada.NewSeries([]float64{4, 5}, []int{0, 10}).SetName("bar")
	fmt.Println("--Series to merge--")
	fmt.Println(s2)

	fmt.Println("--result--")
	merged, _ := s.Merge(s2)
	fmt.Println(merged)
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
	// +---++-----+--------+
	// | - || foo |  bar   |
	// |---||-----|--------|
	// | 0 ||   1 |      4 |
	// | 1 ||   2 | (null) |
	// +---++-----+--------+
}

func ExampleSeries_Merge_withOptions() {
	s := tada.NewSeries([]float64{1, 2}, []string{"foo", "bar"}, []int{0, 1}).SetLabelNames([]string{"a", "b"})
	fmt.Println("--original Series--")
	fmt.Println(s)

	s2 := tada.NewSeries([]float64{4, 5}, []int{0, 10}, []string{"baz", "bar"}).SetLabelNames([]string{"a", "b"})
	fmt.Println("--Series to lookup--")
	fmt.Println(s2)

	fmt.Println("--result--")
	merged, _ := s.Merge(s2,
		tada.JoinOptionHow("inner"),
		tada.JoinOptionLeftOn([]string{"a"}),
		tada.JoinOptionRightOn([]string{"b"}),
	)
	fmt.Println(merged)
	// Output:
	// --original Series--
	// +-----+---++---+
	// |  a  | b || 0 |
	// |-----|---||---|
	// | foo | 0 || 1 |
	// | bar | 1 || 2 |
	// +-----+---++---+
	//
	// --Series to lookup--
	// +----+-----++---+
	// | a  |  b  || 0 |
	// |----|-----||---|
	// |  0 | baz || 4 |
	// | 10 | bar || 5 |
	// +----+-----++---+
	//
	// --result--
	// +-----+---++---+-----+
	// |  a  | b || 0 | 0_1 |
	// |-----|---||---|-----|
	// | bar | 1 || 2 |   5 |
	// +-----+---++---+-----+
}

func ExampleSeries_Apply_float64() {
	s := tada.NewSeries([]float64{1, 2, 3}).SetName("foo")
	fmt.Println(s)

	times2 := func(slice interface{}, isNull []bool) interface{} {
		vals := slice.([]float64)
		ret := make([]float64, len(vals))
		for i := range ret {
			ret[i] = vals[i] * 2
		}
		return ret
	}
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

func ExampleSeries_Resample_byMonth() {
	s := tada.NewSeries([]time.Time{time.Date(2020, 1, 15, 12, 30, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	byMonth := tada.Resampler{ByMonth: true}
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
	s := tada.NewSeries([]time.Time{time.Date(2020, 1, 15, 12, 30, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	byWeek := tada.Resampler{ByWeek: true, StartOfWeek: time.Sunday}
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
	s := tada.NewSeries([]time.Time{time.Date(2020, 1, 15, 12, 30, 0, 0, time.UTC)}).SetName("foo")
	fmt.Println(s)

	byHour := tada.Resampler{ByDuration: time.Hour}
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
	s := tada.NewSeries([]time.Time{
		time.Date(2020, 1, 15, 12, 15, 0, 0, time.UTC),
		time.Date(2020, 1, 15, 12, 45, 0, 0, time.UTC),
	}).SetName("foo")
	fmt.Println(s)

	byHalfHour := tada.Resampler{ByDuration: 30 * time.Minute}
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

func ExampleSeries_Cast_date() {
	s := tada.NewSeries([]time.Time{
		time.Date(2020, 1, 15, 12, 15, 0, 0, time.UTC),
	}).SetName("foo")
	fmt.Println(s)

	s.Cast(map[string]tada.DType{"foo": tada.Date})
	fmt.Println(s)
	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:15:00Z |
	// +---++----------------------+
	//
	// +---++------------+
	// | - ||    foo     |
	// |---||------------|
	// | 0 || 2020-01-15 |
	// +---++------------+
}

func ExampleSeries_Cast_time() {
	s := tada.NewSeries([]time.Time{
		time.Date(2020, 1, 15, 12, 15, 0, 0, time.UTC),
	}).SetName("foo")
	fmt.Println(s)

	s.Cast(map[string]tada.DType{"foo": tada.Time})
	fmt.Println(s)
	// Output:
	// +---++----------------------+
	// | - ||         foo          |
	// |---||----------------------|
	// | 0 || 2020-01-15T12:15:00Z |
	// +---++----------------------+
	//
	// +---++----------+
	// | - ||   foo    |
	// |---||----------|
	// | 0 || 12:15:00 |
	// +---++----------+
}

func ExampleSeries_GroupBy() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []string{"foo", "bar", "foo", "bar"})
	g := s.GroupBy()
	fmt.Println(g)
	// Output:
	// 	+-----++---+
	// |  -  || 0 |
	// |-----||---|
	// | foo || 1 |
	// |     || 3 |
	// | bar || 2 |
	// |     || 4 |
	// +-----++---+
}
func ExampleSeries_GroupBy_compoundGroup() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []string{"foo", "baz", "foo", "baz"}, []string{"bar", "qux", "bar", "qux"})
	g := s.GroupBy()
	fmt.Println(g)
	// +-----+-----++---+
	// |  -  |  -  || 0 |
	// |-----|-----||---|
	// | foo | bar || 1 |
	// |     |     || 3 |
	// | baz | qux || 2 |
	// |     |     || 4 |
	// +-----+-----++---+
}

func ExampleGroupedSeries_Mean() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []int{0, 1, 0, 1}).
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
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []int{0, 1, 0, 1}).
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

func ExampleGroupedSeries_Reduce() {
	s := tada.NewSeries([]float64{1, 2, 3, 4, 5, 6}, []int{0, 0, 0, 1, 1, 1}).
		SetName("foo").
		SetLabelNames([]string{"baz"})
	fmt.Println(s)

	g := s.GroupBy("baz")
	maxOdd := func(slice interface{}, isNull []bool) (value interface{}, null bool) {
		vals := slice.([]float64)
		max := math.Inf(-1)
		for i := range vals {
			if !isNull[i] && int(vals[i])%2 == 1 && vals[i] > max {
				max = vals[i]
			}
		}
		return max, false
	}
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
	s := tada.NewSeries([]float64{1, 2, 3, 4, 5}).SetName("foo")
	fmt.Println(s)

	vals := s.GetValuesAsFloat64()
	ret := make([]float64, s.Len())
	mean := s.Mean()
	std := s.StdDev()
	for i := range vals {
		val := (vals[i] - mean) / std
		ret[i] = math.Round((val * 100)) / 100 // round to 2 decimal points
	}
	df := s.DataFrame().WithCol("zscore_foo", ret)
	fmt.Println(df)
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
	// +---++-----+------------+
	// | - || foo | zscore_foo |
	// |---||-----|------------|
	// | 0 ||   1 |      -1.41 |
	// | 1 ||   2 |      -0.71 |
	// | 2 ||   3 |          0 |
	// | 3 ||   4 |       0.71 |
	// | 4 ||   5 |       1.41 |
	// +---++-----+------------+
}

func ExampleGroupedSeries_Apply() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []string{"bar", "bar", "foo", "bar"}, []int{0, 1, 2, 3}).
		SetName("foobar").
		SetLabelNames([]string{"baz", "qux"})
	fmt.Println(s)

	g := s.GroupBy("baz")
	// if group has at least 3 items, multiply by 2. otherwise set as null.
	modifyBigGroup := func(slice interface{}, isNull []bool) interface{} {
		vals, _ := slice.([]float64) // in normal usage, check the type assertion and handle an error
		ret := make([]float64, len(vals))
		if len(vals) >= 3 {
			for i := range ret {
				ret[i] = vals[i] * 2
			}
		} else {
			for i := range ret {
				isNull[i] = true
			}
		}
		return ret
	}
	fmt.Println(g.Apply(modifyBigGroup).Series())

	// Output:
	// +-----+-----++--------+
	// | baz | qux || foobar |
	// |-----|-----||--------|
	// | bar |   0 ||      1 |
	// |     |   1 ||      2 |
	// | foo |   2 ||      3 |
	// | bar |   3 ||      4 |
	// +-----+-----++--------+
	//
	// +-----++--------+
	// | baz || foobar |
	// |-----||--------|
	// | bar ||      2 |
	// |     ||      4 |
	// |     ||      8 |
	// | foo || (null) |
	// +-----++--------+
}

func ExampleGroupedSeries_Apply_align() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []string{"bar", "bar", "foo", "bar"}, []int{0, 1, 2, 3}).
		SetName("foobar").
		SetLabelNames([]string{"baz", "qux"})
	fmt.Println(s)

	g := s.GroupBy("baz")
	// if group has at least 3 items, multiply by 2. otherwise set as null.
	modifyBigGroup := func(slice interface{}, isNull []bool) interface{} {
		vals, _ := slice.([]float64) // in normal usage, check the type assertion and handle an error
		ret := make([]float64, len(vals))
		if len(vals) >= 3 {
			for i := range ret {
				ret[i] = vals[i] * 2
			}
		} else {
			for i := range ret {
				isNull[i] = true
			}
		}
		return ret
	}
	g.Align()
	fmt.Println(g.Apply(modifyBigGroup).Series())

	// Output:
	// +-----+-----++--------+
	// | baz | qux || foobar |
	// |-----|-----||--------|
	// | bar |   0 ||      1 |
	// |     |   1 ||      2 |
	// | foo |   2 ||      3 |
	// | bar |   3 ||      4 |
	// +-----+-----++--------+
	//
	// +-----+-----++--------+
	// | baz | qux || foobar |
	// |-----|-----||--------|
	// | bar |   0 ||      2 |
	// |     |   1 ||      4 |
	// | foo |   2 || (null) |
	// | bar |   3 ||      8 |
	// +-----+-----++--------+
}

func ExampleGroupedSeries_HavingCount_sum() {
	s := tada.NewSeries([]float64{1, 2, 3, 4}, []int{0, 1, 1, 1}).
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
