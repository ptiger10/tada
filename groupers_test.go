package tada

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/d4l3k/messagediff"
)

func TestGroupedSeries_Err(t *testing.T) {
	type fields struct {
		labels     []*valueContainer
		rowIndices [][]int
		series     *Series
		aligned    bool
		err        error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"error", fields{err: errors.New("foo")}, true},
		{"no error", fields{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				labels:     tt.fields.labels,
				rowIndices: tt.fields.rowIndices,
				series:     tt.fields.series,
				aligned:    tt.fields.aligned,
				err:        tt.fields.err,
			}
			if err := g.Err(); (err != nil) != tt.wantErr {
				t.Errorf("GroupedSeries.Err() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGroupedSeries_GetGroup(t *testing.T) {
	type fields struct {
		rowIndices  [][]int
		orderedKeys []string
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	type args struct {
		group string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{
			name: "single level",
			fields: fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{
					slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"bar"},
			want: &Series{values: &valueContainer{slice: []string{"c", "d"}, isNull: []bool{false, false}, name: "baz"},
				labels: []*valueContainer{{slice: []string{"bar", "bar"}, isNull: []bool{false, false}, name: "*0"}}},
		},
		{
			name: "fail - no group",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{
					slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"corge"},
			want: &Series{err: fmt.Errorf("GetGroup(): `group` (corge) not in groups")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				rowIndices:  tt.fields.rowIndices,
				orderedKeys: tt.fields.orderedKeys,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.GetGroup(tt.args.group); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.GetGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedFloatFunc(t *testing.T) {
	type args struct {
		vals       []float64
		nulls      []bool
		name       string
		aligned    bool
		rowIndices [][]int
		fn         func(val []float64, isNull []bool, index []int) (float64, bool)
	}
	tests := []struct {
		name string
		args args
		want *valueContainer
	}{
		{"grouped", args{
			vals: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(vals []float64, isNull []bool, index []int) (float64, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return 0, true
			}},
			&valueContainer{
				slice:  []float64{1, 3},
				isNull: []bool{false, false},
				name:   "corge",
			},
		},
		{"aligned", args{
			vals: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: true,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(vals []float64, isNull []bool, index []int) (float64, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return 0, true
			}},
			&valueContainer{
				slice:  []float64{1, 1, 3, 3},
				isNull: []bool{false, false, false, false},
				name:   "corge",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupedFloatFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedFloatFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedStringFunc(t *testing.T) {
	type args struct {
		vals       []string
		nulls      []bool
		name       string
		aligned    bool
		rowIndices [][]int
		fn         func(val []string, isNull []bool, index []int) (string, bool)
	}
	tests := []struct {
		name string
		args args
		want *valueContainer
	}{
		{"grouped", args{
			vals: []string{"foo", "qux", "bar", "baz"}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(vals []string, isNull []bool, index []int) (string, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return "", true
			}},
			&valueContainer{
				slice:  []string{"foo", "bar"},
				isNull: []bool{false, false},
				name:   "corge",
			},
		},
		{"aligned", args{
			vals: []string{"foo", "qux", "bar", "baz"}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: true,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(vals []string, isNull []bool, index []int) (string, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return "", true
			}},
			&valueContainer{
				slice:  []string{"foo", "foo", "bar", "bar"},
				isNull: []bool{false, false, false, false},
				name:   "corge",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupedStringFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedStringFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedDateTimeFunc(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type args struct {
		vals       []time.Time
		nulls      []bool
		name       string
		aligned    bool
		rowIndices [][]int
		fn         func(val []time.Time, isNull []bool, index []int) (time.Time, bool)
	}
	tests := []struct {
		name string
		args args
		want *valueContainer
	}{
		{"grouped", args{
			vals: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return time.Time{}, true
			}},
			&valueContainer{
				slice:  []time.Time{d, d.AddDate(0, 0, 2)},
				isNull: []bool{false, false},
				name:   "corge",
			},
		},
		{"aligned", args{
			vals: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: true,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return time.Time{}, true
			}},
			&valueContainer{
				slice:  []time.Time{d, d, d.AddDate(0, 0, 2), d.AddDate(0, 0, 2)},
				isNull: []bool{false, false, false, false},
				name:   "corge",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupedDateTimeFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedDateTimeFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedIndexFunc(t *testing.T) {
	type args struct {
		vals       interface{}
		nulls      []bool
		name       string
		aligned    bool
		index      int
		rowIndices [][]int
	}
	tests := []struct {
		name string
		args args
		want *valueContainer
	}{
		{"grouped", args{
			vals: []int{0, 1, 2, 3}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			index:      0,
			rowIndices: [][]int{{0, 1}, {2, 3}},
		},
			&valueContainer{
				slice:  []int{0, 2},
				isNull: []bool{false, false},
				name:   "corge",
			},
		},
		{"out of range - too high", args{
			vals: []int{0, 1, 2, 3}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			index:      2,
			rowIndices: [][]int{{0}, {1, 2, 3}},
		},
			&valueContainer{
				slice:  []int{0, 3},
				isNull: []bool{true, false},
				name:   "corge",
			},
		},
		{"out of range - too low", args{
			vals: []int{0, 1, 2, 3}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			index:      -2,
			rowIndices: [][]int{{0}, {1, 2, 3}},
		},
			&valueContainer{
				slice:  []int{0, 2},
				isNull: []bool{true, false},
				name:   "corge",
			},
		},
		{"-1", args{
			vals: []int{0, 1, 2, 3}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: false,
			index:      -1,
			rowIndices: [][]int{{0, 1}, {2, 3}},
		},
			&valueContainer{
				slice:  []int{1, 3},
				isNull: []bool{false, false},
				name:   "corge",
			},
		},
		{"aligned", args{
			vals: []string{"foo", "qux", "bar", "baz"}, nulls: []bool{false, false, false, false},
			name: "corge", aligned: true,
			index:      0,
			rowIndices: [][]int{{0, 1}, {2, 3}},
		},
			&valueContainer{
				slice:  []string{"foo", "foo", "bar", "bar"},
				isNull: []bool{false, false, false, false},
				name:   "corge",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupedIndexFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.index, tt.args.rowIndices); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedIndexFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Sum(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{3, 3, 7, 7}, isNull: []bool{false, false, false, false}, name: "qux_sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{
			name: "two levels - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"},
				},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			want: &Series{
				values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Sum(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Mean(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "mean"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{1.5, 1.5, 3.5, 3.5}, isNull: []bool{false, false, false, false}, name: "qux_mean"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{
			name: "two levels - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"},
				},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			want: &Series{
				values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "mean"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Mean(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Mean() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Median(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "median"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{1.5, 1.5, 3.5, 3.5}, isNull: []bool{false, false, false, false}, name: "qux_median"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{
			name: "two levels - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"},
				},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			want: &Series{
				values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "median"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Median(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Median() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Std(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{0.5, 0.5}, isNull: []bool{false, false}, name: "std"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{0.5, 0.5, 0.5, 0.5}, isNull: []bool{false, false, false, false}, name: "qux_std"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{
			name: "two levels - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"},
				},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			want: &Series{values: &valueContainer{slice: []float64{0.5, 0.5}, isNull: []bool{false, false}, name: "std"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Std(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Std() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Min(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "min"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{1, 1, 3, 3}, isNull: []bool{false, false, false, false}, name: "qux_min"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Min(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Max(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{2, 4}, isNull: []bool{false, false}, name: "max"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{2, 2, 4, 4}, isNull: []bool{false, false, false, false}, name: "qux_max"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Max(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Count(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{2, 2}, isNull: []bool{false, false}, name: "count"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{2, 2, 2, 2}, isNull: []bool{false, false, false, false}, name: "qux_count"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Count(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_First(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "first"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []string{"a", "a", "c", "c"}, isNull: []bool{false, false, false, false}, name: "qux_first"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.First(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.First() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Last(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []string{"b", "d"}, isNull: []bool{false, false}, name: "last"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []string{"b", "b", "d", "d"}, isNull: []bool{false, false, false, false}, name: "qux_last"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Last(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Last() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Earliest(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "earliest"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []time.Time{d, d, d.AddDate(0, 0, 2), d.AddDate(0, 0, 2)}, isNull: []bool{false, false, false, false}, name: "qux_earliest"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Earliest(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Earliest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Latest(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 3)}, isNull: []bool{false, false}, name: "latest"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 1), d.AddDate(0, 0, 3), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux_latest"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Latest(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Latest() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestGroupedSeries_Apply(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	type args struct {
		name   string
		lambda GroupApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		// -- float64
		{"no nulls - not aligned - float", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{F64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&Series{values: &valueContainer{
				slice: []float64{3, 7}, isNull: []bool{false, false}, name: "custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"nulls - not aligned - float", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{F64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&Series{values: &valueContainer{
				slice: []float64{3, 0}, isNull: []bool{false, true}, name: "custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"no nulls - aligned - float", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     true,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{F64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{3, 3, 7, 7}, isNull: []bool{false, false, false, false}, name: "qux_custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{"nulls - aligned - float", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     true,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{F64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&Series{
				sharedData: true,
				values: &valueContainer{
					slice: []float64{3, 3, 0, 0}, isNull: []bool{false, false, true, true}, name: "qux_custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		// -- string
		{"no nulls - not aligned - string", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&Series{values: &valueContainer{
				slice: []string{"A", "C"}, isNull: []bool{false, false}, name: "custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"nulls - not aligned - string", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&Series{values: &valueContainer{
				slice: []string{"A", ""}, isNull: []bool{false, true}, name: "custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"no nulls - aligned - string", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     true,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&Series{
				sharedData: true,
				values: &valueContainer{
					slice: []string{"A", "A", "C", "C"}, isNull: []bool{false, false, false, false}, name: "qux_custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{"nulls - aligned - string", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     true,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&Series{
				sharedData: true,
				values: &valueContainer{
					slice: []string{"A", "A", "", ""}, isNull: []bool{false, false, true, true}, name: "qux_custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		// -- datetime
		{"no nulls - not aligned - datetime", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&Series{values: &valueContainer{
				slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"nulls - not aligned - datetime", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&Series{values: &valueContainer{
				slice: []time.Time{d, {}}, isNull: []bool{false, true}, name: "custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"no nulls - aligned - datetime", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     true,
			series: &Series{
				values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&Series{
				sharedData: true,
				values: &valueContainer{
					slice: []time.Time{d, d, d.AddDate(0, 0, 2), d.AddDate(0, 0, 2)}, isNull: []bool{false, false, false, false}, name: "qux_custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{"nulls - aligned - datetime", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     true,
			series: &Series{
				values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&Series{
				sharedData: true,
				values: &valueContainer{
					slice: []time.Time{d, d, {}, {}}, isNull: []bool{false, false, true, true}, name: "qux_custom"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{"fail", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupApplyFn{}},
			&Series{err: fmt.Errorf("Apply(): no lambda function provided")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Apply(tt.args.name, tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Apply(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		name   string
		cols   []string
		lambda GroupApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		// -- float64
		{"no nulls - not aligned - float", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{
				values: []*valueContainer{{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "foo"}},
			args{"custom", []string{"qux"}, GroupApplyFn{F64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom"},
		},

		// -- string
		{"no nulls - not aligned - string", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{
				values: []*valueContainer{{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "foo"}},
			args{"custom", []string{"qux"}, GroupApplyFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"A", "C"}, isNull: []bool{false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom"}},

		// -- datetime
		{"no nulls - not aligned - datetime", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{
				values: []*valueContainer{{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "foo"}},
			args{"custom", []string{"qux"}, GroupApplyFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom"}},

		{"fail", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{
				values: []*valueContainer{{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "foo"}},
			args{"custom", []string{"qux"}, GroupApplyFn{}},
			&DataFrame{err: fmt.Errorf("Apply(): no lambda function provided")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Apply(tt.args.name, tt.args.cols, tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestGroupedSeries_Align(t *testing.T) {
	type fields struct {
		series  *Series
		aligned bool
		err     error
	}
	tests := []struct {
		name   string
		fields fields
		want   *GroupedSeries
	}{
		{"pass", fields{aligned: false}, &GroupedSeries{aligned: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				series:  tt.fields.series,
				aligned: tt.fields.aligned,
				err:     tt.fields.err,
			}
			if got := g.Align(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Align() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_floatFunc(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		name string
		cols []string
		fn   func(val []float64, isNull []bool, index []int) (float64, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels:     []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", []string{"qux"}, func(vals []float64, isNull []bool, index []int) (float64, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return 0, true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "qux"}},
				labels:        []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "first"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.floatFunc(tt.args.name, tt.args.cols, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.floatFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_stringFunc(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		name string
		cols []string
		fn   func(val []string, isNull []bool, index []int) (string, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels:     []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", []string{"qux"}, func(vals []string, isNull []bool, index []int) (string, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return "", true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "qux"}},
				labels:        []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "first"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.stringFunc(tt.args.name, tt.args.cols, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.stringFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_dateTimeFunc(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		name string
		cols []string
		fn   func(val []time.Time, isNull []bool, index []int) (time.Time, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels:     []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "qux"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", []string{"qux"}, func(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return time.Time{}, true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "qux"}},
				labels:        []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "first"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.dateTimeFunc(tt.args.name, tt.args.cols, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.dateTimeFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Sum(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{11, 15}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "sum",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Sum(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Mean(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{5.5, 7.5}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "mean",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Mean(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Mean() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Median(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{5.5, 7.5}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "median",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Median(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Median() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Std(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{0.5, 0.5}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{0.5, 0.5}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "std",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Std(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Std() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Count(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{2, 2}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{2, 2}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "count",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Count(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Min(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{5, 7}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "min",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Min(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Max(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{2, 4}, isNull: []bool{false, false}, name: "corge"},
					{slice: []float64{6, 8}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "max",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Max(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_First(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []string{"e", "f", "g", "h"}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "corge"},
					{slice: []string{"e", "g"}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "first",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.First(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.First() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Last(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []string{"e", "f", "g", "h"}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []string{"b", "d"}, isNull: []bool{false, false}, name: "corge"},
					{slice: []string{"f", "h"}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "last",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Last(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Last() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Earliest(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "corge"},
					{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "earliest",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Earliest(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Earliest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Latest(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level, all colNames",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 3)}, isNull: []bool{false, false}, name: "corge"},
					{slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 3)}, isNull: []bool{false, false}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "latest",
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Latest(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Latest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Col(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		colName string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedSeries
	}{
		{
			name: "pass",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{"qux"},
			want: &GroupedSeries{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					sharedData: true,
				},
			}},
		{name: "fail - bad column",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "quux"},
						{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{"corge"},
			want: &GroupedSeries{
				err: fmt.Errorf("Col(): `name` (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.Col(tt.args.colName); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Col() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Err(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"error", fields{err: errors.New("foo")}, true},
		{"no error", fields{}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if err := g.Err(); (err != nil) != tt.wantErr {
				t.Errorf("GroupedDataFrame.Err() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestSeries_RollingN(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		n int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedSeries
	}{
		{"pass", fields{values: &valueContainer{
			slice: []float64{1, 0, 0, 4}, isNull: []bool{false, true, true, false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			}}, args{2},
			&GroupedSeries{
				rowIndices: [][]int{{0, 1}, {1, 2}, {2, 3}, {}},
				aligned:    true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 0, 0, 4}, isNull: []bool{false, true, true, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}}},
			}},
		{"fail", fields{values: &valueContainer{
			slice: []float64{1, 0, 0, 4}, isNull: []bool{false, true, true, false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			}}, args{0},
			&GroupedSeries{
				err: fmt.Errorf("RollingN(): `n` must be greater than zero (not 0)"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.RollingN(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.RollingN() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestSeries_RollingDuration(t *testing.T) {
	d1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	d2 := d1.AddDate(0, 0, 3)
	d3 := d1.AddDate(0, 0, 4)
	d4 := d1.AddDate(0, 0, 9)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		d time.Duration
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedSeries
	}{
		{"pass", fields{values: &valueContainer{slice: []time.Time{d1, d2, d3, d4}, isNull: []bool{false, false, false, false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			}}, args{5 * 24 * time.Hour},
			&GroupedSeries{
				rowIndices: [][]int{{0, 1, 2}, {1, 2}, {2}, {3}},
				aligned:    true,
				series: &Series{
					values: &valueContainer{slice: []time.Time{d1, d2, d3, d4}, isNull: []bool{false, false, false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}}},
			}},
		{"fail", fields{values: &valueContainer{slice: []time.Time{d1, d2, d3, d4}, isNull: []bool{false, false, false, false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			}}, args{-1},
			&GroupedSeries{
				err: fmt.Errorf("RollingDuration(): `d` must be greater than zero (not -1ns)"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.RollingDuration(tt.args.d); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.RollingDuration() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_HavingCount(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	type args struct {
		lambda func(int) bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedSeries
	}{
		{name: "pass",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0}, {1, 2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{func(v int) bool {
				if v >= 2 {
					return true
				}
				return false
			}},
			want: &GroupedSeries{
				orderedKeys: []string{"bar"},
				rowIndices:  [][]int{{1, 2, 3}},
				labels: []*valueContainer{
					{slice: []string{"bar"}, isNull: []bool{false}, name: "*0"},
				},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
					}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.HavingCount(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.HavingCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_HavingCount(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		lambda func(int) bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedDataFrame
	}{
		{name: "pass",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0}, {1, 2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{values: []*valueContainer{{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{func(v int) bool {
				if v >= 2 {
					return true
				}
				return false
			}},
			want: &GroupedDataFrame{
				orderedKeys: []string{"bar"},
				rowIndices:  [][]int{{1, 2, 3}},
				labels: []*valueContainer{
					{slice: []string{"bar"}, isNull: []bool{false}, name: "*0"},
				},
				df: &DataFrame{values: []*valueContainer{{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
					}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.HavingCount(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.HavingCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_GetGroup(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		group string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{
			name: "single level",
			fields: fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{values: []*valueContainer{
					{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}}},
			args: args{"bar"},
			want: &DataFrame{values: []*valueContainer{{slice: []string{"c", "d"}, isNull: []bool{false, false}, name: "baz"}},
				labels:        []*valueContainer{{slice: []string{"bar", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.GetGroup(tt.args.group); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.GetGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_IterGroups(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   []*Series
	}{
		{fields: fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: []*Series{
				{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, name: "*0"}}},
				{values: &valueContainer{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"bar", "bar"}, isNull: []bool{false, false}, name: "*0"}}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.IterGroups(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.IterGroups() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_ListGroups(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"pass", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			[]string{"foo", "bar"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.ListGroups(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.ListGroups() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_IterGroups(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   []*DataFrame
	}{
		{name: "single level",
			fields: fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{values: []*valueContainer{
					{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}}},
			want: []*DataFrame{
				{values: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "baz"}},
					labels:        []*valueContainer{{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}},
				{values: []*valueContainer{{slice: []string{"c", "d"}, isNull: []bool{false, false}, name: "baz"}},
					labels:        []*valueContainer{{slice: []string{"bar", "bar"}, isNull: []bool{false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.IterGroups(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.IterGroups() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_ListGroups(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{name: "single level",
			fields: fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{values: []*valueContainer{
					{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}}},
			want: []string{"foo", "bar"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				df:          tt.fields.df,
				err:         tt.fields.err,
			}
			if got := g.ListGroups(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.ListGroups() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Nth(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	type args struct {
		index int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{
			name: "1st position - includes null",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0}, {1, 2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{1},
			want: &Series{values: &valueContainer{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "nth"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Nth(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Nth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_UniqueList(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass",
			fields{
				orderedKeys: []string{"foo", "bar", "baz"},
				rowIndices:  [][]int{{0, 1}, {2, 3}, {4}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []string{"a", "a", "c", "d", ""}, isNull: []bool{false, false, false, false, true}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar", "baz"}, isNull: []bool{false, false, false, false, false}, name: "*0"}}}},
			&Series{values: &valueContainer{slice: [][]string{{"a"}, {"c", "d"}, nil}, isNull: []bool{false, false, true}, name: "unique"},
				labels: []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				orderedKeys: tt.fields.orderedKeys,
				rowIndices:  tt.fields.rowIndices,
				labels:      tt.fields.labels,
				series:      tt.fields.series,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.UniqueList(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.UniqueList() = %v, want %v", got, tt.want)
			}
		})
	}
}
