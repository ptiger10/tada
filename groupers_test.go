package tada

import (
	"errors"
	"fmt"
	"reflect"
	"strings"
	"testing"
	"time"
)

func TestGroupedSeries_Err(t *testing.T) {
	type fields struct {
		groups     map[string]int
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
		{"no error", fields{
			groups: map[string]int{"foo": 0, "bar": 2},
			series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}}, false},
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
			want: &Series{values: &valueContainer{
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
			want: &Series{values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
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
			want: &Series{values: &valueContainer{
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
			want: &Series{values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "mean"},
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
			want: &Series{values: &valueContainer{
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
			want: &Series{values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "median"},
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
			want: &Series{values: &valueContainer{
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
			&Series{values: &valueContainer{
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
			&Series{values: &valueContainer{
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
			&Series{values: &valueContainer{
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
			&Series{values: &valueContainer{
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
				slice: []time.Time{d, time.Time{}}, isNull: []bool{false, true}, name: "custom"},
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
			&Series{values: &valueContainer{
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
			&Series{values: &valueContainer{
				slice: []time.Time{d, d, time.Time{}, time.Time{}}, isNull: []bool{false, false, true, true}, name: "qux_custom"},
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
			if got := g.Apply(tt.args.name, tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

// func TestGroupedSeries_First(t *testing.T) {
// 	type fields struct {
// 		groups map[string]int
// 		series *Series
// 		err    error
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		want   *Series
// 	}{
// 		{name: "single level",
// 			fields: fields{
// 				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
// 				orderedKeys: []string{"foo", "bar"},
// 				levelNames:  []string{"*0"},
// 				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
// 					labels: []*valueContainer{
// 						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
// 			want: &Series{values: &valueContainer{slice: []string{"1", "3"}, isNull: []bool{false, false}, name: "first"},
// 				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			g := GroupedSeries{
// 				groups: tt.fields.groups,
// 				series: tt.fields.series,
// 				err:    tt.fields.err,
// 			}
// 			if got := g.First(); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("GroupedSeries.First() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

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
