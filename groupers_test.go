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
			want: &Series{err: fmt.Errorf("getting group: group (corge) not in groups")},
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
			if got := g.GetGroup(tt.args.group); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.GetGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedInterfaceFunc(t *testing.T) {
	type args struct {
		slice      interface{}
		nulls      []bool
		name       string
		aligned    bool
		rowIndices [][]int
		fn         func(slice interface{}) interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"grouped", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "foo", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				return vals[0]
			}},
			&valueContainer{
				slice:  []float64{1, 3},
				isNull: []bool{false, false},
				name:   "foo",
			},
			false},
		{"grouped - new type", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "foo", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				return fmt.Sprintf("%.2f", vals[0])
			}},
			&valueContainer{
				slice:  []string{"1.00", "3.00"},
				isNull: []bool{false, false},
				name:   "foo",
			},
			false},
		{"grouped and nested", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "foo", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				return vals
			}},
			&valueContainer{
				slice:  [][]float64{{1, 2}, {3, 4}},
				isNull: []bool{false, false},
				name:   "foo",
			},
			false},
		{"aligned", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "foo", aligned: true,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				return vals[0]
			}},
			&valueContainer{
				slice:  []float64{1, 1, 3, 3},
				isNull: []bool{false, false, false, false},
				name:   "foo",
			},
			false},
		{"fail - unsupported", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name: "foo", aligned: false,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				return [][][]float64{{{vals[0]}}}
			}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := groupedInterfaceReduceFunc(tt.args.slice, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("groupedInterfaceReduceFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedInterfaceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedFloat64Func(t *testing.T) {
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
			name:       "corge",
			aligned:    false,
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
			name:       "corge",
			aligned:    true,
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
			if got := groupedFloat64ReduceFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedFloat64ReduceFunc() = %v, want %v", got, tt.want)
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
			name:       "corge",
			aligned:    false,
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
			name:       "corge",
			aligned:    true,
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
			if got := groupedStringReduceFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedStringReduceFunc() = %v, want %v", got, tt.want)
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
			name:       "corge",
			aligned:    false,
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
			name:       "corge",
			aligned:    true,
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
			if got := groupedDateTimeReduceFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedDateTimeReduceFunc() = %v, want %v", got, tt.want)
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
			if got := groupedIndexReduceFunc(tt.args.vals, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.index, tt.args.rowIndices); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedIndexReduceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_stringReduceFunc(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	type args struct {
		name string
		fn   func(slice []string, isNull []bool, index []int) (string, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{
			name: "grouped",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				aligned:     false,
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"foo", func([]string, []bool, []int) (string, bool) { return "foo", false }},
			want: &Series{
				values: &valueContainer{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}},
		},
		{
			name: "aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				aligned:     true,
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"foo", func([]string, []bool, []int) (string, bool) { return "foo", false }},
			want: &Series{
				values:     &valueContainer{slice: []string{"foo", "foo", "foo", "foo"}, isNull: []bool{false, false, false, false}, name: "foo_foo"},
				labels:     []*valueContainer{{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				sharedData: true},
		},
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
			if got := g.stringReduceFunc(tt.args.name, tt.args.fn); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.stringReduceFunc() = %v, want %v", got, tt.want)
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
					slice: []float64{3, 3, 7, 7}, isNull: []bool{false, false, false, false}, name: "sum_qux"},
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
			if got := g.Sum(); !EqualSeries(got, tt.want) {
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
					slice: []float64{1.5, 1.5, 3.5, 3.5}, isNull: []bool{false, false, false, false}, name: "mean_qux"},
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
			if got := g.Mean(); !EqualSeries(got, tt.want) {
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
					slice: []float64{1.5, 1.5, 3.5, 3.5}, isNull: []bool{false, false, false, false}, name: "median_qux"},
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
			if got := g.Median(); !EqualSeries(got, tt.want) {
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
					slice: []float64{0.5, 0.5, 0.5, 0.5}, isNull: []bool{false, false, false, false}, name: "std_qux"},
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
			if got := g.StdDev(); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.StdDev() = %v, want %v", got, tt.want)
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
					slice: []float64{1, 1, 3, 3}, isNull: []bool{false, false, false, false}, name: "min_qux"},
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
			if got := g.Min(); !EqualSeries(got, tt.want) {
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
					slice: []float64{2, 2, 4, 4}, isNull: []bool{false, false, false, false}, name: "max_qux"},
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
			if got := g.Max(); !EqualSeries(got, tt.want) {
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
			want: &Series{values: &valueContainer{slice: []int{2, 2}, isNull: []bool{false, false}, name: "count"},
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
					slice: []int{2, 2, 2, 2}, isNull: []bool{false, false, false, false}, name: "count_qux"},
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
			if got := g.Count(); !EqualSeries(got, tt.want) {
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
					slice: []string{"a", "a", "c", "c"}, isNull: []bool{false, false, false, false}, name: "first_qux"},
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
			if got := g.First(); !EqualSeries(got, tt.want) {
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
					slice: []string{"b", "b", "d", "d"}, isNull: []bool{false, false, false, false}, name: "last_qux"},
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
			if got := g.Last(); !EqualSeries(got, tt.want) {
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
					slice: []time.Time{d, d, d.AddDate(0, 0, 2), d.AddDate(0, 0, 2)}, isNull: []bool{false, false, false, false}, name: "earliest_qux"},
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
			if got := g.Earliest(); !EqualSeries(got, tt.want) {
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
					slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 1), d.AddDate(0, 0, 3), d.AddDate(0, 0, 3)}, isNull: []bool{false, false, false, false}, name: "latest_qux"},
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
			if got := g.Latest(); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.Latest() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestGroupedSeries_Reduce(t *testing.T) {
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
		lambda GroupReduceFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		// -- float64
		{"not aligned - float", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 0, 0}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupReduceFn{Float64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&Series{values: &valueContainer{
				slice: []float64{3, 0}, isNull: []bool{false, true}, name: "custom_qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		// -- string
		{"not aligned - string", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupReduceFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&Series{values: &valueContainer{
				slice: []string{"A", ""}, isNull: []bool{false, true}, name: "custom_qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		// -- datetime
		{" not aligned - datetime", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []time.Time{d, d.AddDate(0, 0, 1), {}, {}}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupReduceFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&Series{values: &valueContainer{
				slice: []time.Time{d, {}}, isNull: []bool{false, true}, name: "custom_qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		// -- interface
		{"not aligned - interface", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupReduceFn{Interface: func(vals interface{}) interface{} {
				arr := vals.([]string)
				return strings.ToUpper(arr[0])
			}}},
			&Series{values: &valueContainer{
				slice: []string{"A", ""}, isNull: []bool{false, true}, name: "custom_qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
				}}},
		{"not aligned - interface - error", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupReduceFn{Interface: func(vals interface{}) interface{} {
				return complex64(1)
			}}},
			&Series{err: fmt.Errorf("reducing grouped Series: interface{} output: unable to calculate null values ([]complex64 not supported)")}},
		// -- no function
		{"fail", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			aligned:     false,
			series: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", GroupReduceFn{}},
			&Series{err: fmt.Errorf("reducing grouped Series: no lambda function provided")}},
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
			if got := g.Reduce(tt.args.name, tt.args.lambda); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.Reduce() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Reduce(t *testing.T) {
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
		lambda GroupReduceFn
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
			args{"custom", []string{"qux"}, GroupReduceFn{Float64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "custom_qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom_foo"},
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
			args{"custom", []string{"qux"}, GroupReduceFn{String: func(vals []string) string {
				return strings.ToUpper(vals[0])
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"A", "C"}, isNull: []bool{false, false}, name: "custom_qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom_foo"}},

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
			args{"custom", []string{"qux"}, GroupReduceFn{DateTime: func(vals []time.Time) time.Time {
				return vals[0]
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "custom_qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom_foo"}},

		// -- interface
		{"no nulls - not aligned - interface", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{
				values: []*valueContainer{{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "foo"}},
			args{"custom", []string{"qux"}, GroupReduceFn{Interface: func(vals interface{}) interface{} {
				arr := vals.([]string)
				return strings.ToUpper(arr[0])
			}}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"A", "C"}, isNull: []bool{false, false}, name: "custom_qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "custom_foo"}},
		{"no nulls - not aligned - interface error", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{
				values: []*valueContainer{{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "foo"}},
			args{"custom", []string{"qux"}, GroupReduceFn{Interface: func(vals interface{}) interface{} {
				return complex64(1)
			}}},
			&DataFrame{err: fmt.Errorf("reducing grouped DataFrame: interface{} output: unable to calculate null values ([]complex64 not supported)")}},

		// -- no function
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
			args{"custom", []string{"qux"}, GroupReduceFn{}},
			&DataFrame{err: fmt.Errorf("reducing grouped DataFrame: no lambda function provided")}},
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
			if got := g.Reduce(tt.args.name, tt.args.cols, tt.args.lambda); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.Reduce() = %v, want %v", got, tt.want)
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
			if got := g.Align(); !equalGroupedSeries(got, tt.want) {
				t.Errorf("GroupedSeries.Align() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_float64ReduceFunc(t *testing.T) {
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
			args: args{"first", nil, func(vals []float64, isNull []bool, index []int) (float64, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return 0, true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "first_qux"}},
				labels:        []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "first"},
		},
		{
			name: "select one specific column (and not the first)",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels:     []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []string{"foo", "quux", "baz", "bar"}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "waldo"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", []string{"waldo"}, func(vals []float64, isNull []bool, index []int) (float64, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return 0, true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "first_waldo"}},
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
			if got := g.float64ReduceFunc(tt.args.name, tt.args.cols, tt.args.fn); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.float64ReduceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_stringReduceFunc(t *testing.T) {
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
			args: args{"first", nil, func(vals []string, isNull []bool, index []int) (string, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return "", true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "first_qux"}},
				labels:        []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "first"},
		},
		{
			name: "select one specific column (and not the first)",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels:     []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []string{"foo", "quux", "baz", "bar"}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "waldo"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", []string{"waldo"}, func(vals []string, isNull []bool, index []int) (string, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return "", true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []string{"1", "3"}, isNull: []bool{false, false}, name: "first_waldo"}},
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
			if got := g.stringReduceFunc(tt.args.name, tt.args.cols, tt.args.fn); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.stringReduceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_dateTimeReduceFunc(t *testing.T) {
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
			args: args{"first", nil, func(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return time.Time{}, true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "first_qux"}},
				labels:        []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "first"},
		},
		{
			name: "select one specific column (and not the first)",
			fields: fields{
				rowIndices: [][]int{{0, 1}, {2, 3}},
				labels:     []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []string{"foo", "quux", "baz", "bar"}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []time.Time{d, d, d.AddDate(0, 0, 1), d}, isNull: []bool{false, false, false, false}, name: "waldo"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", []string{"waldo"}, func(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return time.Time{}, true
			}},
			want: &DataFrame{values: []*valueContainer{
				{slice: []time.Time{d, d.AddDate(0, 0, 1)}, isNull: []bool{false, false}, name: "first_waldo"}},
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
			if got := g.dateTimeReduceFunc(tt.args.name, tt.args.cols, tt.args.fn); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.dateTimeReduceFunc() = %v, want %v", got, tt.want)
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
					{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum_corge"},
					{slice: []float64{11, 15}, isNull: []bool{false, false}, name: "sum_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "sum_qux",
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
			if got := g.Sum(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "mean_corge"},
					{slice: []float64{5.5, 7.5}, isNull: []bool{false, false}, name: "mean_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "mean_qux",
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
			if got := g.Mean(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "median_corge"},
					{slice: []float64{5.5, 7.5}, isNull: []bool{false, false}, name: "median_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "median_qux",
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
			if got := g.Median(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []float64{0.5, 0.5}, isNull: []bool{false, false}, name: "std_corge"},
					{slice: []float64{0.5, 0.5}, isNull: []bool{false, false}, name: "std_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "std_qux",
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
			if got := g.StdDev(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.StdDev() = %v, want %v", got, tt.want)
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
					{slice: []int{2, 2}, isNull: []bool{false, false}, name: "count_corge"},
					{slice: []int{2, 2}, isNull: []bool{false, false}, name: "count_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "count_qux",
			}},
		{
			name: "select column (not first)",
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
			args: args{[]string{"waldo"}},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []int{2, 2}, isNull: []bool{false, false}, name: "count_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "count_qux",
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
			if got := g.Count(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "min_corge"},
					{slice: []float64{5, 7}, isNull: []bool{false, false}, name: "min_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "min_qux",
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
			if got := g.Min(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []float64{2, 4}, isNull: []bool{false, false}, name: "max_corge"},
					{slice: []float64{6, 8}, isNull: []bool{false, false}, name: "max_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "max_qux",
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
			if got := g.Max(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "first_corge"},
					{slice: []string{"e", "g"}, isNull: []bool{false, false}, name: "first_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "first_qux",
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
			if got := g.First(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []string{"b", "d"}, isNull: []bool{false, false}, name: "last_corge"},
					{slice: []string{"f", "h"}, isNull: []bool{false, false}, name: "last_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "last_qux",
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
			if got := g.Last(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "earliest_corge"},
					{slice: []time.Time{d, d.AddDate(0, 0, 2)}, isNull: []bool{false, false}, name: "earliest_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "earliest_qux",
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
			if got := g.Earliest(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
					{slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 3)}, isNull: []bool{false, false}, name: "latest_corge"},
					{slice: []time.Time{d.AddDate(0, 0, 1), d.AddDate(0, 0, 3)}, isNull: []bool{false, false}, name: "latest_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "latest_qux",
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
			if got := g.Latest(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
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
				err: fmt.Errorf("getting column from grouped Series: name (corge) not found")}},
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
			if got := g.Col(tt.args.colName); !equalGroupedSeries(got, tt.want) {
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
				err: fmt.Errorf("rolling n: n must be greater than zero (not 0)"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.RollingN(tt.args.n); !equalGroupedSeries(got, tt.want) {
				t.Errorf("Series.RollingN() = %v, want %v", got, tt.want)
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
				err: fmt.Errorf("rolling duration: d must be greater than zero (not -1ns)"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.RollingDuration(tt.args.d); !equalGroupedSeries(got, tt.want) {
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
			args: args{func(v int) bool { return v >= 2 }},
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
			if got := g.HavingCount(tt.args.lambda); !equalGroupedSeries(got, tt.want) {
				t.Errorf("GroupedSeries.HavingCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Series(t *testing.T) {
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
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 2}, {1, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			&Series{values: &valueContainer{slice: []float64{1, 3, 2, 4}, isNull: []bool{false, false, false, false}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
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
			if got := g.Series(); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.Series() = %v, want %v", got, tt.want)
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
			args: args{func(v int) bool { return v >= 2 }},
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
			if got := g.HavingCount(tt.args.lambda); !equalGroupedDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.HavingCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_DataFrame(t *testing.T) {
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
		want   *DataFrame
	}{
		{name: "pass",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 2}, {1, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{values: []*valueContainer{{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &DataFrame{values: []*valueContainer{{slice: []float64{1, 3, 2, 4}, isNull: []bool{false, false, false, false}}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
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
			if got := g.DataFrame(); EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.DataFrame() = %v, want %v", got, tt.want)
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
		{name: "fail",
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
			args: args{"corge"},
			want: &DataFrame{err: fmt.Errorf("getting group: group (corge) not in groups")},
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
			if got := g.GetGroup(tt.args.group); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.GetGroup() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Iterator(t *testing.T) {
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
		want   *GroupedSeriesIterator
	}{
		{"pass",
			fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			&GroupedSeriesIterator{
				current:    -1,
				rowIndices: [][]int{{0, 1}, {2, 3}},
				s: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}},
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
			got := g.Iterator()
			if !reflect.DeepEqual(got.current, tt.want.current) {
				t.Errorf("GroupedSeries.Iterator() = %v, want %v", got.current, tt.want.current)
			}
			if !reflect.DeepEqual(got.rowIndices, tt.want.rowIndices) {
				t.Errorf("GroupedSeries.Iterator() = %v, want %v", got.rowIndices, tt.want.rowIndices)
			}
			if !EqualSeries(got.s, tt.want.s) {
				t.Errorf("GroupedSeries.Iterator() = %v, want %v", got.s, tt.want.s)
			}
		})
	}
}

func TestGroupedSeriesIterator_Next(t *testing.T) {
	type fields struct {
		current    int
		rowIndices [][]int
		s          *Series
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"not at end", fields{current: -1,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			s: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}},
		}, true},
		{"at end", fields{current: 1,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			s: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeriesIterator{
				current:    tt.fields.current,
				rowIndices: tt.fields.rowIndices,
				s:          tt.fields.s,
			}
			if got := g.Next(); got != tt.want {
				t.Errorf("GroupedSeriesIterator.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeriesIterator_Series(t *testing.T) {
	type fields struct {
		current    int
		rowIndices [][]int
		s          *Series
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{current: 0,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			s: &Series{
				values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}},
		}, &Series{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
			labels: []*valueContainer{
				{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, name: "*0"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeriesIterator{
				current:    tt.fields.current,
				rowIndices: tt.fields.rowIndices,
				s:          tt.fields.s,
			}
			if got := g.Series(); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeriesIterator.Series() = %v, want %v", got, tt.want)
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

func TestGroupedDataFrame_Iterator(t *testing.T) {
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
		want   *GroupedDataFrameIterator
	}{
		{"pass",
			fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				df: &DataFrame{values: []*valueContainer{
					{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}}},
			&GroupedDataFrameIterator{
				current:    -1,
				rowIndices: [][]int{{0, 1}, {2, 3}},
				df: &DataFrame{values: []*valueContainer{
					{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
					colLevelNames: []string{"*0"}}},
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
			got := g.Iterator()
			if !reflect.DeepEqual(got.current, tt.want.current) {
				t.Errorf("GroupedDataFrame.Iterator() = %v, want %v", got.current, tt.want.current)
			}
			if !reflect.DeepEqual(got.rowIndices, tt.want.rowIndices) {
				t.Errorf("GroupedDataFrame.Iterator() = %v, want %v", got.rowIndices, tt.want.rowIndices)
			}
			if !EqualDataFrames(got.df, tt.want.df) {
				t.Errorf("GroupedDataFrame.Iterator() = %v, want %v", got.df, tt.want.df)
			}
		})
	}
}

func TestGroupedDataFrameIterator_Next(t *testing.T) {
	type fields struct {
		current    int
		rowIndices [][]int
		df         *DataFrame
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"not at end", fields{current: -1,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			df: &DataFrame{values: []*valueContainer{
				{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		}, true},
		{"at end", fields{current: 1,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			df: &DataFrame{values: []*valueContainer{
				{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrameIterator{
				current:    tt.fields.current,
				rowIndices: tt.fields.rowIndices,
				df:         tt.fields.df,
			}
			if got := g.Next(); got != tt.want {
				t.Errorf("GroupedDataFrameIterator.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrameIterator_DataFrame(t *testing.T) {
	type fields struct {
		current    int
		rowIndices [][]int
		df         *DataFrame
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrame
	}{
		{"pass", fields{current: 0,
			rowIndices: [][]int{{0, 1}, {2, 3}},
			df: &DataFrame{values: []*valueContainer{
				{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		}, &DataFrame{values: []*valueContainer{
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "baz"}},
			labels: []*valueContainer{
				{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrameIterator{
				current:    tt.fields.current,
				rowIndices: tt.fields.rowIndices,
				df:         tt.fields.df,
			}
			if got := g.DataFrame(); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrameIterator.DataFrame() = %v, want %v", got, tt.want)
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
			if got := g.Nth(tt.args.index); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.Nth() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_GetLabels(t *testing.T) {
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
		want   []interface{}
	}{
		{
			name: "pass",
			fields: fields{
				orderedKeys: []string{"foo|0", "bar|0"},
				rowIndices:  [][]int{{0}, {1, 2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"},
				},
				series: &Series{values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			want: []interface{}{
				[]string{"foo", "bar"},
				[]int{0, 0},
			},
		},
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
			if got := g.GetLabels(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.GetLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_GetLabels(t *testing.T) {
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
		want   []interface{}
	}{
		{"preserve type",
			fields{
				orderedKeys: []string{"foo|0", "bar|0"},
				rowIndices:  [][]int{{0}, {1, 2, 3}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{0, 0}, isNull: []bool{false, false}, name: "*1"},
				},
				df: &DataFrame{values: []*valueContainer{{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}}},
					labels: []*valueContainer{
						{slice: []string{"foo", "bar", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			[]interface{}{
				[]string{"foo", "bar"},
				[]int{0, 0}},
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
			if got := g.GetLabels(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.GetLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_interfaceReduceFunc(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		series      *Series
		aligned     bool
		err         error
	}
	type args struct {
		name string
		fn   func(interface{}) interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Series
		wantErr bool
	}{
		{"grouped",
			fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"foo", func(vals interface{}) interface{} {
				v := vals.([]float64)
				return v[0]
			}},
			&Series{values: &valueContainer{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}},
			false},
		{"aligned",
			fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"custom", func(vals interface{}) interface{} {
				v := vals.([]float64)
				return v[0]
			}},
			&Series{values: &valueContainer{slice: []float64{1, 1, 3, 3}, isNull: []bool{false, false, false, false}, name: "custom_foo"},
				labels:     []*valueContainer{{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}},
				sharedData: true},
			false},
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
			got, err := g.interfaceReduceFunc(tt.args.name, tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("GroupedSeries.interfaceReduceFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.interfaceReduceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupedInterfaceTransformFunc(t *testing.T) {
	type args struct {
		slice      interface{}
		nulls      []bool
		name       string
		rowIndices [][]int
		fn         func(slice interface{}) interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"pass", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name:       "foo",
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				ret := make([]float64, len(vals))
				for i := range vals {
					ret[i] = vals[i] - 1
				}
				return ret
			}},
			&valueContainer{
				slice:  []float64{0, 1, 2, 3},
				isNull: []bool{false, false, false, false},
				name:   "foo",
			},
			false},
		{"pass - new type", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name:       "foo",
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				ret := make([]string, len(vals))
				for i := range vals {
					ret[i] = fmt.Sprintf("%.0f", vals[i])
				}
				return ret
			}},
			&valueContainer{
				slice:  []string{"1", "2", "3", "4"},
				isNull: []bool{false, false, false, false},
				name:   "foo",
			},
			false},
		{"fail - wrong length", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name:       "foo",
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				return []float64{0}
			}},
			nil,
			true},
		{"fail - not slice", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name:       "foo",
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				return 0
			}},
			nil,
			true},
		{"fail - not slice on not-first group", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name:       "foo",
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				vals := slice.([]float64)
				if vals[0] != 1 {
					return 0
				}
				return []float64{1, 2}
			}},
			nil,
			true},
		{"fail - unsupported type", args{
			slice: []float64{1, 2, 3, 4}, nulls: []bool{false, false, false, false},
			name:       "foo",
			rowIndices: [][]int{{0, 1}, {2, 3}},
			fn: func(slice interface{}) interface{} {
				return []complex64{1, 2}
			}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := groupedInterfaceTransformFunc(tt.args.slice, tt.args.name, tt.args.rowIndices, tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("groupedInterfaceTransformFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedInterfaceTransformFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Transform(t *testing.T) {
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
		lambda func(interface{}) interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{
					slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{name: "foo", lambda: func(slice interface{}) interface{} {
				vals := slice.([]string)
				ret := make([]int, len(vals))
				for i := range vals {
					ret[i] = i
				}
				return ret
			}},
			&Series{values: &valueContainer{slice: []int{0, 1, 0, 1}, isNull: []bool{false, false, false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
		{"fail",
			fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{
					slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "baz"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{name: "foo", lambda: func(slice interface{}) interface{} {
				return "foo"
			}},
			&Series{err: fmt.Errorf("transforming grouped Series: group 0: output must be slice (string != slice)")}},
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
			if got := g.Transform(tt.args.name, tt.args.lambda); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.Transform() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_NUnique(t *testing.T) {
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
				series: &Series{values: &valueContainer{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []int{1, 2}, isNull: []bool{false, false}, name: "nunique"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{
				sharedData: true,
				values: &valueContainer{
					slice: []int{1, 1, 2, 2}, isNull: []bool{false, false, false, false}, name: "nunique_qux"},
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
			if got := g.NUnique(); !EqualSeries(got, tt.want) {
				t.Errorf("GroupedSeries.NUnique() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Len(t *testing.T) {
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
		want   int
	}{
		{
			"pass",
			fields{
				orderedKeys: []string{"foo", "bar"},
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				series: &Series{values: &valueContainer{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			2},
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
			if got := g.Len(); got != tt.want {
				t.Errorf("GroupedSeries.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Len(t *testing.T) {
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
		want   int
	}{
		{"pass", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{values: []*valueContainer{{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			2},
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
			if got := g.Len(); got != tt.want {
				t.Errorf("GroupedDataFrame.Len() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_interfaceReduceFunc(t *testing.T) {
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
		fn   func(interface{}) interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"pass", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{values: []*valueContainer{
				{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{
				"custom", nil, func(vals interface{}) interface{} {
					arr := vals.([]float64)
					return int(arr[0])
				}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{1, 3}, isNull: []bool{false, false}, name: "custom_foo"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				name:          "custom",
				colLevelNames: []string{"*0"}},
			false},
		{"pass - select second column", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{values: []*valueContainer{
				{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "foo"},
				{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}, name: "bar"},
			},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{
				"custom", []string{"bar"}, func(vals interface{}) interface{} {
					arr := vals.([]float64)
					return int(arr[0])
				}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{1, 3}, isNull: []bool{false, false}, name: "custom_bar"}},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				name:          "custom",
				colLevelNames: []string{"*0"}},
			false},
		{"fail", fields{
			orderedKeys: []string{"foo", "bar"},
			rowIndices:  [][]int{{0, 1}, {2, 3}},
			labels:      []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			df: &DataFrame{values: []*valueContainer{{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{
				"foo", nil, func(vals interface{}) interface{} {
					return complex64(1)
				}},
			nil,
			true},
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
			got, err := g.interfaceReduceFunc(tt.args.name, tt.args.cols, tt.args.fn)
			if (err != nil) != tt.wantErr {
				t.Errorf("GroupedDataFrame.interfaceReduceFunc() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.interfaceReduceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_NUnique(t *testing.T) {
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
						{slice: []float64{1, 1, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
						{slice: []float64{5, 6, 7, 7}, isNull: []bool{false, false, false, false}, name: "waldo"},
					},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}},
					colLevelNames: []string{"*0"},
					name:          "qux"}},
			args: args{nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "nunique_corge"},
					{slice: []int{2, 1}, isNull: []bool{false, false}, name: "nunique_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "nunique_qux",
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
			if got := g.NUnique(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.NUnique() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedDataFrame_Nth(t *testing.T) {
	type fields struct {
		orderedKeys []string
		rowIndices  [][]int
		labels      []*valueContainer
		df          *DataFrame
		err         error
	}
	type args struct {
		index    int
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
			args: args{0, nil},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "nth_corge"},
					{slice: []string{"e", "g"}, isNull: []bool{false, false}, name: "nth_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "nth_qux",
			}},
		{
			name: "select column (not first)",
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
			args: args{0, []string{"waldo"}},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []string{"e", "g"}, isNull: []bool{false, false}, name: "nth_waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "nth_qux",
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
			if got := g.Nth(tt.args.index, tt.args.colNames...); !EqualDataFrames(got, tt.want) {
				t.Errorf("GroupedDataFrame.Nth() = %v, want %v", got, tt.want)
			}
		})
	}
}
