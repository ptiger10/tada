package tada

import (
	"reflect"
	"testing"
)

func Test_convertSimplifiedgenericTypeReduceFunc(t *testing.T) {
	type args struct {
		simplifiedFn func([]genericType) genericType
	}
	type callArgs struct {
		a []genericType
		b []bool
		c []int
	}
	tests := []struct {
		name     string
		args     args
		callArgs callArgs
	}{
		{"pass", args{func([]genericType) genericType {
			return genericTypeContainer{}
		}}, callArgs{
			[]genericType{genericTypeContainer{}}, []bool{false}, []int{0},
		}},
		{"pass", args{func([]genericType) genericType {
			return genericTypeContainer{}
		}}, callArgs{
			[]genericType{genericTypeContainer{}}, []bool{true}, []int{0},
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			fn := convertSimplifiedgenericTypeReduceFunc(tt.args.simplifiedFn)
			fn(tt.callArgs.a, tt.callArgs.b, tt.callArgs.c)
		})
	}
}

func Test_groupedgenericTypeReduceFunc(t *testing.T) {
	type args struct {
		slice      []genericType
		nulls      []bool
		name       string
		aligned    bool
		rowIndices [][]int
		fn         func([]genericType, []bool, []int) (genericType, bool)
	}
	tests := []struct {
		name string
		args args
		want *valueContainer
	}{
		{"pass",
			args{[]genericType{nil}, []bool{false}, "foo", true, [][]int{{0}}, func([]genericType, []bool, []int) (genericType, bool) { return nil, false }},
			&valueContainer{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"},
		},
		{"pass",
			args{[]genericType{nil}, []bool{false}, "foo", false, [][]int{{0}}, func([]genericType, []bool, []int) (genericType, bool) { return nil, false }},
			&valueContainer{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupedgenericTypeReduceFunc(tt.args.slice, tt.args.nulls, tt.args.name, tt.args.aligned, tt.args.rowIndices, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupedgenericTypeReduceFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_genericTypeReduceFunc(t *testing.T) {
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
		fn   func(slice []genericType, isNull []bool, index []int) (genericType, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				orderedKeys: []string{},
				rowIndices:  [][]int{{}},
				labels: []*valueContainer{
					{slice: []string{}, isNull: []bool{}, name: "*0"}},
				series: &Series{
					values: &valueContainer{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"},
					labels: []*valueContainer{}},
			},
			args{"foo", func([]genericType, []bool, []int) (genericType, bool) { return nil, false }},
			&Series{
				values: &valueContainer{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{}},
		},
		{"pass",
			fields{
				orderedKeys: []string{},
				rowIndices:  [][]int{{}},
				aligned:     true,
				labels: []*valueContainer{
					{slice: []string{}, isNull: []bool{}, name: "*0"}},
				series: &Series{
					values: &valueContainer{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"},
					labels: []*valueContainer{}},
			},
			args{"foo", func([]genericType, []bool, []int) (genericType, bool) { return nil, false }},
			&Series{
				values: &valueContainer{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{}},
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
			g.genericTypeReduceFunc(tt.args.name, tt.args.fn)
		})
	}
}

func TestGroupedDataFrame_genericTypeReduceFunc(t *testing.T) {
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
		fn   func(slice []genericType, isNull []bool, index []int) (genericType, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{
				orderedKeys: []string{},
				rowIndices:  [][]int{{}},
				labels: []*valueContainer{
					{slice: []string{}, isNull: []bool{}, name: "*0"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"}},
					labels: []*valueContainer{}},
			},
			args{"foo", []string{}, func([]genericType, []bool, []int) (genericType, bool) { return nil, false }},
			&DataFrame{
				values: []*valueContainer{{slice: []genericType{nil}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{}},
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
			g.genericTypeReduceFunc(tt.args.name, tt.args.cols, tt.args.fn)
		})
	}
}
