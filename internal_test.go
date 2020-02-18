package tada

import (
	"errors"
	"math"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/d4l3k/messagediff"
)

func TestDataFrame_resetWithError(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		err error
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{errors.New("foo")},
			&DataFrame{
				err: errors.New("foo"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				name:          tt.fields.name,
				err:           tt.fields.err,
				colLevelNames: tt.fields.colLevelNames,
			}
			if df.resetWithError(tt.args.err); !reflect.DeepEqual(df, tt.want) {
				t.Errorf("df.resetWithError() = %v, want %v", df.err, tt.want.err)
				t.Errorf(messagediff.PrettyDiff(df, tt.want))
			}
		})
	}
}

func Test_dataFrameWithError(t *testing.T) {
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"pass", args{errors.New("foo")}, &DataFrame{err: errors.New("foo")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dataFrameWithError(tt.args.err); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dataFrameWithError() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeValueContainerFromInterface(t *testing.T) {
	type args struct {
		slice interface{}
		name  string
	}
	tests := []struct {
		name    string
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"pass", args{[]float64{1}, "0"},
			&valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "0"}, false},
		{"fail - empty slice", args{[]float64{}, "0"},
			nil, true},
		{"fail - unsupported slice", args{[]complex64{1}, "0"},
			nil, true},
		{"fail - not slice", args{"foo", "0"},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeValueContainerFromInterface(tt.args.slice, tt.args.name)
			if (err != nil) != tt.wantErr {
				t.Errorf("makeValueContainerFromInterface() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeValueContainerFromInterface() = %#v, want %#v", got.slice, tt.want.slice)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func Test_makeValueContainersFromInterfaces(t *testing.T) {
	type args struct {
		slices         []interface{}
		prefixAsterisk bool
	}
	tests := []struct {
		name    string
		args    args
		want    []*valueContainer
		wantErr bool
	}{
		{"pass, no prefix", args{[]interface{}{[]float64{1}, []string{"foo"}}, false},
			[]*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			false,
		},
		{"pass, prefix", args{[]interface{}{[]float64{1}, []string{"foo"}}, true},
			[]*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "*0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "*1"}},
			false,
		},
		{"fail, unsupported", args{[]interface{}{"foo"}, false},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := makeValueContainersFromInterfaces(tt.args.slices, tt.args.prefixAsterisk)
			if (err != nil) != tt.wantErr {
				t.Errorf("makeValueContainersFromInterfaces() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeValueContainersFromInterfaces() = %v, want %v", got[0], tt.want[0])
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func Test_findMatchingKeysBetweenTwoLabelContainers(t *testing.T) {
	type args struct {
		labels1 []*valueContainer
		labels2 []*valueContainer
	}
	tests := []struct {
		name  string
		args  args
		want  []int
		want1 []int
	}{
		{"1 match", args{
			labels1: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*1"},
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
			},
			labels2: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
		}, []int{1}, []int{0}},
		{"duplicates", args{
			labels1: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{0}, isNull: []bool{false}, name: "*1"},
			},
			labels2: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
		}, []int{0}, []int{0}},
		{"no matches", args{
			labels1: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*1"},
			},
			labels2: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
		}, nil, nil},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := findMatchingKeysBetweenTwoLabelContainers(tt.args.labels1, tt.args.labels2)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findMatchingKeysBetweenTwoLabelContainers() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("findMatchingKeysBetweenTwoLabelContainers() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_setNullsFromInterface(t *testing.T) {
	type args struct {
		input interface{}
	}
	tests := []struct {
		name string
		args args
		want []bool
	}{
		{"float", args{[]float64{1, math.NaN()}}, []bool{false, true}},
		{"int", args{[]int{0}}, []bool{false}},
		{"string", args{[]string{"foo", ""}}, []bool{false, true}},
		{"dateTime", args{[]time.Time{time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), time.Time{}}}, []bool{false, true, true}},
		{"element", args{[]Element{{0, true}, {1, false}}}, []bool{true, false}},
		{"interface", args{[]interface{}{
			int(1), uint(1), float32(1), float64(1), time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC), "foo",
			math.NaN(), "", time.Time{}}},
			[]bool{false, false, false, false, false, false,
				true, true, true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := setNullsFromInterface(tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setNullsFromInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isSlice(t *testing.T) {
	type args struct {
		input interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"pass", args{[]string{"foo"}}, true},
		{"fail", args{"foo"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isSlice(tt.args.input); got != tt.want {
				t.Errorf("isSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_copy(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	tests := []struct {
		name   string
		fields fields
		want   *valueContainer
	}{
		{"pass", fields{slice: []float64{1}, name: "foo", isNull: []bool{false}},
			&valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			got := vc.copy()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.copy() = %v, want %v", got, tt.want)
			}
			got.slice.([]float64)[0] = 2
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original values")
			}
			got.name = "qux"
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original values name")
			}
			got.isNull[0] = true
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original isNull")
			}
		})
	}
}

func Test_makeDefaultLabels(t *testing.T) {
	type args struct {
		min int
		max int
	}
	tests := []struct {
		name       string
		args       args
		wantLabels *valueContainer
	}{
		{"normal", args{0, 2}, &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLabels := makeDefaultLabels(tt.args.min, tt.args.max)
			if !reflect.DeepEqual(gotLabels, tt.wantLabels) {
				t.Errorf("makeDefaultLabels() gotLabels = %v, want %v", gotLabels, tt.wantLabels)
			}
		})
	}
}

func Test_intersection(t *testing.T) {
	type args struct {
		slices [][]int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{"1 match", args{[][]int{{0, 1}, {1, 2}}}, []int{1}},
		{"all matches", args{[][]int{{2, 1}, {1, 2}}}, []int{1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intersection(tt.args.slices); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("intersection() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_union(t *testing.T) {
	type args struct {
		slices [][]int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{"normal", args{[][]int{{0, 1}, {1, 5}}}, []int{0, 1, 5}},
		{"sorted", args{[][]int{{1, 5}, {0, 1}}}, []int{0, 1, 5}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := union(tt.args.slices); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("union() = %v, want %v", got, tt.want)
			}
		})
	}
}
func Test_lookup(t *testing.T) {
	type args struct {
		how     string
		values1 *valueContainer
		labels1 []*valueContainer
		leftOn  []int
		values2 *valueContainer
		labels2 []*valueContainer
		rightOn []int
	}
	tests := []struct {
		name    string
		args    args
		want    *Series
		wantErr bool
	}{
		{name: "left", args: args{
			how: "left", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []int{10, 0}, isNull: []bool{false, true}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}, wantErr: false,
		},
		{name: "right", args: args{
			how: "right", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}},
				labels: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}}, wantErr: false,
		},
		{name: "inner", args: args{
			how: "inner", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []int{10}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}, wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lookup(tt.args.how, tt.args.values1, tt.args.labels1, tt.args.leftOn, tt.args.values2, tt.args.labels2, tt.args.rightOn)
			if (err != nil) != tt.wantErr {
				t.Errorf("lookup() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("lookup() = %v, want %v", got.labels[0], tt.want.labels[0])
			}
		})
	}
}

func Test_difference(t *testing.T) {
	type args struct {
		slice1 []int
		slice2 []int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{"pass", args{[]int{0, 1, 2}, []int{1}}, []int{0, 2}},
		{"reverse", args{[]int{2, 1, 0}, []int{1}}, []int{0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := difference(tt.args.slice1, tt.args.slice2); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("difference() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_labelsToMap(t *testing.T) {
	type args struct {
		labels []*valueContainer
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  map[string][]int
		want1 map[string]int
		want2 []string
		want3 map[int]int
	}{
		{"normal", args{[]*valueContainer{{slice: []float64{1}}, {slice: []string{"foo"}}}, []int{0, 1}},
			map[string][]int{"1|foo": []int{0}}, map[string]int{"1|foo": 0},
			[]string{"1|foo"}, map[int]int{0: 0}},
		{"reversed", args{[]*valueContainer{{slice: []float64{1}}, {slice: []string{"foo"}}}, []int{1, 0}},
			map[string][]int{"foo|1": []int{0}}, map[string]int{"foo|1": 0},
			[]string{"foo|1"}, map[int]int{0: 0}},
		{"skip", args{[]*valueContainer{{slice: []float64{1}}, {slice: []string{"foo"}}, {slice: []bool{true}}}, []int{2, 0}},
			map[string][]int{"true|1": []int{0}}, map[string]int{"true|1": 0},
			[]string{"true|1"}, map[int]int{0: 0}},
		{"multiple same", args{[]*valueContainer{{slice: []float64{1, 1}}, {slice: []string{"foo", "foo"}}}, []int{0, 1}},
			map[string][]int{"1|foo": []int{0, 1}}, map[string]int{"1|foo": 0},
			[]string{"1|foo"}, map[int]int{0: 0, 1: 0}},
		{"multiple different", args{[]*valueContainer{{slice: []float64{2, 1}}, {slice: []string{"foo", "bar"}}}, []int{0, 1}},
			map[string][]int{"2|foo": []int{0}, "1|bar": []int{1}}, map[string]int{"1|bar": 1, "2|foo": 0},
			[]string{"2|foo", "1|bar"}, map[int]int{0: 0, 1: 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, got2, got3 := labelsToMap(tt.args.labels, tt.args.index)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("labelsToMap() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("labelsToMap() got1 = %v, want %v", got1, tt.want1)
			}
			if !reflect.DeepEqual(got2, tt.want2) {
				t.Errorf("labelsToMap() got2 = %v, want %v", got2, tt.want2)
			}
			if !reflect.DeepEqual(got3, tt.want3) {
				t.Errorf("labelsToMap() got3 = %v, want %v", got3, tt.want3)
			}
		})
	}
}

func Test_copyInterfaceIntoValueContainers(t *testing.T) {
	type args struct {
		slices []interface{}
		isNull [][]bool
		names  []string
	}
	tests := []struct {
		name string
		args args
		want []*valueContainer
	}{
		{"pass", args{
			slices: []interface{}{[]string{"foo"}, []float64{1}},
			isNull: [][]bool{{false}, {false}},
			names:  []string{"corge", "waldo"},
		},
			[]*valueContainer{
				{slice: []string{"foo"}, isNull: []bool{false}, name: "corge"},
				{slice: []float64{1}, isNull: []bool{false}, name: "waldo"},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyInterfaceIntoValueContainers(tt.args.slices, tt.args.isNull, tt.args.names); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("copyInterfaceIntoValueContainers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_copyGroupedLabels(t *testing.T) {
	type args struct {
		labels     []string
		levelNames []string
	}
	tests := []struct {
		name string
		args args
		want []*valueContainer
	}{
		{"pass", args{labels: []string{"foo|0", "bar|1"}, levelNames: []string{"corge", "waldo"}},
			[]*valueContainer{
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "corge"},
				{slice: []string{"0", "1"}, isNull: []bool{false, false}, name: "waldo"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyGroupedLabels(tt.args.labels, tt.args.levelNames); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("copyGroupedLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeBoolMatrix(t *testing.T) {
	type args struct {
		numCols int
		numRows int
	}
	tests := []struct {
		name string
		args args
		want [][]bool
	}{
		{"2 col, 1 row", args{2, 1}, [][]bool{{false}, {false}}},
		{"1 col, 2 row", args{1, 2}, [][]bool{{false, false}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeBoolMatrix(tt.args.numCols, tt.args.numRows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeBoolMatrix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeStringMatrix(t *testing.T) {
	type args struct {
		numCols int
		numRows int
	}
	tests := []struct {
		name string
		args args
		want [][]string
	}{
		{"2 col, 1 row", args{2, 1}, [][]string{{""}, {""}}},
		{"1 col, 2 row", args{1, 2}, [][]string{{"", ""}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeStringMatrix(tt.args.numCols, tt.args.numRows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeStringMatrix() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_gt(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			args{2}, []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.gt(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.gt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_gte(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			args{2}, []int{1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.gte(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.gte() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_lt(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			args{2}, []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.lt(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.lt() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_lte(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			args{2}, []int{0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.lte(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.lte() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_floateq(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			args{2}, []int{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.floateq(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.floateq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_floatneq(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			args{2}, []int{0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.floatneq(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.floatneq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_eq(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
			args{"foo"}, []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.eq(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.eq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_neq(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
			args{"foo"}, []int{1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.neq(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.neq() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_contains(t *testing.T) {
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		substr string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
			args{"ba"}, []int{1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.contains(tt.args.substr); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_before(t *testing.T) {
	date := time.Date(2019, 1, 1, 1, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []time.Time{date, date.AddDate(0, 0, 1), date.AddDate(0, 0, 2)}, isNull: []bool{false, false, false}},
			args{date.AddDate(0, 0, 1)}, []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.before(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.before() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_after(t *testing.T) {
	date := time.Date(2019, 1, 1, 1, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		name   string
		isNull []bool
	}
	type args struct {
		comparison time.Time
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{slice: []time.Time{date, date.AddDate(0, 0, 1), date.AddDate(0, 0, 2)}, isNull: []bool{false, false, false}},
			args{date.AddDate(0, 0, 1)}, []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
			}
			if got := vc.after(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.after() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_concatenateLabelsToStrings(t *testing.T) {
	type args struct {
		labels []*valueContainer
		index  []int
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"one level", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}}},
			index: []int{0}},
			[]string{"foo", "bar"}},
		{"two levels, one index", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}},
			{slice: []int{0, 1}}},
			index: []int{0}},
			[]string{"foo", "bar"}},
		{"two levels, two index", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}},
			{slice: []int{0, 1}}},
			index: []int{0, 1}},
			[]string{"foo|0", "bar|1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := concatenateLabelsToStrings(tt.args.labels, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("concatenateLabelsToStrings() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_shift(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		n int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *valueContainer
	}{
		{"positive", fields{slice: []string{"1", "2", "3"}, isNull: []bool{false, false, false}, name: "foo"},
			args{1},
			&valueContainer{
				slice: []string{"", "1", "2"}, isNull: []bool{true, false, false}, name: "foo"}},
		{"negative", fields{slice: []string{"1", "2", "3"}, isNull: []bool{false, false, false}, name: "foo"},
			args{-1},
			&valueContainer{
				slice: []string{"2", "3", ""}, isNull: []bool{false, false, true}, name: "foo"}},
		{"too many positions", fields{slice: []string{"1", "2", "3"}, isNull: []bool{false, false, false}, name: "foo"},
			args{5},
			&valueContainer{
				slice: []string{"", "", ""}, isNull: []bool{true, true, true}, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.shift(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("vc.shift() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_applyFormat(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		apply ApplyFormatFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
		{"float",
			fields{slice: []float64{.75}, isNull: []bool{false}},
			args{ApplyFormatFn{F64: func(v float64) string {
				return strconv.FormatFloat(v, 'f', 1, 64)
			}}},
			[]string{"0.8"}},
		{"datetime",
			fields{slice: []time.Time{time.Date(2019, 1, 1, 0, 0, 0, 0, time.UTC)}, isNull: []bool{false}},
			args{ApplyFormatFn{DateTime: func(v time.Time) string {
				return v.Format("2006-01-02")
			}}},
			[]string{"2019-01-01"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.applyFormat(tt.args.apply); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.applyFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_append(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		other *valueContainer
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *valueContainer
	}{
		{"floats", fields{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			args{&valueContainer{slice: []float64{2}, isNull: []bool{false}, name: "bar"}},
			&valueContainer{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.append(tt.args.other); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.append() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_cumsum(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name string
		args args
		want []float64
	}{
		{"pass", args{vals: []float64{1, 0, 2}, isNull: []bool{false, true, false}, index: []int{0, 1, 2}},
			[]float64{1, 1, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cumsum(tt.args.vals, tt.args.isNull, tt.args.index)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cumsum() got = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_first(t *testing.T) {
	type args struct {
		vals   []string
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		{"valid", args{vals: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, index: []int{0, 1, 2}},
			"foo", false},
		{"null", args{vals: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, index: []int{1}},
			"", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := first(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("first() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("first() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_last(t *testing.T) {
	type args struct {
		vals   []string
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  string
		want1 bool
	}{
		{"valid", args{vals: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, index: []int{0, 1, 2}},
			"baz", false},
		{"skip", args{vals: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, index: []int{0, 1}},
			"foo", false},
		{"null", args{vals: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, index: []int{1}},
			"", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := last(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("last() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("last() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_valueContainer_cut(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		bins    []float64
		andLess bool
		andMore bool
		labels  []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{"supplied labels, no less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{0, 2, 4}, andLess: false, andMore: false, labels: []string{"low", "high"}},
			[]string{"low", "low", "high", "high"}, false},
		{"supplied labels, no less, no more, with null",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, true}, name: "foo"},
			args{bins: []float64{0, 2, 4}, andLess: false, andMore: false, labels: []string{"low", "high"}},
			[]string{"low", "low", "high", ""}, false},
		{"supplied labels, less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: false, labels: []string{"low", "medium", "high"}},
			[]string{"low", "medium", "high", ""}, false},
		{"supplied labels, no less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: true, labels: []string{"low", "medium", "high"}},
			[]string{"", "low", "medium", "high"}, false},
		{"supplied labels, less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: true, labels: []string{"low", "medium", "high", "higher"}},
			[]string{"low", "medium", "high", "higher"}, false},
		{"default labels, no less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{0, 2, 4}, andLess: false, andMore: false, labels: nil},
			[]string{"0-2", "0-2", "2-4", "2-4"}, false},
		{"default labels, less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: false, labels: nil},
			[]string{"<=1", "1-2", "2-3", ""}, false},
		{"default labels, no less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: true, labels: nil},
			[]string{"", "1-2", "2-3", ">3"}, false},
		{"default labels, less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: true, labels: nil},
			[]string{"<=1", "1-2", "2-3", ">3"}, false},
		{"fail: zero bins",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{}, andLess: false, andMore: false, labels: []string{}},
			nil, true},
		{"fail: bin - label mismatch",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: false, labels: []string{"foo"}},
			nil, true},
		{"fail: bin - label mismatch, less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: false, labels: []string{"foo", "bar"}},
			nil, true},
		{"fail: bin - label mismatch, no less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: true, labels: []string{"foo", "bar"}},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			got, err := vc.cut(tt.args.bins, tt.args.andLess, tt.args.andMore, tt.args.labels)
			if (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.cut() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.cut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_rank(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name string
		args args
		want []float64
	}{
		{"no repeats", args{vals: []float64{4, 5, 3}, isNull: []bool{false, false, false}, index: []int{0, 1, 2}},
			[]float64{2, 3, 1}},
		{"no repeats, null", args{vals: []float64{4, 0, 5, 3}, isNull: []bool{false, true, false, false},
			index: []int{0, 1, 2, 3}}, []float64{2, -999, 3, 1}},
		{"repeats", args{vals: []float64{4, 5, 4, 3}, isNull: []bool{false, false, false, false}, index: []int{0, 1, 2, 3}},
			[]float64{2, 3, 2, 1}},
		{"more repeats", args{vals: []float64{3, 2, 0, 4, 1, 3}, isNull: []bool{false, false, true, false, false, false},
			index: []int{0, 1, 2, 3, 4, 5}},
			[]float64{3, 2, -999, 4, 1, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rank(tt.args.vals, tt.args.isNull, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("rank() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_findColWithName(t *testing.T) {
	type args struct {
		name string
		cols []*valueContainer
	}
	tests := []struct {
		name    string
		args    args
		want    int
		wantErr bool
	}{
		{"pass", args{"foo", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, name: "foo"}}},
			1, false},
		{"pass - uppercase search", args{"FOO", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, name: "foo"}}},
			1, false},
		{"pass - title case name", args{"foo", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, name: "Foo"}}},
			1, false},
		{"fail - not found", args{"foo", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := findColWithName(tt.args.name, tt.args.cols)
			if (err != nil) != tt.wantErr {
				t.Errorf("findColWithName() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("findColWithName() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_percentile(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name string
		args args
		want []float64
	}{
		{"no null", args{vals: []float64{1, 2, 3, 6}, isNull: []bool{false, false, false, false}, index: []int{0, 1, 2, 3}},
			[]float64{0, .25, .5, .75}},
		{"repeats", args{vals: []float64{1, 2, 2, 4}, isNull: []bool{false, false, false, false}, index: []int{0, 1, 2, 3}},
			[]float64{0, .25, .25, .75}},
		{"null", args{vals: []float64{0, 1, 2, 3, 4}, isNull: []bool{true, false, false, false, false},
			index: []int{0, 1, 2, 3, 4}}, []float64{-999, 0, .25, .5, .75}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := percentile(tt.args.vals, tt.args.isNull, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("percentile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_pcut(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		bins   []float64
		labels []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{"default labels",
			fields{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{0, .5, 1}, labels: nil},
			[]string{"0-0.5", "0-0.5", "0.5-1", "0.5-1"}, false},
		{"supplied labels",
			fields{slice: []float64{-1, 2, 6, 10, 12}, isNull: []bool{false, false, false, false, false}},
			args{bins: []float64{0, .5, 1}, labels: []string{"Bottom 50%", "Top 50%"}},
			[]string{"Bottom 50%", "Bottom 50%", "Bottom 50%", "Top 50%", "Top 50%"}, false},
		{"default labels, nulls, repeats",
			fields{slice: []float64{5, 0, 6, 7, 7, 7, 8},
				isNull: []bool{false, true, false, false, false, false, false}},
			args{bins: []float64{0, .2, .4, .6, .8, 1}, labels: nil},
			[]string{"0-0.2", "", "0-0.2", "0.2-0.4", "0.2-0.4", "0.2-0.4", "0.8-1"}, false},
		{"fail: above 1",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{0, .5, 1.5}, labels: []string{"Bottom 50%", "Top 50%"}},
			nil, true},
		{"fail: below 0",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{bins: []float64{-0.1, .5, 1}, labels: []string{"Bottom 50%", "Top 50%"}},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			got, err := vc.pcut(tt.args.bins, tt.args.labels)
			if (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.pcut() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.pcut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_sort(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		dtype      DType
		descending bool
		index      []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"no nulls",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: Float, descending: false, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"nulls",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, true, false}, name: "foo"},
			args{dtype: Float, descending: false, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.sort(tt.args.dtype, tt.args.descending, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.sort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mockCSVFromDTypes(t *testing.T) {
	randSeed = 3
	type args struct {
		dtypes      []map[DType]int
		numMockRows int
	}
	tests := []struct {
		name string
		args args
		want [][]string
	}{
		{"2x rows",
			args{
				[]map[DType]int{
					{Float: 3, String: 0, DateTime: 0},
					{Float: 0, String: 3, DateTime: 0},
					{Float: 0, String: 1, DateTime: 2}},
				2},
			[][]string{{"3", "foo", "12/1/2019"}, {"1", "foo", "12/1/2019"}},
		},
		{"3x rows",
			args{
				[]map[DType]int{
					{Float: 1, String: 0, DateTime: 0},
					{Float: 0, String: 1, DateTime: 0}},
				3},
			[][]string{{"3", "foo"}, {"1", "foo"}, {"1", "foo"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mockCSVFromDTypes(tt.args.dtypes, tt.args.numMockRows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("mockCSVFromDTypes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_inferType(t *testing.T) {
	type args struct {
		input string
	}
	tests := []struct {
		name string
		args args
		want DType
	}{
		{"date", args{"1/1/20"}, DateTime},
		{"float", args{"1"}, Float},
		{"string", args{"foo"}, String},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := inferType(tt.args.input); got != tt.want {
				t.Errorf("inferType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_apply(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		apply ApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   interface{}
	}{
		{"float", fields{
			slice:  []float64{1, 2},
			isNull: []bool{false, false},
			name:   "foo"},
			args{ApplyFn{F64: func(v float64) float64 { return v * 2 }}},
			[]float64{2, 4}},
		{"string", fields{
			slice:  []string{"foo", "bar"},
			isNull: []bool{false, false},
			name:   "foo"},
			args{ApplyFn{String: func(s string) string { return strings.Replace(s, "o", "a", -1) }}},
			[]string{"faa", "bar"}},
		{"date", fields{
			slice:  []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
			isNull: []bool{false},
			name:   "foo"},
			args{ApplyFn{DateTime: func(v time.Time) time.Time { return v.AddDate(0, 0, 1) }}},
			[]time.Time{time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.apply(tt.args.apply); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_withColumn(t *testing.T) {
	type args struct {
		cols        []*valueContainer
		name        string
		input       interface{}
		requiredLen int
	}
	tests := []struct {
		name    string
		args    args
		want    []*valueContainer
		wantErr bool
	}{
		{"rename", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: "corge", requiredLen: 2},
			[]*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "corge"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, false,
		},
		{"overwrite", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: []int{3, 4}, requiredLen: 2},
			[]*valueContainer{
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, false,
		},
		{"append", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "corge", input: []int{3, 4}, requiredLen: 2},
			[]*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "corge"},
			}, false,
		},
		{"overwrite Series", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: &Series{values: &valueContainer{
				slice: []float64{3, 4}, isNull: []bool{false, false},
			}}, requiredLen: 2},
			[]*valueContainer{
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, false,
		},
		{"append Series", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "corge", input: &Series{values: &valueContainer{
				slice: []float64{3, 4}, isNull: []bool{false, false},
			}}, requiredLen: 2},
			[]*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "corge"},
			}, false,
		},
		{"fail - unsupported type", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "corge", input: []complex64{3, 4}, requiredLen: 2},
			nil, true,
		},
		{"fail - wrong length", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: []float64{0, 1, 2, 3, 4}, requiredLen: 2},
			nil, true,
		},
		{"fail - wrong length", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: []float64{0, 1, 2, 3, 4}, requiredLen: 2},
			nil, true,
		},
		{"fail - not Series pointer", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: &time.Time{}, requiredLen: 2},
			nil, true,
		},
		{"fail - Series of wrong length", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: &Series{values: &valueContainer{
				slice: []float64{1, 2, 3}, isNull: []bool{false, false, false},
			}}, requiredLen: 2},
			nil, true,
		},
		{"fail - unsupported input", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "qux"},
			}, name: "foo", input: map[string]int{}, requiredLen: 2},
			nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := withColumn(tt.args.cols, tt.args.name, tt.args.input, tt.args.requiredLen)
			if (err != nil) != tt.wantErr {
				t.Errorf("withColumn() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("withColumn() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_toCSVByRows(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	type args struct {
		ignoreLabels bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    [][]string
		wantErr bool
	}{
		{name: "one col level",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args: args{ignoreLabels: false},
			want: [][]string{
				{"*0", "foo", "bar"},
				{"0", "1", "a"},
				{"1", "2", "b"},
			},
			wantErr: false},
		{name: "one col level - ignore labels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args: args{ignoreLabels: true},
			want: [][]string{
				{"foo", "bar"},
				{"1", "a"},
				{"2", "b"},
			},
			wantErr: false},
		{name: "two col levels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo|baz"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar|qux"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0", "*1"},
			},
			args: args{ignoreLabels: false},
			want: [][]string{
				{"", "foo", "bar"},
				{"*0", "baz", "qux"},
				{"0", "1", "a"},
				{"1", "2", "b"},
			}},
		{name: "two label levels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{10, 11}, isNull: []bool{false, false}, name: "*1"},
				},
				colLevelNames: []string{"*0"},
			},
			want: [][]string{
				{"*0", "*1", "foo", "bar"},
				{"0", "10", "1", "a"},
				{"1", "11", "2", "b"},
			},
			wantErr: false},
		{name: "fail - no values",
			fields: fields{
				values: nil,
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{10, 11}, isNull: []bool{false, false}, name: "*1"},
				},
				colLevelNames: []string{"*0"},
			},
			want: nil, wantErr: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				name:          tt.fields.name,
				colLevelNames: tt.fields.colLevelNames,
				err:           tt.fields.err,
			}
			got, err := df.toCSVByRows(tt.args.ignoreLabels)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.toCSVByRows() = %v, want %v", got, tt.want)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.toCSVByRows() err = %v, want %v", err, tt.wantErr)
			}
		})
	}
}

func Test_readCSVByRows(t *testing.T) {
	type args struct {
		csv    [][]string
		config *ReadConfig
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"1 header row, 2 columns, no label levels",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: &ReadConfig{NumHeaderRows: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}}},
		{"1 header row, 1 column, 1 label level",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: &ReadConfig{NumHeaderRows: 1, NumLabelCols: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels: []*valueContainer{
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"}},
				colLevelNames: []string{"*0"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readCSVByRows(tt.args.csv, tt.args.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readCSVByRows() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func Test_readCSVByCols(t *testing.T) {
	type args struct {
		csv    [][]string
		config *ReadConfig
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"1 header row, 2 columns, no label levels",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6"}},
				config: &ReadConfig{NumHeaderRows: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}}},
		{"1 header row, 1 column, 1 label levels",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6"}},
				config: &ReadConfig{NumHeaderRows: 1, NumLabelCols: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels: []*valueContainer{
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				},
				colLevelNames: []string{"*0"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readCSVByCols(tt.args.csv, tt.args.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readCSVByCols() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readStruct(t *testing.T) {
	type args struct {
		slice interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []*valueContainer
		wantErr bool
	}{
		{"pass", args{[]testStruct{{"foo", 1}, {"bar", 2}}},
			[]*valueContainer{
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "Name"},
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "Age"}},
			false},
		{"fail - not slice", args{testStruct{"foo", 1}},
			nil, true},
		{"fail - not struct", args{[]string{"foo"}},
			nil, true},
		{"fail - empty", args{[]testStruct{}},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readStruct(tt.args.slice)
			if (err != nil) != tt.wantErr {
				t.Errorf("readStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}
