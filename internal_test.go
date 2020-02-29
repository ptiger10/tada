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
			if df.resetWithError(tt.args.err); !EqualDataFrames(df, tt.want) {
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
			if got := dataFrameWithError(tt.args.err); !EqualDataFrames(got, tt.want) {
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
		{"dateTime", args{[]time.Time{time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), {}}}, []bool{false, true, true}},
		{"element", args{[]Element{{0, true}, {1, false}}}, []bool{true, false}},
		{"interface", args{[]interface{}{
			int(1), uint(1), float32(1), float64(1), time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC), "foo",
			math.NaN(), "", time.Time{}}},
			[]bool{false, false, false, false, false, false,
				true, true, true}},
		{"nested string", args{[][]string{{"foo"}, {}}}, []bool{false, true}},
		{"nil - not slice", args{"foo"}, nil},
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
		min            int
		max            int
		prefixAsterisk bool
	}
	tests := []struct {
		name       string
		args       args
		wantLabels *valueContainer
	}{
		{"normal", args{0, 2, true}, &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
		{"normal", args{0, 2, false}, &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLabels := makeDefaultLabels(tt.args.min, tt.args.max, tt.args.prefixAsterisk)
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
			if !EqualSeries(got, tt.want) {
				t.Errorf("lookup() = %v, want %v", got.labels[0], tt.want.labels[0])
			}
		})
	}
}

func Test_lookupDataFrame(t *testing.T) {
	type args struct {
		how           string
		name          string
		colLevelNames []string
		values1       []*valueContainer
		labels1       []*valueContainer
		leftOn        []int
		values2       []*valueContainer
		labels2       []*valueContainer
		rightOn       []int
		excludeLeft   []string
		excludeRight  []string
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{name: "left", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []int{10, 20}, isNull: []bool{false, false}, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []int{10, 0}, isNull: []bool{false, true}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}},
				name:   "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "left - nulls", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}},
				name:   "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "left - repeated label appears only once", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []string{"c", "d"}, isNull: []bool{false, false}, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{1, 1}, isNull: []bool{false, false}, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}},
				name:   "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "left - exclude named column", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "baz"},
				{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "quux"}}, rightOn: []int{1},
			excludeRight: []string{"baz"}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}},
				name:   "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "right", args: args{
			how: "right", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []int{10, 20}, isNull: []bool{false, false}, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"}},
				labels: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, name: "quux"}},
				name:   "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "inner", args: args{
			how: "inner", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []int{10, 20}, isNull: []bool{false, false}, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []int{10}, isNull: []bool{false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
				name:   "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := lookupDataFrame(tt.args.how, tt.args.name, tt.args.colLevelNames, tt.args.values1, tt.args.labels1, tt.args.leftOn, tt.args.values2, tt.args.labels2, tt.args.rightOn, tt.args.excludeLeft, tt.args.excludeRight)
			if (err != nil) != tt.wantErr {
				t.Errorf("lookupDataFrame() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("lookupDataFrame() = %v, want %v", got, tt.want)
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

func Test_reduceContainers(t *testing.T) {
	type args struct {
		containers []*valueContainer
		index      []int
	}
	tests := []struct {
		name                   string
		args                   args
		wantNewContainers      []*valueContainer
		wantOriginalRowIndexes [][]int
		wantOrderedKeys        []string
		wantOldToNewRowMapping map[int]int
	}{
		{name: "single level",
			args: args{containers: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"bar", "qux", "bar"}, isNull: []bool{false, false, false}, name: "baz"},
			},
				index: []int{1}},
			wantNewContainers: []*valueContainer{
				{slice: []string{"bar", "qux"}, isNull: []bool{false, false}, name: "baz"},
			},
			wantOriginalRowIndexes: [][]int{{0, 2}, {1}},
			wantOrderedKeys:        []string{"bar", "qux"},
			wantOldToNewRowMapping: map[int]int{0: 0, 1: 1, 2: 0}},
		{name: "multi level",
			args: args{containers: []*valueContainer{
				{slice: []float64{1, 1, 1}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"bar", "qux", "bar"}, isNull: []bool{false, false, false}, name: "baz"},
			},
				index: []int{0, 1}},
			wantNewContainers: []*valueContainer{
				{slice: []float64{1, 1}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"bar", "qux"}, isNull: []bool{false, false}, name: "baz"},
			},
			wantOriginalRowIndexes: [][]int{{0, 2}, {1}},
			wantOrderedKeys:        []string{"1|bar", "1|qux"},
			wantOldToNewRowMapping: map[int]int{0: 0, 1: 1, 2: 0}},
		{name: "single level - null",
			args: args{containers: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"bar", "", "bar"}, isNull: []bool{false, true, false}, name: "baz"},
			},
				index: []int{1}},
			wantNewContainers: []*valueContainer{
				{slice: []string{"bar", ""}, isNull: []bool{false, true}, name: "baz"},
			},
			wantOriginalRowIndexes: [][]int{{0, 2}, {1}},
			wantOrderedKeys:        []string{"bar", ""},
			wantOldToNewRowMapping: map[int]int{0: 0, 1: 1, 2: 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNewContainers, gotOriginalRowIndexes, gotOrderedKeys, gotOldToNewRowMapping := reduceContainers(tt.args.containers, tt.args.index)
			if !reflect.DeepEqual(gotNewContainers, tt.wantNewContainers) {
				t.Errorf("reduceContainers() gotNewContainers = %v, want %v", gotNewContainers[0], tt.wantNewContainers[0])
			}
			if !reflect.DeepEqual(gotOriginalRowIndexes, tt.wantOriginalRowIndexes) {
				t.Errorf("reduceContainers() gotOriginalRowIndexes = %v, want %v", gotOriginalRowIndexes, tt.wantOriginalRowIndexes)
			}
			if !reflect.DeepEqual(gotOrderedKeys, tt.wantOrderedKeys) {
				t.Errorf("reduceContainers() gotOrderedKeys = %v, want %v", gotOrderedKeys, tt.wantOrderedKeys)
			}
			if !reflect.DeepEqual(gotOldToNewRowMapping, tt.wantOldToNewRowMapping) {
				t.Errorf("reduceContainers() gotOldToNewRowMapping = %v, want %v", gotOldToNewRowMapping, tt.wantOldToNewRowMapping)
			}
		})
	}
}

func Test_reduceContainersLimited(t *testing.T) {
	type args struct {
		containers []*valueContainer
		index      []int
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{name: "single level",
			args: args{containers: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"bar", "qux", "bar"}, isNull: []bool{false, false, false}, name: "baz"},
			},
				index: []int{1}},
			want: map[string]int{"bar": 0, "qux": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reduceContainersLimited(tt.args.containers, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("reduceContainersLimited() = %v, want %v", got, tt.want)
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
		{"create nulls from interface", args{
			slices: []interface{}{[]string{"foo", ""}, []float64{1, 2}},
			isNull: nil,
			names:  []string{"corge", "waldo"},
		},
			[]*valueContainer{
				{slice: []string{"foo", ""}, isNull: []bool{false, true}, name: "corge"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"},
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

func Test_copyFloatsIntoValueContainers(t *testing.T) {
	type args struct {
		slices [][]float64
		isNull [][]bool
		names  []string
	}
	tests := []struct {
		name string
		args args
		want []*valueContainer
	}{
		{"pass", args{
			slices: [][]float64{{0, 1}, {1, 2}},
			isNull: [][]bool{{true, false}, {false, false}},
			names:  []string{"corge", "waldo"},
		},
			[]*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "corge"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := copyFloatsIntoValueContainers(tt.args.slices, tt.args.isNull, tt.args.names); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("copyFloatsIntoValueContainers() = %v, want %v", got, tt.want)
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

func Test_makeFloatMatrix(t *testing.T) {
	type args struct {
		numCols int
		numRows int
	}
	tests := []struct {
		name string
		args args
		want [][]float64
	}{
		{"2 col, 1 row", args{2, 1}, [][]float64{{0}, {0}}},
		{"1 col, 2 row", args{1, 2}, [][]float64{{0, 0}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeFloatMatrix(tt.args.numCols, tt.args.numRows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeFloatMatrix() = %v, want %v", got, tt.want)
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
		{"two levels, one index", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}},
			{slice: []int{0, 1}}},
			index: []int{1}},
			[]string{"0", "1"}},
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
			args{ApplyFormatFn{Float: func(v float64) string {
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
			&valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"}},
		{"floats and ints", fields{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			args{&valueContainer{slice: []int{2}, isNull: []bool{false}, name: "bar"}},
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
			got, err := indexOfContainer(tt.args.name, tt.args.cols)
			if (err != nil) != tt.wantErr {
				t.Errorf("indexOfContainer() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("indexOfContainer() = %v, want %v", got, tt.want)
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
	d := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		dtype     DType
		ascending bool
		index     []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"float - no nulls",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: Float, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"float - convert from string",
			fields{slice: []string{"3", "1", "0", "2"}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: Float, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"float - no nulls - descending",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: Float, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"float - nulls",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, true, false}, name: "foo"},
			args{dtype: Float, ascending: true, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
		{"strings - no nulls",
			fields{slice: []string{"foo", "bar", "a", "baz"}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: String, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"strings - convert from float",
			fields{slice: []string{"3", "11", "0", "2"}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: String, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"strings - no nulls - descending",
			fields{slice: []string{"foo", "bar", "a", "baz"}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: String, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"strings - nulls",
			fields{slice: []string{"foo", "bar", "a", "baz"}, isNull: []bool{false, false, true, false}, name: "foo"},
			args{dtype: String, ascending: true, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
		{"datetime - no nulls",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: DateTime, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"datetime - convert from string",
			fields{slice: []string{"2020-01-04", "2020-01-02", "2020-01-01", "2020-01-03"}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: DateTime, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"datetime - no nulls - descending",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, false, false}, name: "foo"},
			args{dtype: DateTime, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"datetime - nulls",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, true, false}, name: "foo"},
			args{dtype: DateTime, ascending: true, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.sort(tt.args.dtype, tt.args.ascending, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.sort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sortContainers(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	d1 := d.AddDate(0, 0, 1)
	type args struct {
		containers []*valueContainer
		sorters    []Sorter
	}
	tests := []struct {
		name    string
		args    args
		want    []int
		wantErr bool
	}{
		{"multi sort - floats",
			args{[]*valueContainer{
				{slice: []float64{2, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{3, 2, 1}, isNull: []bool{false, false, false}, name: "bar"},
			}, []Sorter{{Name: "foo"}, {Name: "bar"}}},
			[]int{1, 2, 0}, false},
		{"multi sort - floats - ordered repeats",
			args{[]*valueContainer{
				{slice: []float64{2, 2, 1}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{1, 2, 1}, isNull: []bool{false, false, false}, name: "bar"},
			}, []Sorter{{Name: "foo"}, {Name: "bar"}}},
			[]int{2, 0, 1}, false},
		{"multi sort - converted string to date + string",
			args{[]*valueContainer{
				{slice: []string{"2020-01-02 00:00:00 +0000 UTC", "2020-01-02 00:00:00 +0000 UTC", "2020-01-01 00:00:00 +0000 UTC", "2020-01-01 00:00:00 +0000 UTC"}, isNull: []bool{false, false, false, false}, name: "foo"},
				{slice: []string{"foo", "qux", "qux", "foo"}, isNull: []bool{false, false, false, false}, name: "bar"},
			}, []Sorter{{Name: "foo", DType: DateTime}, {Name: "bar", DType: String}}},
			[]int{3, 2, 0, 1}, false},
		{"multi sort - converted string to date + string",
			args{[]*valueContainer{
				{slice: []time.Time{d, d1, d, d1}, isNull: []bool{false, false, false, false}, name: "foo"},
				{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "bar"},
			}, []Sorter{{Name: "foo", DType: DateTime}, {Name: "bar", DType: String}}},
			[]int{2, 0, 3, 1}, false},
		{"fail - bad container",
			args{[]*valueContainer{
				{slice: []float64{2, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{3, 2, 1}, isNull: []bool{false, false, false}, name: "bar"},
			}, []Sorter{{Name: "corge"}}},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := sortContainers(tt.args.containers, tt.args.sorters)
			if (err != nil) != tt.wantErr {
				t.Errorf("sortContainers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("sortContainers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_getDominantDType(t *testing.T) {
	type args struct {
		dtypes map[string]int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"pass", args{map[string]int{"int": 0, "float": 1, "string": 2, "datetime": 3}}, "datetime"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := getDominantDType(tt.args.dtypes); got != tt.want {
				t.Errorf("getDominantDType() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_mockCSVFromDTypes(t *testing.T) {
	randSeed = 3
	type args struct {
		dtypes      []map[string]int
		numMockRows int
	}
	tests := []struct {
		name string
		args args
		want [][]string
	}{
		{"2x rows",
			args{
				dtypes: []map[string]int{
					{"float": 3, "int": 1, "string": 1, "datetime": 1, "date": 1, "bool": 1},
					{"float": 1, "int": 3, "string": 1, "datetime": 1, "date": 1, "bool": 1},
					{"float": 1, "int": 1, "string": 3, "datetime": 1, "date": 1, "bool": 1},
					{"float": 1, "int": 1, "string": 1, "datetime": 3, "date": 1, "bool": 1},
					{"float": 1, "int": 1, "string": 1, "datetime": 1, "date": 3, "bool": 1},
					{"float": 1, "int": 1, "string": 1, "datetime": 1, "date": 1, "bool": 3}},
				numMockRows: 2},
			[][]string{
				{".5", "1", "quuz", "2020-01-01T12:30:00Z00:00", "2020-01-02", "true"},
				{".5", "4", "qux", "2020-01-01T12:30:00Z00:00", "2020-02-01", "true"}},
		},
		{"3x rows",
			args{
				[]map[string]int{
					{"float": 1, "string": 0},
					{"float": 0, "string": 1}},
				3},
			[][]string{{".5", "foo"}, {".9", "baz"}, {".5", "foo"}},
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
		want string
	}{
		{"float", args{"1.5"}, "float"},
		{"int", args{"1"}, "int"},
		{"string", args{"foo"}, "string"},
		{"datetime", args{"1/1/20 3pm"}, "datetime"},
		{"date", args{"1/1/20"}, "date"},
		{"bool", args{"true"}, "bool"},
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
			args{ApplyFn{Float: func(v float64) float64 { return v * 2 }}},
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
			if got := readCSVByRows(tt.args.csv, tt.args.config); !EqualDataFrames(got, tt.want) {
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
			if got := readCSVByCols(tt.args.csv, tt.args.config); !EqualDataFrames(got, tt.want) {
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

func Test_cut(t *testing.T) {
	type args struct {
		vals           []float64
		isNull         []bool
		bins           []float64
		leftInclusive  bool
		rightExclusive bool
		includeLess    bool
		includeMore    bool
		labels         []string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"pass - default labels", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2},
			leftInclusive: false, rightExclusive: false,
			includeLess: false, includeMore: false,
			labels: nil},
			[]string{"", "1-2", ""}, false},
		{"pass - supplied labels", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2},
			leftInclusive: false, rightExclusive: false,
			includeLess: false, includeMore: false,
			labels: []string{"qualifies"}},
			[]string{"", "qualifies", ""}, false},
		{"skip nulls", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, true, false}, bins: []float64{1, 2},
			leftInclusive: false, rightExclusive: false,
			includeLess: false, includeMore: false,
			labels: []string{"qualifies"}},
			[]string{"", "", ""}, false},
		{"inlcudeLeft - default labels", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2},
			leftInclusive: false, rightExclusive: false,
			includeLess: true, includeMore: false,
			labels: nil},
			[]string{"<=1", "1-2", ""}, false},
		{"inlcudeLeft - default labels - leftInclusive/rightExclusive", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{2, 3},
			leftInclusive: true, rightExclusive: true,
			includeLess: true, includeMore: false,
			labels: nil},
			[]string{"<2", "2-3", ""}, false},
		{"includeMore - default labels", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2},
			leftInclusive: false, rightExclusive: false,
			includeLess: false, includeMore: true,
			labels: nil},
			[]string{"", "1-2", ">2"}, false},
		{"includeMore - default labels - leftInclusive/rightExclusive", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2},
			leftInclusive: true, rightExclusive: true,
			includeLess: false, includeMore: true,
			labels: nil},
			[]string{"1-2", ">=2", ">=2"}, false},
		{"fail - no bins", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: nil,
			leftInclusive: false, rightExclusive: false,
			includeLess: false, includeMore: false,
			labels: nil},
			nil, true},
		{"fail - bins/label mismatch", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2, 3},
			leftInclusive: false, rightExclusive: false,
			includeLess: false, includeMore: false,
			labels: []string{"a", "b", "c"}},
			nil, true},
		{"fail - bad combination of inclusive/exclusive", args{
			vals: []float64{1, 2, 3}, isNull: []bool{false, false, false}, bins: []float64{1, 2, 3},
			leftInclusive: true, rightExclusive: false,
			includeLess: false, includeMore: false,
			labels: nil},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := cut(tt.args.vals, tt.args.isNull, tt.args.bins, tt.args.leftInclusive, tt.args.rightExclusive, tt.args.includeLess, tt.args.includeMore, tt.args.labels)
			if (err != nil) != tt.wantErr {
				t.Errorf("cut() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("cut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_sum(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			6, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := sum(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("sum() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("sum() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_mean(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			2, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := mean(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("mean() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("mean() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_median(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			2, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := median(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("median() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("median() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_std(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			0.816496580927726, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := std(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("std() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("std() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_count(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			3, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := count(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("count() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("count() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_min(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			1, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := min(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("min() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("min() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_max(t *testing.T) {
	type args struct {
		vals   []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  float64
		want1 bool
	}{
		{"at least one valid", args{
			[]float64{1, 2, 3}, []bool{false, false, false}, []int{0, 1, 2}},
			3, false},
		{"all null", args{
			[]float64{1, 2, 3}, []bool{false, true, true}, []int{1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := max(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("max() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("max() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_earliest(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type args struct {
		vals   []time.Time
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  time.Time
		want1 bool
	}{
		{"at least one valid", args{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{false, false, false}, []int{0, 1, 2}},
			d, false},
		{"all null", args{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{true, true, true}, []int{0, 1, 2}},
			time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := earliest(tt.args.vals, tt.args.isNull, tt.args.index)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("earliest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("earliest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_latest(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type args struct {
		vals   []time.Time
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  time.Time
		want1 bool
	}{
		{"at least one valid", args{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{false, false, false}, []int{0, 1, 2}},
			d.AddDate(0, 0, 2), false},
		{"all null", args{
			[]time.Time{d, d.AddDate(0, 0, 1), d.AddDate(0, 0, 2)}, []bool{true, true, true}, []int{0, 1, 2}},
			time.Time{}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := latest(tt.args.vals, tt.args.isNull, tt.args.index)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("latest() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("latest() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_withinWindow(t *testing.T) {
	d1 := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	d2 := time.Date(2020, 1, 1, 12, 0, 0, 0, time.UTC)
	d3 := time.Date(2020, 1, 2, 0, 0, 0, 0, time.UTC)
	d4 := time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
	d5 := time.Date(2019, 12, 31, 0, 0, 0, 0, time.UTC)
	type args struct {
		root  time.Time
		other time.Time
		d     time.Duration
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"true - itself", args{d1, d1, 24 * time.Hour}, true},
		{"true", args{d1, d2, 24 * time.Hour}, true},
		{"false - exclusive", args{d1, d3, 24 * time.Hour}, false},
		{"false", args{d1, d4, 24 * time.Hour}, false},
		{"false - before", args{d1, d5, 24 * time.Hour}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := withinWindow(tt.args.root, tt.args.other, tt.args.d); got != tt.want {
				t.Errorf("withinWindow() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_relabel(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   *valueContainer
	}{
		{"pass", fields{
			slice:  []float64{2, 3, 0},
			isNull: []bool{false, false, true},
			name:   "foo",
		}, &valueContainer{
			slice:  []int{0, 1, 2},
			isNull: []bool{false, false, true},
			name:   "foo",
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			vc.relabel()
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("relabel() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_valueContainer_fillnull(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		lambda NullFiller
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *valueContainer
	}{
		{"fill forward", fields{slice: []int{10, 1, 0, 2, 0}, isNull: []bool{true, false, true, false, true}},
			args{NullFiller{FillForward: true}},
			&valueContainer{slice: []int{0, 1, 1, 2, 2}, isNull: []bool{false, false, false, false, false}},
		},
		{"fill backward", fields{slice: []int{10, 1, 0, 2, 0}, isNull: []bool{true, false, true, false, true}},
			args{NullFiller{FillBackward: true}},
			&valueContainer{slice: []int{1, 1, 2, 2, 0}, isNull: []bool{false, false, false, false, false}},
		},
		{"fill zero", fields{slice: []int{10, 1, 0, 2, 0}, isNull: []bool{true, false, true, false, true}},
			args{NullFiller{FillZero: true}},
			&valueContainer{slice: []int{0, 1, 0, 2, 0}, isNull: []bool{false, false, false, false, false}},
		},
		{"fill float", fields{slice: []int{10, 1, 0, 2, 0}, isNull: []bool{true, false, true, false, true}},
			args{NullFiller{FillFloat: 0}},
			&valueContainer{slice: []float64{0, 1, 0, 2, 0}, isNull: []bool{false, false, false, false, false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			vc.fillnull(tt.args.lambda)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("vc.fillnull() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_valueContainer_resample(t *testing.T) {
	d := time.Date(2020, 2, 2, 12, 30, 45, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	type args struct {
		by Resampler
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *valueContainer
	}{
		{"year", fields{slice: []time.Time{d}, isNull: []bool{false}, name: "foo"},
			args{Resampler{ByYear: true}},
			&valueContainer{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
				isNull: []bool{false}, name: "foo"}},
		{"year - string", fields{slice: []string{"2020-02-02T12:30:45"}, isNull: []bool{false}, name: "foo"},
			args{Resampler{ByYear: true}},
			&valueContainer{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
				isNull: []bool{false}, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			vc.resample(tt.args.by)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("vc.resample() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_resample(t *testing.T) {
	d := time.Date(2020, 2, 2, 12, 30, 45, 100, time.UTC)
	type args struct {
		t  time.Time
		by Resampler
	}
	tests := []struct {
		name string
		args args
		want time.Time
	}{
		{"year", args{d, Resampler{ByYear: true, Location: time.UTC}}, time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
		{"month", args{d, Resampler{ByMonth: true, Location: time.UTC}}, time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)},
		{"day", args{d, Resampler{ByDay: true, Location: time.UTC}}, time.Date(2020, 2, 2, 0, 0, 0, 0, time.UTC)},
		{"week (Sunday)", args{d, Resampler{ByWeek: true, Location: time.UTC}},
			time.Date(2020, 2, 2, 0, 0, 0, 0, time.UTC)},
		{"week (Monday)", args{d, Resampler{ByWeek: true, StartOfWeek: time.Monday, Location: time.UTC}},
			time.Date(2020, 1, 27, 0, 0, 0, 0, time.UTC)},
		{"hour", args{d, Resampler{ByDuration: time.Hour, Location: time.UTC}}, time.Date(2020, 2, 2, 12, 0, 0, 0, time.UTC)},
		{"minute", args{d, Resampler{ByDuration: time.Minute, Location: time.UTC}}, time.Date(2020, 2, 2, 12, 30, 0, 0, time.UTC)},
		{"second", args{d, Resampler{ByDuration: time.Second, Location: time.UTC}}, time.Date(2020, 2, 2, 12, 30, 45, 0, time.UTC)},
		{"default", args{d, Resampler{Location: time.UTC}}, time.Date(2020, 2, 2, 12, 30, 45, 100, time.UTC)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := resample(tt.args.t, tt.args.by); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("resample() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_valueCounts(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]int
	}{
		{"pass", fields{
			slice:  []float64{1, 1, 2, 0},
			isNull: []bool{false, false, false, true},
			name:   "foo",
		}, map[string]int{"1": 2, "2": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.valueCounts(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.valueCounts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nunique(t *testing.T) {
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
		{"pass", args{[]string{"foo", "foo", "bar", ""}, []bool{false, false, false, true}, []int{0, 1, 2, 3}}, "2", false},
		{"fail", args{[]string{"", "", "", ""}, []bool{true, true, true, true}, []int{0, 1, 2, 3}}, "", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := nunique(tt.args.vals, tt.args.isNull, tt.args.index)
			if got != tt.want {
				t.Errorf("nunique() got = %v, want %v", got, tt.want)
			}
			if got1 != tt.want1 {
				t.Errorf("nunique() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_deduplicateContainerNames(t *testing.T) {
	type args struct {
		containers []*valueContainer
	}
	tests := []struct {
		name string
		args args
		want []*valueContainer
	}{
		{"pass", args{[]*valueContainer{
			{name: "foo"},
			{name: "foo"},
			{name: "bar"},
			{name: "foo"},
		}},
			[]*valueContainer{
				{name: "foo"},
				{name: "foo_1"},
				{name: "bar"},
				{name: "foo_2"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			deduplicateContainerNames(tt.args.containers)
			if !reflect.DeepEqual(tt.args.containers, tt.want) {
				t.Errorf("deduplicateContainerNames() -> %v, want %v", tt.args.containers, tt.want)
			}
		})
	}
}

func Test_lookupWithAnchor(t *testing.T) {
	type args struct {
		name    string
		labels1 []*valueContainer
		leftOn  []int
		values2 *valueContainer
		labels2 []*valueContainer
		rightOn []int
	}
	tests := []struct {
		name string
		args args
		want *Series
	}{
		{"pass", args{name: "waldo", labels1: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			leftOn:  []int{0},
			values2: &valueContainer{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "qux"},
			labels2: []*valueContainer{{slice: []float64{1, 0, 2}, isNull: []bool{false, false, false}, name: "foo"}},
			rightOn: []int{0}},
			&Series{
				values: &valueContainer{slice: []string{"", "foo"}, isNull: []bool{true, false}, name: "waldo"},
				labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lookupWithAnchor(
				tt.args.name, tt.args.labels1, tt.args.leftOn, tt.args.values2, tt.args.labels2, tt.args.rightOn); !EqualSeries(got, tt.want) {
				t.Errorf("lookupWithAnchor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_lookupDataFrameWithAnchor(t *testing.T) {
	type args struct {
		name           string
		colLevelNames  []string
		anchorLabels   []*valueContainer
		originalLabels []*valueContainer
		leftOn         []int
		lookupColumns  []*valueContainer
		lookupLabels   []*valueContainer
		rightOn        []int
		exclude        []string
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"pass", args{name: "waldo", colLevelNames: []string{"*0"},
			anchorLabels: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
			},
			originalLabels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			leftOn:         []int{0},
			lookupColumns: []*valueContainer{
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "a"},
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "b"}},
			lookupLabels: []*valueContainer{
				{slice: []float64{1, 0, 2}, isNull: []bool{false, false, false}, name: "c"},
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "a"},
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "b"}},
			rightOn: []int{0},
			exclude: []string{"c"}},

			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"", "foo"}, isNull: []bool{true, false}, name: "a"},
					{slice: []string{"", "foo"}, isNull: []bool{true, false}, name: "b"},
				},
				labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
				name:   "waldo", colLevelNames: []string{"*0"},
			}},
		{"exclude", args{name: "waldo", colLevelNames: []string{"*0"},
			anchorLabels: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
			},
			originalLabels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			leftOn:         []int{0},
			lookupColumns: []*valueContainer{
				{slice: []float64{1, 0, 2}, isNull: []bool{false, true, false}, name: "a"},
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "b"}},
			lookupLabels: []*valueContainer{
				{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "c"},
				{slice: []float64{1, 0, 2}, isNull: []bool{false, true, false}, name: "a"},
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, name: "b"}},
			rightOn: []int{1},
			exclude: []string{"a"}},

			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"", "foo"}, isNull: []bool{true, false}, name: "b"},
				},
				labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
				name:   "waldo", colLevelNames: []string{"*0"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lookupDataFrameWithAnchor(
				tt.args.name, tt.args.colLevelNames, tt.args.anchorLabels,
				tt.args.originalLabels, tt.args.leftOn, tt.args.lookupColumns, tt.args.lookupLabels,
				tt.args.rightOn, tt.args.exclude); !EqualDataFrames(got, tt.want) {
				t.Errorf("lookupDataFrameWithAnchor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_uniqueIndex(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   []int
	}{
		{"pass", fields{
			slice:  []float64{1, 1, 2, 0},
			isNull: []bool{false, false, false, true},
			name:   "foo",
		}, []int{0, 2, 3},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.uniqueIndex(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.uniqueIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_multiUniqueIndex(t *testing.T) {
	type args struct {
		containers []*valueContainer
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{"pass", args{[]*valueContainer{
			{slice: []float64{1, 1, 2, 1}, isNull: []bool{false, false, false, false}, name: "foo"},
			{slice: []int{0, 0, 2, 3}, isNull: []bool{false, false, false, false}, name: "qux"},
		}}, []int{0, 2, 3}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := multiUniqueIndex(tt.args.containers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("multiUniqueIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_dtype(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"pass", fields{slice: []float64{1}, isNull: []bool{false}, name: "foo"}, "[]float64"},
		{"pass", fields{slice: []string{"1"}, isNull: []bool{false}, name: "foo"}, "[]string"},
		{"pass", fields{slice: [][]string{{"1"}}, isNull: []bool{false}, name: "foo"}, "[][]string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.dtype(); got != tt.want {
				t.Errorf("valueContainer.dtype() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dropFromContainers(t *testing.T) {
	type args struct {
		name       string
		containers []*valueContainer
	}
	tests := []struct {
		name    string
		args    args
		want    []*valueContainer
		wantErr bool
	}{
		{"pass", args{"foo", []*valueContainer{
			{name: "foo"},
			{name: "bar"},
		}}, []*valueContainer{
			{name: "bar"},
		}, false},
		{"fail - bad column", args{"corge", []*valueContainer{
			{name: "foo"},
			{name: "bar"},
		}}, nil, true},
		{"fail - last column", args{"foo", []*valueContainer{
			{name: "foo"},
		}}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := dropFromContainers(tt.args.name, tt.args.containers)
			if (err != nil) != tt.wantErr {
				t.Errorf("dropFromContainers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("dropFromContainers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEqualSeries(t *testing.T) {
	type args struct {
		a *Series
		b *Series
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"pass", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
		}, true},
		{"pass - both nil", args{
			a: nil,
			b: nil,
		}, true},
		{"fail - nil", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
			b: nil,
		}, false},
		{"fail - values", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
		}, false},
		{"fail - labels", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
		}, false},
		{"fail - shared data", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				sharedData: true,
				err:        errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				sharedData: false,
				err:        errors.New("foo")},
		}, false},
		{"fail - has err", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: nil},
		}, false},
		{"fail - err value", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				err: errors.New("bar")},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EqualSeries(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("EqualSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEqualDataFrames(t *testing.T) {
	type args struct {
		a *DataFrame
		b *DataFrame
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"pass", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
		}, true},
		{"pass - both nil", args{
			a: nil,
			b: nil,
		}, true},
		{"fail - nil", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: nil,
		}, false},
		{"fail - values", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{2}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
		}, false},
		{"fail - labels", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
		}, false},
		{"fail - colLevel names", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*1"},
				name:          "baz",
				err:           errors.New("foo")},
		}, false},
		{"fail - names", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "corge",
				err:           errors.New("foo")},
		}, false},
		{"fail - has err", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           nil},
		}, false},
		{"fail - err value", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("bar")},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := EqualDataFrames(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("EqualDataFrames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_equalGroupedSeries(t *testing.T) {
	type args struct {
		a *GroupedSeries
		b *GroupedSeries
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"pass", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			true},
		{"pass - both nil", args{
			a: nil,
			b: nil,
		}, true},
		{"fail - nil", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: nil,
		}, false},
		{"fail - orderedKeys", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"bar"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - rowIndices", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{1}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - labels", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "baz"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - nil series", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				err:         errors.New("foo")}},
			false},
		{"fail - series", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "corge"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - has err", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: nil}},
			false},
		{"fail - err value", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("bar")}},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := equalGroupedSeries(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("equalGroupedSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_equalGroupedDataFrames(t *testing.T) {
	type args struct {
		a *GroupedDataFrame
		b *GroupedDataFrame
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"pass", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			true},
		{"pass - both nil", args{
			a: nil,
			b: nil,
		}, true},
		{"fail - nil", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: nil,
		}, false},
		{"fail - orderedKeys", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"bar"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - rowIndices", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{1}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - labels", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - has df", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				err:         errors.New("foo")}},
			false},
		{"fail - df", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "corge"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - has err", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: nil}},
			false},
		{"fail - err value", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				},
				err: errors.New("bar")}},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := equalGroupedDataFrames(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("equalGroupedDataFrames() = %v, want %v", got, tt.want)
			}
		})
	}
}
