package tada

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"reflect"
	"testing"
	"time"

	"cloud.google.com/go/civil"
)

type mockClock struct{}

func (c mockClock) now() time.Time {
	return time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
}

var mockID = tadaID + fmt.Sprint(int(mockClock{}.now().UnixNano()))

func TestMain(m *testing.M) {
	DisableWarnings()
	clock = mockClock{}
	defer func() { clock = realClock{} }()
	code := m.Run()
	os.Exit(code)
}

func Test_errorWarning(t *testing.T) {
	EnableWarnings()
	type args struct {
		err error
	}
	tests := []struct {
		name string
		args args
	}{
		{"pass", args{errors.New("foo")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			b := new(bytes.Buffer)
			log.SetOutput(b)
			errorWarning(tt.args.err)
			if b.String() == "" {
				t.Errorf("errorWarning() logged , want error")
			}
		})
	}
	log.SetOutput(os.Stdout)
	DisableWarnings()
}

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
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, id: mockID, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, id: mockID, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, id: mockID, name: "*0"}},
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
			&valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "0"}, false},
		{"fail - empty slice", args{[]float64{}, "0"},
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
				t.Errorf("makeValueContainerFromInterface() = %#v, want %#v", got, tt.want)
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
				{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "1"}},
			false,
		},
		{"pass, prefix", args{[]interface{}{[]float64{1}, []string{"foo"}}, true},
			[]*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "*0"},
				{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "*1"}},
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
			}
		})
	}
}

func Test_findMatchingKeysBetweenTwoContainers(t *testing.T) {
	type args struct {
		labels1 []*valueContainer
		labels2 []*valueContainer
	}
	tests := []struct {
		name    string
		args    args
		want    []int
		want1   []int
		wantErr bool
	}{
		{"1 match", args{
			labels1: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*1"},
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"},
			},
			labels2: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
		}, []int{1}, []int{0}, false},
		{"duplicates", args{
			labels1: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"},
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"},
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*1"},
			},
			labels2: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
		}, []int{0}, []int{0}, false},
		{"no matches", args{
			labels1: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*1"},
			},
			labels2: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
		}, nil, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1, err := findMatchingKeysBetweenTwoContainers(tt.args.labels1, tt.args.labels2)
			if (err != nil) != tt.wantErr {
				t.Errorf("findMatchingKeysBetweenTwoContainers() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("findMatchingKeysBetweenTwoContainers() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("findMatchingKeysBetweenTwoContainers() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_setNullsFromInterface(t *testing.T) {
	type args struct {
		input interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []bool
		wantErr bool
	}{
		{"float", args{[]float64{1, math.NaN()}}, []bool{false, true}, false},
		{"int", args{[]int{0}}, []bool{false}, false},
		{"string", args{[]string{"foo", ""}}, []bool{false, true}, false},
		{"civil.date", args{[]civil.Date{civil.DateOf(time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC)), {}}}, []bool{false, true}, false},
		{"civil.time", args{[]civil.Time{civil.TimeOf(time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC)), {Second: -1}}}, []bool{false, true}, false},
		{"dateTime", args{[]time.Time{time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC), time.Date(1, 1, 1, 0, 0, 0, 0, time.UTC), {}}}, []bool{false, true, true}, false},
		{"interface", args{[]interface{}{
			int(1), uint(1), float32(1), float64(1), time.Date(2, 1, 1, 0, 0, 0, 0, time.UTC), "foo",
			math.NaN(), "", time.Time{}}},
			[]bool{false, false, false, false, false, false,
				true, true, true}, false},
		{"nested string", args{[][]string{{"foo"}, {}}}, []bool{false, true}, false},
		{"nested civil.date", args{[][]civil.Date{{{Year: 2020, Month: 1, Day: 1}, {}}, {}}}, []bool{false, true}, false},
		{"map", args{[]map[string]string{{"foo": "bar"}, {}}}, []bool{false, true}, false},
		{"not explicitly supported value", args{[]complex64{1}}, []bool{false}, false},
		{"empty", args{[]int{}}, []bool{}, false},
		{"nil", args{nil}, []bool{}, false},
		{"fail - not slice", args{"foo"}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := setNullsFromInterface(tt.args.input)
			if (err != nil) != tt.wantErr {
				t.Errorf("setNullsFromInterface() error = %v, want %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setNullsFromInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_isNullInterface(t *testing.T) {
	type args struct {
		i interface{}
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"null float64", args{math.NaN()}, true},
		{"null string", args{""}, true},
		{"null time.Time", args{time.Time{}}, true},
		{"null civil.Date", args{civil.Date{}}, true},
		{"null civil.Time", args{civil.Time{Second: -1}}, true},
		{"nil", args{nil}, true},
		{"float64", args{float64(1)}, false},
		{"string", args{"foo"}, false},
		{"time.Time", args{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}, false},
		{"civil.Date", args{civil.Date{Year: 2020, Month: 1, Day: 1}}, false},
		{"civil.Time", args{civil.Time{Hour: 12}}, false},
		{"not explicitly supported - bool", args{[]bool{true}}, false},
		{"not explicitly supported - complex64", args{complex64(1)}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := isNullInterface(tt.args.i)
			if got != tt.want {
				t.Errorf("isNullInterface() = %v, want %v", got, tt.want)
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
		cache  []string
	}
	tests := []struct {
		name   string
		fields fields
		want   *valueContainer
	}{
		{"pass", fields{
			slice:  []float64{1},
			isNull: []bool{false},
			name:   "foo",
			cache:  []string{"foo"},
		},
			&valueContainer{
				slice:  []float64{1},
				isNull: []bool{false},
				name:   "foo",
				cache:  []string{"foo"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				name:   tt.fields.name,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
			}
			got := vc.copy()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.copy() = %v, want %v", got, tt.want)
			}
			got.slice.([]float64)[0] = 2
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original values")
			}
			got = vc.copy()
			got.name = "qux"
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original values name")
			}
			got = vc.copy()
			got.isNull[0] = true
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original isNull")
			}
			got = vc.copy()
			got.cache[0] = "bar"
			if reflect.DeepEqual(vc, got) {
				t.Errorf("valueContainer.copy() retained reference to original cache")
			}
		})
	}
}

func Test_makeDefaultLabels(t *testing.T) {
	type args struct {
		min       int
		max       int
		usePrefix bool
	}
	tests := []struct {
		name       string
		args       args
		wantLabels *valueContainer
	}{
		{"normal", args{0, 2, true}, &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
		{"normal", args{0, 2, false}, &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "0"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLabels := makeDefaultLabels(tt.args.min, tt.args.max, tt.args.usePrefix)
			if !reflect.DeepEqual(gotLabels, tt.wantLabels) {
				t.Errorf("makeDefaultLabels() gotLabels = %v, want %v", gotLabels, tt.wantLabels)
			}
		})
	}
}

func Test_intersection(t *testing.T) {
	type args struct {
		slices [][]int
		maxLen int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{"1 match", args{[][]int{{0, 1}, {1, 2}}, 3}, []int{1}},
		{"all max length", args{[][]int{{1, 0}, {0, 1}}, 2}, []int{0, 1}},
		{"no matches", args{[][]int{{0, 1, 2}, {3}}, 3}, []int{}},
		{"only one slice", args{[][]int{{0, 1}}, 3}, []int{0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := intersection(tt.args.slices, tt.args.maxLen); !reflect.DeepEqual(got, tt.want) {
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
			how: "left", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}, id: mockID},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []int{10, 0}, isNull: []bool{false, true}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, cache: []string{"0", "1"}},
				}}, wantErr: false,
		},
		{name: "right", args: args{
			how: "right", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}, id: mockID},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, id: mockID},
				labels: []*valueContainer{
					{slice: []int{0, 10}, isNull: []bool{false, false}, cache: []string{"0", "10"}},
				}}, wantErr: false,
		},
		{name: "inner", args: args{
			how: "inner", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}, id: mockID},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []int{10}, isNull: []bool{false}, id: mockID},
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
			values1: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []int{10, 20}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, id: mockID, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []int{10, 0}, isNull: []bool{false, true}, id: mockID, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux",
					cache: []string{"0", "1"}}},
				name: "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "left - nulls", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, id: mockID, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []string{"", "c"}, isNull: []bool{true, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux",
					cache: []string{"0", "1"}}},
				name: "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "left - repeated label appears only once", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []string{"c", "d"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{1, 1}, isNull: []bool{false, false}, id: mockID, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []string{"", "c"}, isNull: []bool{true, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux",
					cache: []string{"0", "1"}}},
				name: "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "left - exclude named column", args: args{
			how: "left", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "baz"},
				{slice: []string{"c"}, isNull: []bool{false}, id: mockID, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "quux"}}, rightOn: []int{1},
			excludeRight: []string{"baz"}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []string{"", "c"}, isNull: []bool{true, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux",
					cache: []string{"0", "1"}}},
				name: "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "right", args: args{
			how: "right", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []int{10, 20}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, id: mockID, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []float64{1, 0}, isNull: []bool{false, true}, id: mockID, name: "foo"}},
				labels: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, id: mockID, name: "quux",
					cache: []string{"0", "10"}}},
				name: "baz", colLevelNames: []string{"*0"},
			},
			wantErr: false,
		},
		{name: "inner", args: args{
			how: "inner", name: "baz", colLevelNames: []string{"*0"},
			values1: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}}, leftOn: []int{0},
			values2: []*valueContainer{{slice: []int{10, 20}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}, id: mockID, name: "quux"}}, rightOn: []int{0}},
			want: &DataFrame{
				values: []*valueContainer{{slice: []int{10}, isNull: []bool{false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "qux"}},
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
	}
	tests := []struct {
		name                   string
		args                   args
		wantNewContainers      []*valueContainer
		wantOriginalRowIndexes [][]int
		wantOrderedKeys        []string
	}{
		{name: "single level",
			args: args{containers: []*valueContainer{
				{slice: []string{"bar", "qux", "bar"}, isNull: []bool{false, false, false}, id: mockID, name: "baz"},
			}},
			wantNewContainers: []*valueContainer{
				{slice: []string{"bar", "qux"}, isNull: []bool{false, false}, id: mockID, name: "baz"},
			},
			wantOriginalRowIndexes: [][]int{{0, 2}, {1}},
			wantOrderedKeys:        []string{"bar", "qux"}},
		{name: "multi level",
			args: args{containers: []*valueContainer{
				{slice: []float64{1, 1, 1}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "qux", "bar"}, isNull: []bool{false, false, false}, id: mockID, name: "baz"},
			}},
			wantNewContainers: []*valueContainer{
				{slice: []float64{1, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "qux"}, isNull: []bool{false, false}, id: mockID, name: "baz"},
			},
			wantOriginalRowIndexes: [][]int{{0, 2}, {1}},
			wantOrderedKeys:        []string{"1|bar", "1|qux"}},
		{name: "single level - null",
			args: args{containers: []*valueContainer{
				{slice: []string{"bar", "", "bar"}, isNull: []bool{false, true, false}, id: mockID, name: "baz"},
			}},
			wantNewContainers: []*valueContainer{
				{slice: []string{"bar", ""}, isNull: []bool{false, true}, id: mockID, name: "baz"},
			},
			wantOriginalRowIndexes: [][]int{{0, 2}, {1}},
			wantOrderedKeys:        []string{"bar", ""}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNewContainers, gotOriginalRowIndexes, gotOrderedKeys := reduceContainers(tt.args.containers)
			if !reflect.DeepEqual(gotNewContainers, tt.wantNewContainers) {
				t.Errorf("reduceContainers() gotNewContainers = %v, want %v", gotNewContainers[0], tt.wantNewContainers[0])
			}
			if !reflect.DeepEqual(gotOriginalRowIndexes, tt.wantOriginalRowIndexes) {
				t.Errorf("reduceContainers() gotOriginalRowIndexes = %v, want %v", gotOriginalRowIndexes, tt.wantOriginalRowIndexes)
			}
			if !reflect.DeepEqual(gotOrderedKeys, tt.wantOrderedKeys) {
				t.Errorf("reduceContainers() gotOrderedKeys = %v, want %v", gotOrderedKeys, tt.wantOrderedKeys)
			}
		})
	}
}

func Test_reduceContainersForLookup(t *testing.T) {
	type args struct {
		containers []*valueContainer
	}
	tests := []struct {
		name string
		args args
		want map[string]int
	}{
		{name: "single level",
			args: args{containers: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "qux", "bar"}, isNull: []bool{false, false, false}, id: mockID, name: "baz"},
			}},
			want: map[string]int{"1|bar": 0, "2|qux": 1, "3|bar": 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := reduceContainersForLookup(tt.args.containers); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("reduceContainersForLookup() = %v, want %v", got, tt.want)
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
				{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "corge"},
				{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "waldo"},
			},
		},
		{"create nulls from interface", args{
			slices: []interface{}{[]string{"foo", ""}, []float64{1, 2}},
			isNull: nil,
			names:  []string{"corge", "waldo"},
		},
			[]*valueContainer{
				{slice: []string{"foo", ""}, isNull: []bool{false, true}, id: mockID, name: "corge"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "waldo"},
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
				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "corge"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "waldo"},
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

func Test_valueContainer_shift(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		id     string
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
		{"positive", fields{slice: []string{"1", "2", "3"}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
			args{1},
			&valueContainer{
				slice: []string{"", "1", "2"}, isNull: []bool{true, false, false}, id: mockID, name: "foo"}},
		{"negative", fields{slice: []string{"1", "2", "3"}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
			args{-1},
			&valueContainer{
				slice: []string{"2", "3", ""}, isNull: []bool{false, false, true}, id: mockID, name: "foo"}},
		{"too many positions", fields{slice: []string{"1", "2", "3"}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
			args{5},
			&valueContainer{
				slice: []string{"", "", ""}, isNull: []bool{true, true, true}, id: mockID, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			if got := vc.shift(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("vc.shift() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_append(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		id     string
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
		{"floats", fields{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{&valueContainer{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "bar"}},
			&valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
		{"floats and ints", fields{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{&valueContainer{slice: []int{2}, isNull: []bool{false}, id: mockID, name: "bar"}},
			&valueContainer{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
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

func Test_indexOfContainer(t *testing.T) {
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
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "foo"}}},
			1, false},
		{"pass - number as name", args{"1", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "1"}}},
			0, false},
		{"pass - search by id", args{mockID, []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "1"}}},
			0, false},
		{"fail - uppercase search", args{"FOO", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "foo"}}},
			0, true},
		{"fail - title case name", args{"foo", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "Foo"}}},
			0, true},
		{"fail - not found", args{"foo", []*valueContainer{
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
			{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "qux"}}},
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

func Test_valueContainer_cut(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		id     string
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
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{0, 2, 4}, andLess: false, andMore: false, labels: []string{"low", "high"}},
			[]string{"low", "low", "high", "high"}, false},
		{"supplied labels, no less, no more, with null",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, true}, id: mockID, name: "foo"},
			args{bins: []float64{0, 2, 4}, andLess: false, andMore: false, labels: []string{"low", "high"}},
			[]string{"low", "low", "high", ""}, false},
		{"supplied labels, less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: false, labels: []string{"low", "medium", "high"}},
			[]string{"low", "medium", "high", ""}, false},
		{"supplied labels, no less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: true, labels: []string{"low", "medium", "high"}},
			[]string{"", "low", "medium", "high"}, false},
		{"supplied labels, less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: true, labels: []string{"low", "medium", "high", "higher"}},
			[]string{"low", "medium", "high", "higher"}, false},
		{"default labels, no less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{0, 2, 4}, andLess: false, andMore: false, labels: nil},
			[]string{"0-2", "0-2", "2-4", "2-4"}, false},
		{"default labels, less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: false, labels: nil},
			[]string{"<=1", "1-2", "2-3", ""}, false},
		{"default labels, no less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: true, labels: nil},
			[]string{"", "1-2", "2-3", ">3"}, false},
		{"default labels, less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: true, labels: nil},
			[]string{"<=1", "1-2", "2-3", ">3"}, false},
		{"fail: zero bins",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{}, andLess: false, andMore: false, labels: []string{}},
			nil, true},
		{"fail: bin - label mismatch",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: false, labels: []string{"foo"}},
			nil, true},
		{"fail: bin - label mismatch, less, no more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: true, andMore: false, labels: []string{"foo", "bar"}},
			nil, true},
		{"fail: bin - label mismatch, no less, more",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{1, 2, 3}, andLess: false, andMore: true, labels: []string{"foo", "bar"}},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
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

func Test_valueContainer_pcut(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		id     string
	}
	type args struct {
		bins   []float64
		config *Binner
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []string
		wantErr bool
	}{
		{"default labels",
			fields{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{0, .5, 1}, config: nil},
			[]string{"0-0.5", "0-0.5", "0.5-1", "0.5-1"}, false},
		{"supplied labels",
			fields{slice: []float64{-1, 2, 6, 10, 12}, isNull: []bool{false, false, false, false, false}},
			args{bins: []float64{0, .5, 1}, config: &Binner{Labels: []string{"Bottom 50%", "Top 50%"}}},
			[]string{"Bottom 50%", "Bottom 50%", "Bottom 50%", "Top 50%", "Top 50%"}, false},
		{"default labels - andLess",
			fields{slice: []float64{-1, 2, 6, 10, 12}, isNull: []bool{false, false, false, false, false}},
			args{bins: []float64{.25, .5, 1}, config: &Binner{AndLess: true}},
			[]string{"<0.25", "<0.25", "0.25-0.5", "0.5-1", "0.5-1"}, false},
		{"default labels - andMore",
			fields{slice: []float64{-1, 2, 6, 10, 12}, isNull: []bool{false, false, false, false, false}},
			args{bins: []float64{0, .25, .5}, config: &Binner{AndMore: true}},
			[]string{"0-0.25", "0-0.25", "0.25-0.5", ">=0.5", ">=0.5"}, false},
		{"default labels, nulls, repeats",
			fields{slice: []float64{5, 0, 6, 7, 7, 7, 8},
				isNull: []bool{false, true, false, false, false, false, false}},
			args{bins: []float64{0, .2, .4, .6, .8, 1}, config: nil},
			[]string{"0-0.2", "", "0-0.2", "0.2-0.4", "0.2-0.4", "0.2-0.4", "0.8-1"}, false},
		{"fail: above 1",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{0, .5, 1.5}, config: nil},
			nil, true},
		{"fail: below 0",
			fields{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{bins: []float64{-0.1, .5, 1}, config: nil},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			got, err := vc.pcut(tt.args.bins, tt.args.config)
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
		id     string
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
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: Float64, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"float - convert from string",
			fields{slice: []string{"3", "1", "0", "2"}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: Float64, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"float - no nulls - descending",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: Float64, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"float - nulls",
			fields{slice: []float64{3, 1, 0, 2}, isNull: []bool{false, false, true, false}, id: mockID, name: "foo"},
			args{dtype: Float64, ascending: true, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
		{"strings - no nulls",
			fields{slice: []string{"foo", "bar", "a", "baz"}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: String, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"strings - convert from float",
			fields{slice: []string{"3", "11", "0", "2"}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: String, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"strings - no nulls - descending",
			fields{slice: []string{"foo", "bar", "a", "baz"}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: String, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"strings - nulls",
			fields{slice: []string{"foo", "bar", "a", "baz"}, isNull: []bool{false, false, true, false}, id: mockID, name: "foo"},
			args{dtype: String, ascending: true, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
		{"datetime - no nulls",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: DateTime, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"datetime - convert from string",
			fields{slice: []string{"2020-01-04", "2020-01-02", "2020-01-01", "2020-01-03"}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: DateTime, ascending: true, index: []int{0, 1, 2, 3}}, []int{2, 1, 3, 0}},
		{"datetime - no nulls - descending",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: DateTime, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"datetime - nulls",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, true, false}, id: mockID, name: "foo"},
			args{dtype: DateTime, ascending: true, index: []int{0, 1, 2, 3}}, []int{1, 3, 0, 2}},
		{"civil.Date - no nulls - descending",
			fields{slice: []time.Time{d.AddDate(0, 0, 2), d, d.AddDate(0, 0, -1), d.AddDate(0, 0, 1)}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: Date, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
		{"civil.Time - no nulls - descending",
			fields{slice: []time.Time{
				time.Date(1, 1, 1, 10, 30, 0, 0, time.UTC),
				time.Date(1, 1, 1, 9, 45, 0, 0, time.UTC),
				time.Date(1, 1, 1, 9, 30, 0, 0, time.UTC),
				time.Date(1, 1, 1, 9, 45, 1, 0, time.UTC)},
				isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			args{dtype: Date, ascending: false, index: []int{0, 1, 2, 3}}, []int{0, 3, 1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
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
		// {"multi sort - floats",
		// 	args{[]*valueContainer{
		// 		{slice: []float64{2, 1, 2}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
		// 		{slice: []float64{3, 2, 1}, isNull: []bool{false, false, false}, id: mockID, name: "bar"},
		// 	}, []Sorter{{Name: "foo"}, {Name: "bar"}}},
		// 	[]int{1, 2, 0}, false},
		{"multi sort - floats - ordered repeats",
			args{[]*valueContainer{
				{slice: []float64{2, 2, 1}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
				{slice: []float64{1, 2, 1}, isNull: []bool{false, false, false}, id: mockID, name: "bar"},
			}, []Sorter{{Name: "foo"}, {Name: "bar"}}},
			[]int{2, 0, 1}, false},
		{"multi sort - converted string to date + string",
			args{[]*valueContainer{
				{slice: []string{"2020-01-02 00:00:00 +0000 UTC", "2020-01-02 00:00:00 +0000 UTC", "2020-01-01 00:00:00 +0000 UTC", "2020-01-01 00:00:00 +0000 UTC"}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
				{slice: []string{"foo", "qux", "qux", "foo"}, isNull: []bool{false, false, false, false}, id: mockID, name: "bar"},
			}, []Sorter{{Name: "foo", DType: DateTime}, {Name: "bar", DType: String}}},
			[]int{3, 2, 0, 1}, false},
		{"multi sort - converted string to date + string",
			args{[]*valueContainer{
				{slice: []time.Time{d, d1, d, d1}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
				{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, id: mockID, name: "bar"},
			}, []Sorter{{Name: "foo", DType: DateTime}, {Name: "bar", DType: String}}},
			[]int{2, 0, 3, 1}, false},
		{"fail - bad container",
			args{[]*valueContainer{
				{slice: []float64{2, 1, 2}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
				{slice: []float64{3, 2, 1}, isNull: []bool{false, false, false}, id: mockID, name: "bar"},
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
				{".5", "3", "baz", "2020-01-01T12:30:00Z00:00", "2020-01-02", "true"},
				{".5", "3", "baz", "2020-01-01T12:30:00Z00:00", "2020-01-02", "true"}},
		},
		{"3x rows",
			args{
				[]map[string]int{
					{"float": 1, "string": 0},
					{"float": 0, "string": 1}},
				3},
			[][]string{{".5", "baz"}, {".5", "baz"}, {".5", "baz"}},
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

func Test_mockString(t *testing.T) {
	type args struct {
		dtype   string
		nullPct float64
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"pass", args{"string", .99999999}, ""},
		{"pass", args{"string", .00001}, "baz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := mockString(tt.args.dtype, tt.args.nullPct); got != tt.want {
				t.Errorf("mockString() = %v, want %v", got, tt.want)
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
		{"datetime", args{"2020-01-01 03:00:00 +0000 UTC"}, "datetime"},
		{"date", args{"2020-01-01"}, "date"},
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
		cache  []string
		id     string
	}
	type args struct {
		apply ApplyFn
		index []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"float", fields{
			slice:  []float64{1, 2},
			isNull: []bool{false, false},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				vals := slice.([]float64)
				ret := make([]float64, len(vals))
				for i := range ret {
					ret[i] = vals[i] * 2
				}
				return ret
			}, nil},
			&valueContainer{slice: []float64{2, 4}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			false,
		},
		{"int to float - change null", fields{
			slice:  []int{1, 2},
			isNull: []bool{false, false},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				vals := slice.([]int)
				ret := make([]float64, len(vals))
				for i := range ret {
					if i == 0 {
						isNull[i] = true
					}
					ret[i] = float64(vals[i]) * 2
				}
				return ret
			}, nil},
			&valueContainer{slice: []float64{2, 4}, isNull: []bool{true, false}, id: mockID, name: "foo"},
			false,
		},
		{"subset - with index", fields{
			slice:  []int{1, 2},
			isNull: []bool{false, false},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				vals := slice.([]int)
				ret := make([]int, len(vals))
				for i := range ret {
					ret[i] = vals[i] * 2
				}
				return ret
			}, []int{1}},
			&valueContainer{slice: []int{1, 4}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			false,
		},
		{"fail - does not return slice (resets isNulls to original)", fields{
			slice:  []float64{1, 2},
			isNull: []bool{false, false},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				isNull[0] = true
				return "foo"
			}, nil},
			&valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			true,
		},
		{"fail - does not return equal length slice (resets isNulls to original)", fields{
			slice:  []float64{1, 2},
			isNull: []bool{false, false},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				isNull[0] = true
				return []float64{1}
			}, nil},
			&valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			true,
		},
		{"fail - with index - wrong type (resets isNulls to original)", fields{
			slice:  []float64{1, 2, 3},
			isNull: []bool{false, true, true},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				vals := slice.([]float64)
				ret := make([]int, len(vals))
				for i := range ret {
					isNull[i] = false
					ret[i] = int(vals[i])
				}
				return ret
			}, []int{1, 2}},
			&valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, true}, id: mockID, name: "foo"},
			true,
		},
		{"fail - out of range", fields{
			slice:  []float64{1, 2},
			isNull: []bool{false, false},
			id:     mockID,
			name:   "foo"},
			args{func(slice interface{}, isNull []bool) interface{} {
				isNull[0] = true
				return []float64{1}
			}, []int{2}},
			&valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				cache:  tt.fields.cache,
				id:     tt.fields.id,
			}
			err := vc.apply(tt.args.apply, tt.args.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.apply() error = %v, want %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("valueContainer.apply() -> %v, want %v", vc, tt.want)
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
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, name: "foo", input: "corge", requiredLen: 2},
			[]*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "corge"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, false,
		},
		{"overwrite - reset cache", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo",
					cache: []string{"1", "2"}},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, name: "foo", input: []int{3, 4}, requiredLen: 2},
			[]*valueContainer{
				{slice: []int{3, 4}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, false,
		},
		{"append", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, name: "corge", input: []int{3, 4}, requiredLen: 2},
			[]*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, id: mockID, name: "corge"},
			}, false,
		},
		{"fail - wrong length", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, name: "foo", input: []float64{0, 1, 2, 3, 4}, requiredLen: 2},
			nil, true,
		},
		{"fail - unsupported input", args{
			cols: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, id: mockID, name: "qux"},
			}, name: "foo", input: 1, requiredLen: 2},
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
		includeLabels bool
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
					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args: args{includeLabels: true},
			want: [][]string{
				{"*0", "foo", "bar"},
				{"0", "1", "a"},
				{"1", "2", "b"},
			},
			wantErr: false},
		{name: "one col level - ignore labels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args: args{includeLabels: false},
			want: [][]string{
				{"foo", "bar"},
				{"1", "a"},
				{"2", "b"},
			},
			wantErr: false},
		{name: "two col levels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo|baz"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "bar|qux"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0", "*1"},
			},
			args: args{includeLabels: true},
			want: [][]string{
				{"", "foo", "bar"},
				{"*0", "baz", "qux"},
				{"0", "1", "a"},
				{"1", "2", "b"},
			}},
		{name: "two label levels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"},
					{slice: []int{10, 11}, isNull: []bool{false, false}, id: mockID, name: "*1"},
				},
				colLevelNames: []string{"*0"},
			},
			args: args{includeLabels: true},
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
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"},
					{slice: []int{10, 11}, isNull: []bool{false, false}, id: mockID, name: "*1"},
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
			got, err := df.toCSVByRows(tt.args.includeLabels)
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
		config *readConfig
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header row, 2 columns, no label levels",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: &readConfig{numHeaderRows: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"1 header row, 1 column, 1 label level",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: &readConfig{numHeaderRows: 1, numLabelLevels: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				colLevelNames: []string{"*0"}},
			false},
		{"misaligned: wrong numbers of columns: too many",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "2", "3"}},
				config: &readConfig{numHeaderRows: 1}},
			nil,
			true},
		{"misaligned: wrong numbers of columns: too few",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1"}},
				config: &readConfig{numHeaderRows: 1}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readCSVByRows(tt.args.csv, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("readCSVByRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("readCSVByRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readCSVByCols(t *testing.T) {
	type args struct {
		csv    [][]string
		config *readConfig
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header row, 2 columns, no label levels",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6"}},
				config: &readConfig{numHeaderRows: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"1 header row, 1 column, 1 label levels",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6"}},
				config: &readConfig{numHeaderRows: 1, numLabelLevels: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				},
				colLevelNames: []string{"*0"}},
			false},
		{"misaligned lines: too few",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5"}},
				config: &readConfig{numHeaderRows: 1, numLabelLevels: 1}},
			nil,
			true},
		{"misaligned lines: too many",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6,", "6"}},
				config: &readConfig{numHeaderRows: 1, numLabelLevels: 1}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readCSVByCols(tt.args.csv, tt.args.config)
			if (err != nil) != tt.wantErr {
				t.Errorf("readCSVByRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("readCSVByCols() = %v, want %v", got, tt.want)
			}
		})
	}
}

type testStruct struct {
	Name string
	Age  int
}

type testStructNoFields struct {
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
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "Name"},
				{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
			false},
		{"pass - partial", args{[]testStruct{{Name: "foo"}, {Name: "bar"}}},
			[]*valueContainer{
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "Name"},
				{slice: []int{0, 0}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
			false},
		{"fail - not slice", args{testStruct{"foo", 1}},
			nil, true},
		{"fail - not struct", args{[]string{"foo"}},
			nil, true},
		{"fail - empty", args{[]testStruct{}},
			nil, true},
		{"fail - no fields", args{[]testStructNoFields{{}}},
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

func Test_StdDev(t *testing.T) {
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
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  int
		want1 bool
	}{
		{"at least one valid", args{
			[]bool{false, false, false}, []int{0, 1, 2}},
			3, false},
		{"one null", args{
			[]bool{true, false, false}, []int{0, 1, 2}},
			2, false},
		{"all null", args{
			[]bool{true, true, true}, []int{0, 1, 2}},
			0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := count(nil, tt.args.isNull, tt.args.index)
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

func Test_valueContainer_fillnull(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		cache  []string
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
		{"reset cache - fill zero",
			fields{
				slice: []string{"", "foo"}, isNull: []bool{true, false},
				cache: []string{"", "foo"}},
			args{NullFiller{FillZero: true}},
			&valueContainer{slice: []string{"", "foo"}, isNull: []bool{false, false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				cache:  tt.fields.cache,
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
	tz, err := time.LoadLocation("America/Los_Angeles")
	if err != nil {
		log.Fatal(err)
	}
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		cache  []string
		id     string
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
		{"resets cache",
			fields{slice: []time.Time{d}, isNull: []bool{false}, id: mockID, name: "foo",
				cache: []string{"2020-02-02 12:30:45 +0000 UTC"}},
			args{Resampler{ByYear: true, Location: time.UTC}},
			&valueContainer{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
				isNull: []bool{false}, id: mockID, name: "foo"}},
		{"year - start and return to civil date",
			fields{slice: []civil.Date{civil.DateOf(d)}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{Resampler{ByDay: true}},
			&valueContainer{slice: []civil.Date{{Year: 2020, Month: 2, Day: 2}},
				isNull: []bool{false}, id: mockID, name: "foo"}},
		{"hour - start and return to civil time",
			fields{slice: []civil.Time{civil.TimeOf(d)}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{Resampler{ByDuration: time.Hour}},
			&valueContainer{slice: []civil.Time{{Hour: 12}},
				isNull: []bool{false}, id: mockID, name: "foo"}},
		{"year - string - sets Location automatically ", fields{slice: []string{"2020-02-02T12:30:45Z"}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{Resampler{ByYear: true}},
			&valueContainer{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
				isNull: []bool{false}, id: mockID, name: "foo"}},
		{"day - resets Location automatically ", fields{slice: []time.Time{time.Date(2019, 12, 31, 18, 30, 0, 0, tz)}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{Resampler{ByDay: true}},
			&valueContainer{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
				isNull: []bool{false}, id: mockID, name: "foo"}},
		{"day - retains Location ", fields{slice: []time.Time{time.Date(2019, 12, 31, 18, 30, 0, 0, tz)}, isNull: []bool{false}, id: mockID, name: "foo"},
			args{Resampler{ByDay: true, Location: tz}},
			&valueContainer{slice: []time.Time{time.Date(2019, 12, 31, 0, 0, 0, 0, tz)},
				isNull: []bool{false}, id: mockID, name: "foo"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				cache:  tt.fields.cache,
				id:     tt.fields.id,
			}
			vc.resample(tt.args.by)
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("vc.resample() -> %v, want %v", vc.slice, tt.want.slice)
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
		{"no change", args{d, Resampler{}}, time.Date(2020, 2, 2, 12, 30, 45, 100, time.UTC)},
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
		vals   interface{}
		isNull []bool
		index  []int
	}
	tests := []struct {
		name  string
		args  args
		want  int
		want1 bool
	}{
		{"pass", args{[]string{"foo", "foo", "bar", ""}, []bool{false, false, false, true}, []int{0, 1, 2, 3}}, 2, false},
		{"fail", args{[]string{"", "", "", ""}, []bool{true, true, true, true}, []int{0, 1, 2, 3}}, 0, true},
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
		name         string
		sourceLabels []*valueContainer
		leftOn       []int
		lookupValues *valueContainer
		lookupLabels []*valueContainer
		rightOn      []int
	}
	tests := []struct {
		name string
		args args
		want *Series
	}{
		{"pass", args{name: "waldo", sourceLabels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			leftOn:       []int{0},
			lookupValues: &valueContainer{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, id: mockID, name: "qux"},
			lookupLabels: []*valueContainer{{slice: []float64{1, 0, 2}, isNull: []bool{false, false, false}, id: mockID, name: "foo"}},
			rightOn:      []int{0}},
			&Series{
				values: &valueContainer{slice: []string{"", "foo"}, isNull: []bool{true, false}, id: mockID, name: "waldo"},
				labels: []*valueContainer{
					{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo",
						cache: []string{"0", "1"},
					}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lookupWithAnchor(
				tt.args.name, tt.args.sourceLabels, tt.args.leftOn, tt.args.lookupValues, tt.args.lookupLabels, tt.args.rightOn); !EqualSeries(got, tt.want) {
				t.Errorf("lookupWithAnchor() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_lookupDataFrameWithAnchor(t *testing.T) {
	type args struct {
		name             string
		colLevelNames    []string
		originalLabels   []*valueContainer
		sourceContainers []*valueContainer
		leftOn           []int
		lookupContainers []*valueContainer
		rightOn          []int
		lookupColumns    []*valueContainer
		exclude          []string
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"no exclusion", args{name: "waldo", colLevelNames: []string{"*0"},
			originalLabels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			sourceContainers: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			},
			leftOn: []int{0},
			lookupContainers: []*valueContainer{
				{slice: []float64{1, 0, 2}, isNull: []bool{false, false, false}, id: mockID, name: "c"}},
			rightOn: []int{0},
			lookupColumns: []*valueContainer{
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, id: mockID, name: "a"},
				{slice: []string{"bar", "qux", "baz"}, isNull: []bool{false, false, false}, id: mockID, name: "b"}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"", "foo"}, isNull: []bool{true, false}, id: mockID, name: "a"},
					{slice: []string{"qux", "bar"}, isNull: []bool{false, false}, id: mockID, name: "b"},
				},
				labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				name:   "waldo", colLevelNames: []string{"*0"},
			}},
		{"exclusion", args{name: "waldo", colLevelNames: []string{"*0"},
			originalLabels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			sourceContainers: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
			},
			leftOn: []int{0},
			lookupContainers: []*valueContainer{
				{slice: []float64{1, 0, 2}, isNull: []bool{false, true, false}, id: mockID, name: "a"}},
			rightOn: []int{0},
			lookupColumns: []*valueContainer{
				{slice: []float64{1, 0, 2}, isNull: []bool{false, true, false}, id: mockID, name: "a"},
				{slice: []string{"foo", "", "baz"}, isNull: []bool{false, true, false}, id: mockID, name: "b"}},
			exclude: []string{"a"}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"", "foo"}, isNull: []bool{true, false}, id: mockID, name: "b"},
				},
				labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				name:   "waldo", colLevelNames: []string{"*0"},
			}},
		{"equal labels", args{name: "waldo", colLevelNames: []string{"*0"},
			originalLabels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			sourceContainers: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"},
			},
			leftOn: []int{0},
			lookupContainers: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
			rightOn: []int{0},
			lookupColumns: []*valueContainer{
				{slice: []string{"foo", ""}, isNull: []bool{false, true}, id: mockID, name: "a"},
				{slice: []string{"bar", "qux"}, isNull: []bool{false, false}, id: mockID, name: "b"}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", ""}, isNull: []bool{false, true}, id: mockID, name: "a"},
					{slice: []string{"bar", "qux"}, isNull: []bool{false, false}, id: mockID, name: "b"},
				},
				labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				name:   "waldo", colLevelNames: []string{"*0"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := lookupDataFrameWithAnchor(
				tt.args.name, tt.args.colLevelNames, tt.args.originalLabels,
				tt.args.sourceContainers, tt.args.leftOn,
				tt.args.lookupContainers, tt.args.rightOn,
				tt.args.lookupColumns, tt.args.exclude); !EqualDataFrames(got, tt.want) {
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
			{slice: []float64{1, 1, 2, 1}, isNull: []bool{false, false, false, false}, id: mockID, name: "foo"},
			{slice: []int{0, 0, 2, 3}, isNull: []bool{false, false, false, false}, id: mockID, name: "qux"},
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
		id     string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"pass", fields{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}, "[]float64"},
		{"pass", fields{slice: []string{"1"}, isNull: []bool{false}, id: mockID, name: "foo"}, "[]string"},
		{"pass", fields{slice: [][]string{{"1"}}, isNull: []bool{false}, id: mockID, name: "foo"}, "[][]string"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			if got := vc.dtype(); got.String() != tt.want {
				t.Errorf("valueContainer.dtype() = %v, want %v", got.String(), tt.want)
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
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
		}, true},
		{"pass - both nil", args{
			a: nil,
			b: nil,
		}, true},
		{"fail - nil", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
			b: nil,
		}, false},
		{"fail - values", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
		}, false},
		{"fail - labels", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
		}, false},
		{"fail - shared data", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				sharedData: true,
				err:        errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				sharedData: false,
				err:        errors.New("foo")},
		}, false},
		{"fail - has err", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: nil},
		}, false},
		{"fail - err value", args{
			a: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				err: errors.New("foo")},
			b: &Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
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
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
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
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: nil,
		}, false},
		{"fail - values", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
		}, false},
		{"fail - labels", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
		}, false},
		{"fail - colLevel names", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*1"},
				name:          "baz",
				err:           errors.New("foo")},
		}, false},
		{"fail - names", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "corge",
				err:           errors.New("foo")},
		}, false},
		{"fail - has err", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           nil},
		}, false},
		{"fail - err value", args{
			a: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "baz",
				err:           errors.New("foo")},
			b: &DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
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
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
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
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: nil,
		}, false},
		{"fail - orderedKeys", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"bar"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - rowIndices", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{1}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - labels", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "baz"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - nil series", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				err:         errors.New("foo")}},
			false},
		{"fail - series", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "corge"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - has err", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: nil}},
			false},
		{"fail - err value", args{
			a: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedSeries{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				series: &Series{
					values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
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
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
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
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: nil,
		}, false},
		{"fail - orderedKeys", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"bar"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - rowIndices", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{1}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - labels", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "baz"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - has df", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				err:         errors.New("foo")}},
			false},
		{"fail - df", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "corge"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")}},
			false},
		{"fail - has err", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: nil}},
			false},
		{"fail - err value", args{
			a: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				},
				err: errors.New("foo")},
			b: &GroupedDataFrame{
				orderedKeys: []string{"foo"},
				rowIndices:  [][]int{{0}},
				labels:      []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
				df: &DataFrame{
					values: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, id: mockID, name: "qux"}},
					labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
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

func Test_isNullString(t *testing.T) {
	type args struct {
		s string
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"is null", args{""}, true},
		{"is null", args{"(null)"}, true},
		{"not null", args{"foo"}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := isNullString(tt.args.s); got != tt.want {
				t.Errorf("isNullString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_extractCSVDimensions(t *testing.T) {
	type args struct {
		b     []byte
		comma rune
	}
	tests := []struct {
		name        string
		args        args
		wantNumRows int
		wantNumCols int
		wantErr     bool
	}{
		{"pass", args{[]byte("foo, bar, 1\n baz, qux, 2\n"), ','}, 2, 3, false},
		{"custom delimiter", args{[]byte("foo| bar| 1\n baz| qux| 2\n"), '|'}, 2, 3, false},
		{"no final \n", args{[]byte("foo| bar| 1\n baz| qux| 2"), '|'}, 2, 3, false},
		{"subtract empty row", args{[]byte("foo| bar| 1\n\n baz| qux| 2"), '|'}, 2, 3, false},
		{"bad delimiter", args{[]byte("foo| bar| 1\n baz| qux| 2\n"), '"'}, 0, 0, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotNumRows, gotNumCols, err := extractCSVDimensions(tt.args.b, tt.args.comma)
			if (err != nil) != tt.wantErr {
				t.Errorf("extractCSVDimensions() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotNumRows != tt.wantNumRows {
				t.Errorf("extractCSVDimensions() gotNumRows = %v, want %v", gotNumRows, tt.wantNumRows)
			}
			if gotNumCols != tt.wantNumCols {
				t.Errorf("extractCSVDimensions() gotNumCols = %v, want %v", gotNumCols, tt.wantNumCols)
			}
		})
	}
}

func Test_readCSVBytes(t *testing.T) {
	b0 := "\"foo\",\"bar\"\n\"qux\",\"quz\"\n"
	b1 := "foo,bar,baz\nqux,quux,quz\n"
	b2 := "foo, bar\nqux, quz\n"
	b3 := ",foo\n"
	b4 := "foo,bar\nqux,quz"
	b5 := "foo\nbaz\r\n"
	b6 := "foo\nbaz\r"
	b7 := "foo, bar\n qux, \n quux, quz\n"

	f0 := "foo\nbar,baz\n"
	f1 := "foo,bar\nbaz\n"
	type args struct {
		r        io.Reader
		dstVals  [][]string
		dstNulls [][]bool
		comma    rune
	}
	tests := []struct {
		name      string
		args      args
		wantVals  [][]string
		wantNulls [][]bool
		wantErr   bool
	}{
		{name: "pass with quotes",
			args: args{
				r: bytes.NewBuffer([]byte(b0)),
				dstVals: [][]string{
					{"", ""},
					{"", ""}},
				dstNulls: [][]bool{{false, false}, {false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "qux"},
				{"bar", "quz"}},
			wantNulls: [][]bool{{false, false}, {false, false}},
			wantErr:   false,
		},
		{name: "pass normal",
			args: args{
				r: bytes.NewBuffer([]byte(b1)),
				dstVals: [][]string{
					{"", ""},
					{"", ""},
					{"", ""}},
				dstNulls: [][]bool{{false, false}, {false, false}, {false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "qux"},
				{"bar", "quux"},
				{"baz", "quz"}},
			wantNulls: [][]bool{{false, false}, {false, false}, {false, false}},
			wantErr:   false,
		},
		{name: "pass with leading whitespace",
			args: args{
				r: bytes.NewBuffer([]byte(b2)),
				dstVals: [][]string{
					{"", ""},
					{"", ""}},
				dstNulls: [][]bool{{false, false}, {false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "qux"},
				{"bar", "quz"}},
			wantNulls: [][]bool{{false, false}, {false, false}},
			wantErr:   false,
		},
		{name: "pass with nil",
			args: args{
				r: bytes.NewBuffer([]byte(b3)),
				dstVals: [][]string{
					{""},
					{""}},
				dstNulls: [][]bool{{true}, {false}},
				comma:    ','},
			wantVals: [][]string{
				{""},
				{"foo"}},
			wantNulls: [][]bool{{true}, {false}},
			wantErr:   false,
		},
		{name: "pass with no final \n",
			args: args{
				r: bytes.NewBuffer([]byte(b4)),
				dstVals: [][]string{
					{"", ""},
					{"", ""}},
				dstNulls: [][]bool{{false, false}, {false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "qux"},
				{"bar", "quz"}},
			wantNulls: [][]bool{{false, false}, {false, false}},
			wantErr:   false,
		},
		{name: "pass with \r\n",
			args: args{
				r: bytes.NewBuffer([]byte(b5)),
				dstVals: [][]string{
					{"", ""}},
				dstNulls: [][]bool{{false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "baz"}},
			wantNulls: [][]bool{{false, false}},
			wantErr:   false,
		},
		{name: "pass with final \r",
			args: args{
				r: bytes.NewBuffer([]byte(b6)),
				dstVals: [][]string{
					{"", ""}},
				dstNulls: [][]bool{{false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "baz"}},
			wantNulls: [][]bool{{false, false}},
			wantErr:   false,
		},
		{name: "pass with missing value",
			args: args{
				r: bytes.NewBuffer([]byte(b7)),
				dstVals: [][]string{
					{"", "", ""},
					{"", "", ""},
				},
				dstNulls: [][]bool{
					{false, false, false},
					{false, false, false},
				},
				comma: ','},
			wantVals: [][]string{
				{"foo", "qux", "quux"},
				{"bar", "", "quz"},
			},
			wantNulls: [][]bool{
				{false, false, false},
				{false, true, false},
			},
			wantErr: false,
		},
		{name: "fail - too many fields",
			args: args{
				r: bytes.NewBuffer([]byte(f0)),
				dstVals: [][]string{
					{"", ""}},
				dstNulls: [][]bool{{false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "bar"}},
			wantNulls: [][]bool{{false, false}},
			wantErr:   true,
		},
		{name: "fail - too few fields",
			args: args{
				r: bytes.NewBuffer([]byte(f1)),
				dstVals: [][]string{
					{"", ""},
					{"", ""}},
				dstNulls: [][]bool{{false, false}, {false, false}},
				comma:    ','},
			wantVals: [][]string{
				{"foo", "baz"},
				{"bar", ""}},
			wantNulls: [][]bool{{false, false}, {false, false}},
			wantErr:   true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := readCSVBytes(tt.args.r, tt.args.dstVals, tt.args.dstNulls, tt.args.comma)
			if (err != nil) != tt.wantErr {
				t.Errorf("readCSVBytes() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.args.dstVals, tt.wantVals) {
				t.Errorf("readCSVBytes() -> dstVals = %#v, wantVals %#v", tt.args.dstVals, tt.wantVals)
				for i := range tt.args.dstVals {
					for j := range tt.args.dstVals[i] {
						fmt.Println(string(tt.args.dstVals[i][j]))
					}
				}
			}
			if !reflect.DeepEqual(tt.args.dstNulls, tt.wantNulls) {
				t.Errorf("readCSVBytes() -> dstNulls = %v, wantNulls %v", tt.args.dstNulls, tt.wantNulls)
			}
		})
	}
}

func Test_makeDataFrameFromMatrices(t *testing.T) {
	type args struct {
		values [][]string
		isNull [][]bool
		config *readConfig
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"pass - 1 header col", args{
			values: [][]string{
				{"foo", "bar"},
				{"baz", ""}},
			isNull: [][]bool{{false, false}, {false, true}},
			config: &readConfig{numHeaderRows: 1}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"bar"}, isNull: []bool{false}, id: mockID, name: "foo"},
					{slice: []string{""}, isNull: []bool{true}, id: mockID, name: "baz"},
				},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"},
				},
				colLevelNames: []string{"*0"},
			}},
		{"pass - 1 header col, 1 label level", args{
			values: [][]string{
				{"foo", "bar"},
				{"baz", ""}},
			isNull: [][]bool{{false, false}, {false, true}},
			config: &readConfig{numHeaderRows: 1, numLabelLevels: 1}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{""}, isNull: []bool{true}, id: mockID, name: "baz"},
				},
				labels: []*valueContainer{
					{slice: []string{"bar"}, isNull: []bool{false}, id: mockID, name: "foo"},
				},
				colLevelNames: []string{"*0"},
			}},
		{"pass - 1 label level", args{
			values: [][]string{
				{"foo", "bar"},
				{"baz", ""}},
			isNull: [][]bool{{false, false}, {false, true}},
			config: &readConfig{numLabelLevels: 1}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"baz", ""}, isNull: []bool{false, true}, id: mockID, name: "0"},
				},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "*0"},
				},
				colLevelNames: []string{"*0"},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeDataFrameFromMatrices(tt.args.values, tt.args.isNull, tt.args.config); !EqualDataFrames(got, tt.want) {
				t.Errorf("makeDataFrameFromMatrices() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_combineMath(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		other      *Series
		ignoreNull bool
		fn         func(v1 float64, v2 float64) float64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"same index", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			}},
			args{
				other: &Series{
					values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
						{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}}},
				ignoreNull: false,
				fn:         func(v1, v2 float64) float64 { return v1 + v2 }},
			&Series{
				values: &valueContainer{slice: []float64{3}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}},
			},
		},
		{"different index", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			}},
			args{
				other: &Series{
					values: &valueContainer{slice: []float64{2, 20}, isNull: []bool{false, false}, id: mockID, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "bar"}}},
				ignoreNull: false,
				fn:         func(v1, v2 float64) float64 { return v1 + v2 }},
			&Series{
				values: &valueContainer{slice: []float64{3}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar", cache: []string{"0"}},
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}},
			},
		},
		{"divide by zero", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			}},
			args{
				other: &Series{
					values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, id: mockID, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
						{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}}},
				ignoreNull: false,
				fn:         func(v1, v2 float64) float64 { return v1 / v2 }},
			&Series{
				values: &valueContainer{slice: []float64{0}, isNull: []bool{true}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}},
			},
		},
		{"left nulls - do not ignore null", fields{
			values: &valueContainer{slice: []float64{0}, isNull: []bool{true}, id: mockID, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			}},
			args{
				other: &Series{
					values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
						{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}}},
				ignoreNull: false,
				fn:         func(v1, v2 float64) float64 { return v1 + v2 }},
			&Series{
				values: &valueContainer{slice: []float64{0}, isNull: []bool{true}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}},
			},
		},
		{"left nulls - ignore null", fields{
			values: &valueContainer{slice: []float64{0}, isNull: []bool{true}, id: mockID, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			}},
			args{
				other: &Series{
					values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
						{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}}},
				ignoreNull: true,
				fn:         func(v1, v2 float64) float64 { return v1 + v2 }},
			&Series{
				values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}},
			},
		},
		{"right nulls - ignore null", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
				{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			}},
			args{
				other: &Series{
					values: &valueContainer{slice: []float64{0}, isNull: []bool{true}, id: mockID, name: "foo"},
					labels: []*valueContainer{
						{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
						{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}}},
				ignoreNull: true,
				fn:         func(v1, v2 float64) float64 { return v1 + v2 }},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "bar"},
					{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.combineMath(tt.args.other, tt.args.ignoreNull, tt.args.fn); !EqualSeries(got, tt.want) {
				t.Errorf("Series.combineMath() = %v, want %v", got.labels[1], tt.want.labels[1])

			}
		})
	}
}

func Test_valueContainer_valid(t *testing.T) {
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
		{"pass", fields{slice: []float64{1, 0, 2}, isNull: []bool{false, true, false}}, []int{0, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.valid(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_null(t *testing.T) {
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
		{"pass", fields{slice: []float64{1, 0, 2}, isNull: []bool{false, true, false}}, []int{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
			}
			if got := vc.null(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.null() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_subsetRows(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
		id     string
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"pass", fields{slice: []float64{0, 1, 2}, isNull: []bool{false, true, false}, id: mockID, name: "foo"},
			args{[]int{1}},
			&valueContainer{slice: []float64{1}, isNull: []bool{true}, id: mockID, name: "foo"}, false},
		{"pass - reset cache", fields{slice: []string{"foo", "bar"}, isNull: []bool{false, false},
			cache: []string{"foo", "bar"}, id: mockID, name: "foo"},
			args{[]int{1}},
			&valueContainer{slice: []string{"bar"}, isNull: []bool{false}, id: mockID, name: "foo"}, false},
		{"return existing", fields{slice: []float64{0, 1, 2}, isNull: []bool{false, true, false}, id: mockID, name: "foo"},
			args{[]int{0, 1, 2}},
			&valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, true, false}, id: mockID, name: "foo"}, false},
		{"fail", fields{slice: []float64{0, 1, 2}, isNull: []bool{false, true, false}, id: mockID, name: "foo"},
			args{[]int{10}},
			&valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, true, false}, id: mockID, name: "foo"}, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			err := vc.subsetRows(tt.args.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.subsetRows() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("valueContainer.subsetRows() -> = %v, want %v", vc, tt.want)

			}
		})
	}
}

func Test_valueContainer_filter(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		slice  interface{}
		isNull []bool
		name   string
		id     string
	}
	type args struct {
		filter FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"Float64", fields{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
			args{func(val interface{}) bool { return val.(float64) > 1 }},
			[]int{2}},
		{"DateTime", fields{slice: []time.Time{d.AddDate(0, 0, -1), d, d.AddDate(0, 0, 1)}, isNull: []bool{false, false, false}, id: mockID, name: "foo"},
			args{func(val interface{}) bool { return val.(time.Time).Before(d) }},
			[]int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			got := vc.filter(tt.args.filter)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_makeByteMatrix(t *testing.T) {
	type args struct {
		numCols int
		numRows int
	}
	tests := []struct {
		name string
		args args
		want [][][]byte
	}{
		{"pass", args{2, 1}, [][][]byte{{nil}, {nil}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := makeByteMatrix(tt.args.numCols, tt.args.numRows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("makeByteMatrix() = %#v, want %#v", got, tt.want)
			}
		})
	}
}

func Test_concatenateLabelsToStringsBytes(t *testing.T) {
	type args struct {
		labels []*valueContainer
	}
	tests := []struct {
		name string
		args args
		want []string
	}{
		{"one level", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}}}},
			[]string{"foo", "bar"}},
		{"two levels, two index", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}},
			{slice: []int{0, 1}}}},
			[]string{"foo|0", "bar|1"}},
		{"two levels, two index, cache", args{labels: []*valueContainer{
			{slice: []string{"foo", "bar"}},
			{slice: []int{0, 1}, cache: []string{"0", "1"}}}},
			[]string{"foo|0", "bar|1"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := concatenateLabelsToStringsBytes(tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("concatenateLabelsToStringsBytes() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_filter(t *testing.T) {
	type args struct {
		containers []*valueContainer
		filters    map[string]FilterFn
	}
	tests := []struct {
		name    string
		args    args
		want    []int
		wantErr bool
	}{
		{"pass", args{
			[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			map[string]FilterFn{
				"qux": func(val interface{}) bool { return val.(int) >= 1 },
				"bar": func(val interface{}) bool { return val.(string) == "foo" },
			}},
			[]int{1},
			false},
		{"fail - bad container name", args{
			[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			map[string]FilterFn{
				"corge": func(val interface{}) bool { return val.(int) >= 1 },
			}},
			nil,
			true},
		{"fail - no function", args{
			[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			map[string]FilterFn{
				"qux": nil,
			}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := filter(tt.args.containers, tt.args.filters)
			if (err != nil) != tt.wantErr {
				t.Errorf("filter() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_floatValueContainer_percentile(t *testing.T) {
	type fields struct {
		slice  []float64
		isNull []bool
		index  []int
	}
	tests := []struct {
		name   string
		fields fields
		want   []float64
	}{
		{"pass",
			fields{[]float64{1, 2, 3, 4, 5, 6, 7, 8, 10, 9}, []bool{false, false, false, false, false, false, false, false, false, false},
				[]int{0, 1, 2, 3, 4, 5, 6, 7, 8, 9}},
			[]float64{0, .1, .2, .3, .4, .5, .6, .7, .9, .8}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &floatValueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				index:  tt.fields.index,
			}
			if got := vc.percentile(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("floatValueContainer.percentile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_subsetContainerRows(t *testing.T) {
	type args struct {
		containers []*valueContainer
		index      []int
	}
	tests := []struct {
		name    string
		args    args
		want    []*valueContainer
		wantErr bool
	}{
		{"pass",
			args{
				containers: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				index: []int{0}},
			[]*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "qux"},
				{slice: []string{"foo"}, isNull: []bool{false}, id: mockID, name: "bar"}},
			false},
		{"fail - out of range",
			args{
				containers: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				index: []int{2}},
			[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := subsetContainerRows(tt.args.containers, tt.args.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("subsetContainerRows() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.args.containers, tt.want) {
				t.Errorf("subsetContainerRows() -> %v, want %v", tt.args.containers, tt.want)
			}
		})
	}
}

func Test_subsetContainers(t *testing.T) {
	type args struct {
		containers []*valueContainer
		index      []int
	}
	tests := []struct {
		name    string
		args    args
		want    []*valueContainer
		wantErr bool
	}{
		{"pass",
			args{
				containers: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				index: []int{0}},
			[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"}},
			false},
		{"fail - out of range",
			args{
				containers: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				index: []int{2}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := subsetContainers(tt.args.containers, tt.args.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("subsetContainers() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("subsetContainers() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_subsetInterfaceSlice(t *testing.T) {
	d := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type args struct {
		slice interface{}
		index []int
	}
	tests := []struct {
		name string
		args args
		want interface{}
	}{
		{"float64", args{[]float64{1, 2}, []int{0}}, []float64{1}},
		{"string", args{[]string{"foo", "bar"}, []int{0}}, []string{"foo"}},
		{"time", args{[]time.Time{d, d.AddDate(0, 0, 1)}, []int{0}}, []time.Time{d}},
		{"int", args{[]int{1, 2}, []int{0}}, []int{1}},
		{"other", args{[]uint{1, 2}, []int{0}}, []uint{1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := subsetInterfaceSlice(tt.args.slice, tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("subsetInterfaceSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_seriesIsDistinct(t *testing.T) {
	vc := &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}
	vcs := []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}}
	type args struct {
		a *Series
		b *Series
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"distinct", args{
			&Series{
				labels: []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			},
			&Series{
				labels: vcs,
				values: vc,
			},
		}, true},
		{"not distinct - label containers", args{
			&Series{
				labels: vcs,
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			},
			&Series{
				labels: vcs,
				values: vc,
			},
		}, false},
		{"not distinct - label", args{
			&Series{
				labels: []*valueContainer{vc},
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			},
			&Series{
				labels: []*valueContainer{vc},
				values: vc,
			},
		}, false},
		{"not distinct - values", args{
			&Series{
				labels: []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				values: vc,
			},
			&Series{
				labels: vcs,
				values: vc,
			},
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := seriesIsDistinct(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("seriesIsDistinct() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_dataFrameIsDistinct(t *testing.T) {
	vc := &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}
	vcs := []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}}
	colLevelNames := []string{"*0"}
	type args struct {
		a *DataFrame
		b *DataFrame
	}
	tests := []struct {
		name string
		args args
		want bool
	}{
		{"distinct", args{
			&DataFrame{
				values:        []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				labels:        []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				name:          "foo",
				colLevelNames: []string{"*0"},
			},
			&DataFrame{
				values:        []*valueContainer{vc},
				labels:        []*valueContainer{vc},
				name:          "foo",
				colLevelNames: colLevelNames}},
			true,
		},
		{"not distinct - value containers", args{
			&DataFrame{
				values: vcs,
				labels: []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				name:   "foo", colLevelNames: []string{"*0"}},
			&DataFrame{
				values:        vcs,
				labels:        []*valueContainer{vc},
				name:          "foo",
				colLevelNames: colLevelNames}},
			false,
		},
		{"not distinct - values", args{
			&DataFrame{
				values: []*valueContainer{vc},
				labels: []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				name:   "foo", colLevelNames: []string{"*0"}},
			&DataFrame{
				values:        []*valueContainer{vc},
				labels:        []*valueContainer{vc},
				name:          "foo",
				colLevelNames: colLevelNames}},
			false,
		},
		{"not distinct - label containers", args{
			&DataFrame{
				values: []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				labels: vcs,
				name:   "foo", colLevelNames: []string{"*0"}},
			&DataFrame{
				values:        []*valueContainer{vc},
				labels:        vcs,
				name:          "foo",
				colLevelNames: colLevelNames}},
			false,
		},
		{"not distinct - labels", args{
			&DataFrame{
				values: []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				labels: []*valueContainer{vc}, name: "foo", colLevelNames: []string{"*0"}},
			&DataFrame{
				values: []*valueContainer{vc},
				labels: []*valueContainer{vc},
				name:   "foo", colLevelNames: colLevelNames}},
			false,
		},
		{"not distinct - col level names", args{
			&DataFrame{
				values:        []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				labels:        []*valueContainer{{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}},
				name:          "foo",
				colLevelNames: colLevelNames},
			&DataFrame{
				values:        []*valueContainer{vc},
				labels:        []*valueContainer{vc},
				name:          "foo",
				colLevelNames: colLevelNames}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := dataFrameIsDistinct(tt.args.a, tt.args.b); got != tt.want {
				t.Errorf("dataFrameIsDistinct() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_dropRow(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
		id     string
	}
	type args struct {
		index int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"pass - reset cache", fields{slice: []int{0, 1}, isNull: []bool{false, false},
			name: "qux", id: mockID, cache: []string{"0", "1"}},
			args{0},
			&valueContainer{slice: []int{1}, isNull: []bool{false}, id: mockID, name: "qux"},
			false,
		},
		{"fail", fields{slice: []int{0, 1}, isNull: []bool{false, false},
			name: "qux", id: mockID, cache: []string{"foo"}},
			args{10},
			&valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux",
				cache: []string{"foo"}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			if err := vc.dropRow(tt.args.index); (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.dropRow() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("valueContainer.dropRow() -> %v, want %v", vc, tt.want)
			}
		})
	}
}

func Test_valueContainer_indexOfRows(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
	}
	type args struct {
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass",
			// returns position only of non-null values
			fields{slice: []float64{1, 2, 3, 2, 2}, isNull: []bool{false, false, false, true, false}},
			args{2},
			[]int{1, 4},
		},
		{"no matches",
			fields{slice: []float64{1, 2, 3, 2, 2}, isNull: []bool{false, false, false, true, false}},
			args{5},
			[]int{},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
			}
			if got := vc.indexOfRows(tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.indexOfRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_xs(t *testing.T) {
	type args struct {
		containers []*valueContainer
		values     map[string]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []int
		wantErr bool
	}{
		{"pass",
			args{[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				map[string]interface{}{"qux": 1, "foo": "bar"}},
			[]int{1}, false,
		},
		{"pass - no matches",
			args{[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				map[string]interface{}{"qux": 0, "foo": "bar"}},
			[]int{}, false,
		},
		{"fail",
			args{[]*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				map[string]interface{}{"corge": 1, "foo": "bar"}},
			nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := filterByValue(tt.args.containers, tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("xs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("xs() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_nameOfContainer(t *testing.T) {
	type args struct {
		containers []*valueContainer
		n          int
	}
	tests := []struct {
		name string
		args args
		want string
	}{
		{"pass",
			args{
				[]*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				0},
			"qux"},
		{"fail",
			args{
				[]*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "qux"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
				10},
			"index out of range [10] with length 2"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := nameOfContainer(tt.args.containers, tt.args.n); got != tt.want {
				t.Errorf("nameOfContainer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_setReadConfig(t *testing.T) {
	type args struct {
		options []ReadOption
	}
	tests := []struct {
		name string
		args args
		want *readConfig
	}{
		{"default", args{nil}, &readConfig{
			numHeaderRows:  1,
			numLabelLevels: 0,
			delimiter:      ',',
			majorDimIsCols: false,
		}},
		{"pass", args{[]ReadOption{
			ReadOptionHeaders(2),
			ReadOptionLabels(2),
			ReadOptionDelimiter('|'),
			ReadOptionSwitchDims(),
		}}, &readConfig{
			numHeaderRows:  2,
			numLabelLevels: 2,
			delimiter:      '|',
			majorDimIsCols: true,
		}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := setReadConfig(tt.args.options); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("setReadConfig() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_expand(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
	}
	type args struct {
		n []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *valueContainer
	}{
		{"pass", fields{
			slice:  []string{"foo", "", "bar"},
			isNull: []bool{false, true, false},
			name:   "foobar"},
			args{[]int{1, 2, 1}},
			&valueContainer{
				slice:  []string{"foo", "", "", "bar"},
				isNull: []bool{false, true, true, false},
				name:   "foobar",
			},
		},
		{"pass - repeat then 0", fields{
			slice:  []string{"foo", "", "bar", "baz"},
			isNull: []bool{false, true, false, false},
			name:   "foobar"},
			args{[]int{1, 2, 0, 2}},
			&valueContainer{
				slice:  []string{"foo", "", "", "baz", "baz"},
				isNull: []bool{false, true, true, false, false},
				name:   "foobar",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
			}
			if got := vc.expand(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.expand() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_groupCounts(t *testing.T) {
	type args struct {
		rowIndices [][]int
	}
	tests := []struct {
		name string
		args args
		want []int
	}{
		{"pass", args{[][]int{{0, 1}, {}, {1}}},
			[]int{2, 0, 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := groupCounts(tt.args.rowIndices); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("groupCounts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_rowCount(t *testing.T) {
	type args struct {
		rowIndices [][]int
	}
	tests := []struct {
		name string
		args args
		want int
	}{
		{"pass", args{
			[][]int{{0, 1}, {}, {1, 2, 3}}},
			5,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := rowCount(tt.args.rowIndices); got != tt.want {
				t.Errorf("rowCount() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_set(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
	}
	type args struct {
		newSlice interface{}
		newNulls []bool
		index    []int
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"pass", fields{
			slice:  []string{"foo", "", "bar"},
			isNull: []bool{false, true, false},
			name:   "foobar"},
			args{[]string{"baz"}, []bool{false}, []int{1}},

			&valueContainer{
				slice:  []string{"foo", "baz", "bar"},
				isNull: []bool{false, false, false},
				name:   "foobar",
			},
			false,
		},
		{"fail - different type", fields{
			slice:  []string{"foo", "", "bar"},
			isNull: []bool{false, true, false},
			name:   "foobar"},
			args{[]int{1}, []bool{false}, []int{1}},

			&valueContainer{
				slice:  []string{"foo", "", "bar"},
				isNull: []bool{false, true, false},
				name:   "foobar",
			},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
			}
			err := vc.set(tt.args.newSlice, tt.args.newNulls, tt.args.index)
			if (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.set() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("valueContainer.set() -> %v, want %v", vc, tt.want)

			}
		})
	}
}

func Test_valueContainer_interfaceSlice(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
	}
	type args struct {
		includeHeader bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []interface{}
	}{
		{"pass - exclude header", fields{
			slice:  []int{1, 0, 2},
			isNull: []bool{false, true, false},
			name:   "foobar"},
			args{false},
			[]interface{}{1, "(null)", 2},
		},
		{"pass - include header", fields{
			slice:  []int{1, 0, 2},
			isNull: []bool{false, true, false},
			name:   "foobar"},
			args{true},
			[]interface{}{"foobar", 1, "(null)", 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := &valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
			}
			if got := vc.interfaceSlice(tt.args.includeHeader); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.interfaceSlice() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_unpackIDsByPosition(t *testing.T) {
	var foo string
	var bar string
	type args struct {
		containers []*valueContainer
		receivers  []*string
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"pass", args{
			[]*valueContainer{
				{id: "1"}, {id: "2"},
			},
			[]*string{&foo, &bar},
		},
			[]string{"1", "2"},
			false,
		},
		{"pass - fewer receivers than columns", args{
			[]*valueContainer{
				{id: "1"}, {id: "2"},
			},
			[]*string{&foo},
		},
			[]string{"1"},
			false,
		},
		{"fail - more receivers than columns", args{
			[]*valueContainer{
				{id: "1"}, {id: "2"},
			},
			[]*string{&foo, &bar, &bar},
		},
			[]string{"", ""},
			true,
		},
	}
	for _, tt := range tests {
		foo = ""
		bar = ""
		t.Run(tt.name, func(t *testing.T) {
			err := unpackIDsByPosition(tt.args.containers, tt.args.receivers...)
			if (err != nil) != tt.wantErr {
				t.Errorf("unpackIDsByPosition() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				for i := range tt.args.receivers {
					if *tt.args.receivers[i] != tt.want[i] {
						t.Errorf("unpackIDsByPosition() -> [%d] = %v, want %v", i, *tt.args.receivers[i], tt.want[i])
					}
				}
			}
		})
	}
}

func Test_unpackIDsByName(t *testing.T) {
	var foo string
	var bar string
	type args struct {
		containers []*valueContainer
		receivers  map[string]*string
	}
	tests := []struct {
		name    string
		args    args
		want    map[string]string
		wantErr bool
	}{
		{"pass", args{
			[]*valueContainer{
				{name: "foo", id: "1"}, {name: "bar", id: "2"},
			},
			map[string]*string{"foo": &foo, "bar": &bar},
		},
			map[string]string{"foo": "1", "bar": "2"},
			false,
		},
		{"pass - fewer receivers than containers", args{
			[]*valueContainer{
				{name: "foo", id: "1"},
			},
			map[string]*string{"foo": &foo},
		},
			map[string]string{"foo": "1"},
			false,
		},
		{"fail - invalid container name", args{
			[]*valueContainer{
				{name: "foo", id: "1"},
			},
			map[string]*string{"corge": &foo},
		},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			foo = ""
			bar = ""
			err := unpackIDsByName(tt.args.containers, tt.args.receivers)
			if (err != nil) != tt.wantErr {
				t.Errorf("unpackIDsByName() error = %v, wantErr %v", err, tt.wantErr)
			}
			if err == nil {
				for k := range tt.args.receivers {
					if *tt.args.receivers[k] != tt.want[k] {
						t.Errorf("unpackIDsByName() -> [%s] = %v, want %v", k, *tt.args.receivers[k], tt.want[k])
					}
				}
			}
		})
	}
}

func TestRealClock(t *testing.T) {
	c := realClock{}
	if !c.now().After(time.Time{}) {
		t.Errorf("expected realClock to return a non-zero time")
	}
}
