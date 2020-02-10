package tada

import (
	"math"
	"reflect"
	"strconv"
	"testing"
	"time"
)

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
		wantLabels []int
		wantIsNull []bool
	}{
		{"normal", args{0, 2}, []int{0, 1}, []bool{false, false}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotLabels, gotIsNull := makeDefaultLabels(tt.args.min, tt.args.max)
			if !reflect.DeepEqual(gotLabels, tt.wantLabels) {
				t.Errorf("makeDefaultLabels() gotLabels = %v, want %v", gotLabels, tt.wantLabels)
			}
			if !reflect.DeepEqual(gotIsNull, tt.wantIsNull) {
				t.Errorf("makeDefaultLabels() gotIsNull = %v, want %v", gotIsNull, tt.wantIsNull)
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
