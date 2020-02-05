package tada

import (
	"math"
	"reflect"
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
			how: "left", values1: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels1: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}, leftOn: []int{0},
			values2: &valueContainer{slice: []int{10, 20}, isNull: []bool{false, false}},
			labels2: []*valueContainer{{slice: []int{0, 10}, isNull: []bool{false, false}}}, rightOn: []int{0}},
			want: &Series{
				values: &valueContainer{slice: []int{10, 0}, isNull: []bool{false, true}},
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
	}{
		{"normal", args{[]*valueContainer{{slice: []float64{1}}, {slice: []string{"foo"}}}, []int{0, 1}},
			map[string][]int{"1|foo": []int{0}}, map[string]int{"1|foo": 0}},
		{"reversed", args{[]*valueContainer{{slice: []float64{1}}, {slice: []string{"foo"}}}, []int{1, 0}},
			map[string][]int{"foo|1": []int{0}}, map[string]int{"foo|1": 0}},
		{"skip", args{[]*valueContainer{{slice: []float64{1}}, {slice: []string{"foo"}}, {slice: []bool{true}}}, []int{2, 0}},
			map[string][]int{"true|1": []int{0}}, map[string]int{"true|1": 0}},
		{"multiple", args{[]*valueContainer{{slice: []float64{1, 1}}, {slice: []string{"foo", "foo"}}}, []int{0, 1}},
			map[string][]int{"1|foo": []int{0, 1}}, map[string]int{"1|foo": 0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, got1 := labelsToMap(tt.args.labels, tt.args.index)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("labelsToMap() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("labelsToMap() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}
