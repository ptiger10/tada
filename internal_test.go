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
