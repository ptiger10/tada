package tada

import (
	"errors"
	"reflect"
	"testing"
)

func TestNewSeries(t *testing.T) {
	type args struct {
		slice  interface{}
		labels []interface{}
	}
	tests := []struct {
		name string
		args args
		want *Series
	}{
		{"[]float64, default labels", args{slice: []float64{1}, labels: nil},
			&Series{values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}},
		{"[]string, supplied labels", args{slice: []string{"foo"}, labels: []interface{}{[]string{"bar"}}},
			&Series{values: &valueContainer{slice: []string{"foo"}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"bar"}, isNull: []bool{false}}}}},
		{"dataframe with single column", args{
			slice: DataFrame{
				values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}},
		},
			&Series{values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}},
		{"unsupported string scalar", args{slice: "foo"},
			&Series{err: errors.New("unsupported input type (string); must be slice or DataFrame with single column")}},
		{"unsupported dataframe with multiple columns", args{
			slice: DataFrame{
				values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}}, {slice: []float64{2}, isNull: []bool{false}}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}},
			&Series{err: errors.New("unsupported input type (DataFrame with multiple columns); must be slice or DataFrame with single column")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSeries(tt.args.slice, tt.args.labels...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}
