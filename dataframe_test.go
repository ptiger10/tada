package tada

import (
	"reflect"
	"testing"
)

func TestNewDataFrame(t *testing.T) {
	type args struct {
		slices []interface{}
		labels []interface{}
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"normal", args{
			[]interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
			[]interface{}{[]int{0, 1}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewDataFrame(tt.args.slices, tt.args.labels...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewDataFrame() = %v, want %v", got, tt.want)
			}
		})
	}
}

// for DF to series conversion
// {"unsupported dataframe with multiple columns", args{
// 	slice: DataFrame{
// 		values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}}, {slice: []float64{2}, isNull: []bool{false}}},
// 		labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}},
// 	&Series{err: errors.New("unsupported input type (DataFrame with multiple columns); must be slice or DataFrame with single column")}},
// {"dataframe with single column", args{
// 	slice: DataFrame{
// 		values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}}},
// 		labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}},
// },
// 	&Series{values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
// 		labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}},
