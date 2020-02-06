package tada

import (
	"reflect"
	"testing"
)

func TestGroupedSeries_Sum(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
				orderedKeys: []string{"foo", "bar"},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}}}}},
			want: &Series{values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				err:         tt.fields.err,
			}
			if got := g.Sum(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Sum() = %v, want %v", got.labels, tt.want.labels)
			}
		})
	}
}
