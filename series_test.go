package tada

import (
	"errors"
	"reflect"
	"strings"
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
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}}}},
		{"[]float64, supplied labels", args{slice: []float64{1}, labels: []interface{}{[]string{"bar"}}},
			&Series{values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"bar"}, isNull: []bool{false}, name: "*0"}}}},
		{"unsupported input: string scalar", args{slice: "foo"},
			&Series{err: errors.New("NewSeries(): unsupported kind (string); must be slice")}},
		{"unsupported input: complex slice", args{slice: []complex64{1}},
			&Series{err: errors.New("NewSeries(): unable to calculate null values ([]complex64 not supported)")}},
		{"unsupported label input: scalar", args{slice: []float64{1}, labels: []interface{}{"foo"}},
			&Series{err: errors.New("NewSeries(): unsupported label kind (string) at level 0; must be slice")}},
		{"unsupported label input: complex slice", args{slice: []float64{1}, labels: []interface{}{[]complex64{1}}},
			&Series{err: errors.New("NewSeries(): unable to calculate null values at level 0 ([]complex64 not supported)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSeries(tt.args.slice, tt.args.labels...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("NewSeries() = %v, want %v", got, tt.want)
				t.Errorf("Error %v vs %v", got.err, tt.want.err)
			}
		})
	}
}

func TestSeries_Copy(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{[]float64{1}, "foo", []bool{false}},
				labels: []*valueContainer{{[]float64{1}, "bar", []bool{false}}}},
			&Series{
				values: &valueContainer{[]float64{1}, "foo", []bool{false}},
				labels: []*valueContainer{{[]float64{1}, "bar", []bool{false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got := s.Copy()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Copy() = %v, want %v", got, tt.want)
			}
			got.values.isNull[0] = true
			if reflect.DeepEqual(got, s) {
				t.Errorf("Series.Copy() = retained reference to original values")
			}
			got.err = errors.New("foo")
			if reflect.DeepEqual(got, s) {
				t.Errorf("Series.Copy() retained reference to original error")
			}
		})
	}
}

func TestSeries_ToDataFrame(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrame
	}{
		{"pass",
			fields{
				values: &valueContainer{[]float64{1}, "foo", []bool{false}},
				labels: []*valueContainer{{[]float64{1}, "bar", []bool{false}}},
				err:    errors.New("foo")},
			&DataFrame{
				values: []*valueContainer{{[]float64{1}, "foo", []bool{false}}},
				labels: []*valueContainer{{[]float64{1}, "bar", []bool{false}}},
				err:    errors.New("foo")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.ToDataFrame(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.ToDataFrame() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Subset(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]int{2, 0}},
			&Series{
				values: &valueContainer{slice: []float64{3, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{2, 0}, isNull: []bool{false, false}}}}},
		{"fail: index out of range",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]int{3}},
			&Series{
				err: errors.New("Subset(): index out of range (3 > 2)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Subset(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Subset() = %#v, want %#v", got.err, tt.want.err)
			}
		})
	}
}

func TestSeries_SubsetLabels(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}, {slice: []int{10}, isNull: []bool{false}}, {slice: []int{20}, isNull: []bool{false}}}},
			args{[]int{2, 0}},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{20}, isNull: []bool{false}}, {slice: []int{0}, isNull: []bool{false}}}}},
		{"fail: index out of range",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}, {slice: []int{10}, isNull: []bool{false}}, {slice: []int{20}, isNull: []bool{false}}}},
			args{[]int{3}},
			&Series{
				err: errors.New("SubsetLabels(): index out of range (3 > 2)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.SubsetLabels(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SubsetLabels() = %#v, want %#v", got.labels[0], tt.want.labels[0])
			}
		})
	}
}

func TestSeries_Head(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		rows int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2},
			&Series{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"max out at slice length",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{5},
			&Series{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Head(tt.args.rows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Head() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Tail(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		rows int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2},
			&Series{
				values: &valueContainer{slice: []float64{2, 3}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}}}}},
		{"max out at slice length",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{5},
			&Series{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Tail(tt.args.rows); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Tail() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestSeries_Range(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		first int
		last  int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{1, 2},
			&Series{
				values: &valueContainer{slice: []float64{2, 3}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Range(tt.args.first, tt.args.last); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Range() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Valid(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []string{"foo", ""}, isNull: []bool{false, true}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			&Series{
				values: &valueContainer{slice: []string{"foo"}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Valid(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Valid() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Null(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []string{"foo", ""}, isNull: []bool{false, true}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			&Series{
				values: &valueContainer{slice: []string{""}, isNull: []bool{true}},
				labels: []*valueContainer{{slice: []int{1}, isNull: []bool{false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Null(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Null() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_WithLabels(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		name string
		arg  interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"change name",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"bar", "baz"},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "baz"}}},
		},
		{"overwrite all labels at level",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"bar", []string{"baz"}},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "bar"}}},
		},
		{"append labels at level",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"qux", []string{""}},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{
					{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"},
					{slice: []string{""}, isNull: []bool{true}, name: "qux"},
				}},
		},
		{"fail: string name not in labels",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"qux", "baz"},
			&Series{err: errors.New("WithLabels(): cannot rename column: name (qux) does not match any existing column")},
		},
		{"fail: unsupported slice type",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"qux", []complex64{1}},
			&Series{err: errors.New("WithLabels(): unable to calculate null values ([]complex64 not supported)")},
		},
		{"fail: length of labels does not match length of series",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"qux", []string{"waldo", "corge"}},
			&Series{err: errors.New("WithLabels(): cannot replace items in column qux: length of input does not match existing length (2 != 1)")},
		},
		{"fail: unsupported input",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"qux", map[string]interface{}{"foo": "bar"}},
			&Series{err: errors.New("WithLabels(): unsupported input kind: must be either slice or string")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.WithLabels(tt.args.name, tt.args.arg); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.WithLabels() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestSeries_Elements(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		level []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []Element
	}{
		{"values",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}},
			args{nil}, []Element{{val: float64(1), isNull: false}}},
		{"label level",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}},
			args{[]int{0}}, []Element{{val: "foo", isNull: false}}},
		{"fail: label level not in index",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}},
			args{[]int{1}}, []Element{}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Elements(tt.args.level...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Elements() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Name(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}},
			args{"bar"},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Name(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Drop(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		index int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, true}},
				labels: []*valueContainer{{slice: []string{"foo", "bar", ""}, isNull: []bool{false, false, true}}}},
			args{1},
			&Series{
				values: &valueContainer{slice: []float64{1, 3}, isNull: []bool{false, true}},
				labels: []*valueContainer{{slice: []string{"foo", ""}, isNull: []bool{false, true}}}}},
		{"fail: out of index",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, true}},
				labels: []*valueContainer{{slice: []string{"foo", "bar", ""}, isNull: []bool{false, false, true}}}},
			args{3},
			&Series{
				err: errors.New("Drop(): index out of range (3 > 2)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Drop(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Drop() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Sort(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		by []Sorter
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		// {"sort values as float by default",
		// 	fields{
		// 		values: &valueContainer{slice: []float64{3, 1, 2}, isNull: []bool{false, false, false}},
		// 		labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
		// 	args{nil},
		// 	&Series{
		// 		values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
		// 		labels: []*valueContainer{{slice: []int{1, 2, 0}, isNull: []bool{false, false, false}}}}},
		// {"sort string descending",
		// 	fields{
		// 		values: &valueContainer{slice: []string{"bar", "foo"}, isNull: []bool{false, false}},
		// 		labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
		// 	args{[]Sorter{Sorter{DType: String, Descending: true}}},
		// 	&Series{
		// 		values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, false}},
		// 		labels: []*valueContainer{{slice: []int{1, 0}, isNull: []bool{false, false}}}}},
		// {"sort labels as string then as float",
		// 	fields{
		// 		values: &valueContainer{slice: []string{"baz", "foo", "baz"}, isNull: []bool{false, false, false}},
		// 		labels: []*valueContainer{{name: "*0", slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
		// 	args{[]Sorter{Sorter{DType: String}, Sorter{ColName: "*0", Descending: true}}},
		// 	&Series{
		// 		values: &valueContainer{slice: []string{"baz", "baz", "foo"}, isNull: []bool{false, false, false}},
		// 		labels: []*valueContainer{{name: "*0", slice: []int{2, 0, 1}, isNull: []bool{false, false, false}}}}},
		{"fail: bad label level name",
			fields{
				values: &valueContainer{slice: []string{"baz", "foo", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]Sorter{{ColName: "foo", Descending: true}}},
			&Series{
				err: errors.New("Sort(): cannot use label level: name (foo) does not match any existing column")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Sort(tt.args.by...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Sort() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestSeries_Filter(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		filters []FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"float filter",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]FilterFn{{F64: func(val float64, isNull bool) bool {
				if val > 1 && !isNull {
					return true
				}
				return false
			}}}}, []int{2}},
		{"float and string intersection",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []string{"bar", "foo", "baz"}, isNull: []bool{false, false, false}}}},
			args{[]FilterFn{
				{F64: func(val float64, isNull bool) bool {
					if val > 1 {
						return true
					}
					return false
				}},
				{ColName: "*0", String: func(val string, isNull bool) bool {
					if strings.Contains(val, "a") {
						return true
					}
					return false
				}},
			}}, []int{2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Filter(tt.args.filters...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_IterRows(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   []map[string]Element
	}{
		{"single label level, named values", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
			labels: []*valueContainer{{name: "*0", slice: []string{"bar", ""}, isNull: []bool{false, true}}}},
			[]map[string]Element{
				{"foo": Element{float64(1), false}, "*0": Element{"bar", false}},
				{"foo": Element{float64(2), false}, "*0": Element{"", true}},
			}},
		{"multi label levels, unnamed values", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{
				{name: "*0", slice: []string{"bar", ""}, isNull: []bool{false, true}},
				{name: "*1", slice: []string{"foo", "baz"}, isNull: []bool{false, false}},
			}},
			[]map[string]Element{
				{"": Element{float64(1), false}, "*0": Element{"bar", false}, "*1": Element{"foo", false}},
				{"": Element{float64(2), false}, "*0": Element{"", true}, "*1": Element{"baz", false}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.IterRows(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.IterRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Lookup(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		other   *Series
		how     string
		leftOn  []string
		rightOn []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"single label level, named keys, left join", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&Series{values: &valueContainer{slice: []float64{30, 0}, isNull: []bool{false, true}},
				labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
		},
		{"single label level, no named keys, left join", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: nil, rightOn: nil},
			&Series{values: &valueContainer{slice: []float64{30, 0}, isNull: []bool{false, true}},
				labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
		},
		{"multiple label level, no named keys, left join, match at index 1", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{
				{name: "waldo", slice: []string{"baz", "bar"}, isNull: []bool{false, false}},
				{name: "corge", slice: []int{0, 1}, isNull: []bool{false, false}},
			}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{
						{name: "corge", slice: []int{3, 1, 5}, isNull: []bool{false, false, false}},
						{name: "waldo", slice: []string{"baz", "bar", "quux"}, isNull: []bool{false, false}}}},
				how:    "left",
				leftOn: nil, rightOn: nil},
			&Series{values: &valueContainer{slice: []float64{0, 20}, isNull: []bool{true, false}},
				labels: []*valueContainer{
					{name: "waldo", slice: []string{"baz", "bar"}, isNull: []bool{false, false}},
					{name: "corge", slice: []int{0, 1}, isNull: []bool{false, false}}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Lookup(tt.args.other, tt.args.how, tt.args.leftOn, tt.args.rightOn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Lookup() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestSeries_Apply(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		function ApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"apply to series values by default",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{F64: func(v float64) float64 { return v * 2 }}},
			&Series{
				values: &valueContainer{slice: []float64{0, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"apply to label level and coerce to float",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{F64: func(v float64) float64 { return v * 2 }, ColName: "*0"}},
			&Series{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []float64{0, 2}, isNull: []bool{false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Apply(tt.args.function); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Apply() = %v, want %v", got.labels, tt.want)

			}
		})
	}
}

func TestSeries_Add(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		other         *Series
		ignoreMissing bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"ignore missing - match on different indexes",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: true},
			&Series{
				values: &valueContainer{slice: []float64{1, 6}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"missing as null",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: false},
			&Series{
				values: &valueContainer{slice: []float64{0, 6}, isNull: []bool{true, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Add(tt.args.other, tt.args.ignoreMissing); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Add() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Subtract(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		other         *Series
		ignoreMissing bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"ignore missing",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: true},
			&Series{
				values: &valueContainer{slice: []float64{1, -2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"missing as null",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: false},
			&Series{
				values: &valueContainer{slice: []float64{0, -2}, isNull: []bool{true, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Subtract(tt.args.other, tt.args.ignoreMissing); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Subtract() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestSeries_Multiply(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		other         *Series
		ignoreMissing bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"ignore missing",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: true},
			&Series{
				values: &valueContainer{slice: []float64{1, 8}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"missing as null",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: false},
			&Series{
				values: &valueContainer{slice: []float64{0, 8}, isNull: []bool{true, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Multiply(tt.args.other, tt.args.ignoreMissing); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Multiply() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestSeries_Divide(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		other         *Series
		ignoreMissing bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"ignore missing",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{4, 10}, isNull: []bool{false, false}},
					labels: []*valueContainer{{slice: []int{1, 10}, isNull: []bool{false, false}}}},
				ignoreMissing: true},
			&Series{
				values: &valueContainer{slice: []float64{1, .5}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"missing as null - divide by 0",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{0, 2, 10}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{slice: []int{0, 1, 10}, isNull: []bool{false, false, false}}}},
				ignoreMissing: false},
			&Series{
				values: &valueContainer{slice: []float64{0, 1, 0}, isNull: []bool{true, false, true}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Divide(tt.args.other, tt.args.ignoreMissing); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Divide() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestSeries_Sum(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{"pass", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 3},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Sum(); got != tt.want {
				t.Errorf("Series.Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Mean(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{"pass", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 1.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Mean(); got != tt.want {
				t.Errorf("Series.Mean() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Median(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{"odd", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 1.5},
		{"even", fields{values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}}}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Median(); got != tt.want {
				t.Errorf("Series.Median() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Std(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   float64
	}{
		{"pass", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 0.5},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Std(); got != tt.want {
				t.Errorf("Series.Std() = %v, want %v", got, tt.want)
			}
		})
	}
}
