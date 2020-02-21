package tada

import (
	"errors"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/d4l3k/messagediff"
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
			&Series{values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}}}},
		{"[]float64, supplied labels", args{slice: []float64{1}, labels: []interface{}{[]string{"bar"}}},
			&Series{values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				labels: []*valueContainer{{slice: []string{"bar"}, isNull: []bool{false}, name: "*0"}}}},
		{"unsupported input: empty slice", args{slice: []float64{}},
			&Series{err: errors.New("NewSeries(): `slice`: empty slice: cannot be empty")}},
		{"unsupported label input: scalar", args{slice: []float64{1}, labels: []interface{}{"foo"}},
			&Series{err: errors.New("NewSeries(): `labels`: error at position 0: unsupported kind (string); must be slice")}},
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
				values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}},
			&Series{
				values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}}},
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
			got = s.Copy()
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
				values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}},
				err:    errors.New("foo")},
			&DataFrame{
				values:        []*valueContainer{{slice: []float64{1}, name: "foo", isNull: []bool{false}}},
				labels:        []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}},
				err:           errors.New("foo"),
				colLevelNames: []string{"*0"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got := s.ToDataFrame()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.ToDataFrame() = %v, want %v", got, tt.want)
			}
			got.labels[0] = &valueContainer{slice: []float64{10}, name: "baz", isNull: []bool{false}}
			if reflect.DeepEqual(got.labels, s.labels) {
				t.Errorf("Series.ToDataFrame() retained reference to original labels")
			}
		})
	}
}

func TestSeries_ToCSV(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
			labels: []*valueContainer{{slice: []int{1}, name: "bar", isNull: []bool{false}}}},
			args{false},
			[][]string{{"bar", "foo"}, {"1", "1"}}, false},
		{"fail - empty", fields{
			values: nil,
			labels: nil},
			args{false},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got, err := s.ToCSV(tt.args.ignoreLabels)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.ToCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.ToCSV() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Err(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"error present",
			fields{
				values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}},
				err:    errors.New("foo")},
			true},
		{"no error present",
			fields{
				values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if err := s.Err(); (err != nil) != tt.wantErr {
				t.Errorf("Series.Err() error = %v, wantErr %v", err, tt.wantErr)
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
		{"likely invalid filter",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]int{-999}},
			&Series{
				err: errors.New("Subset(): likely invalid filter (every filter must have at least one filter function; if ColName is supplied, it must be valid)")}},
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
				values: &valueContainer{slice: []float64{2}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []int{1}, isNull: []bool{false}}}}},
		{"fail - first",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{3, 4},
			&Series{
				err: errors.New("Range(): first index out of range (3 > 2)")}},
		{"fail - last",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2, 4},
			&Series{
				err: errors.New("Range(): last index out of range (4 > 3)")}},
		{"fail - first > last",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2, 1},
			&Series{
				err: errors.New("Range(): first is greater than last (2 > 1)")}},
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
			if got := s.DropNull(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.DropNull() = %v, want %v", got, tt.want)
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
		{"append new Series - ignore new Series labels",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"qux", &Series{
				values: &valueContainer{slice: []string{""}, isNull: []bool{true}},
				labels: []*valueContainer{{slice: []string{"anything"}, isNull: []bool{false}, name: "bar"}}},
			},
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
			&Series{err: errors.New("WithLabels(): cannot rename column: `name` (qux) not found")},
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
			&Series{err: errors.New("WithLabels(): unsupported input kind: must be either slice, string, or Series")},
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
				t.Errorf("Series.WithLabels() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestSeries_WithValues(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		input interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"overwrite values",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "qux"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{[]string{"baz"}},
			&Series{
				values: &valueContainer{slice: []string{"baz"}, isNull: []bool{false}, name: "qux"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
		},
		{"change name",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "qux"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{"baz"},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "baz"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
		},
		{"fail",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "qux"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}}},
			args{[]float64{1, 2, 3}},
			&Series{err: errors.New("WithValues(): cannot replace items in column qux: length of input does not match existing length (3 != 1)")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.WithValues(tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.WithValues() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Append(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		other *Series
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass - coerce to string, maintain value/level names",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "qux"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "bar"}}},
			args{&Series{
				values: &valueContainer{slice: []int{2}, isNull: []bool{false}, name: "corge"},
				labels: []*valueContainer{{slice: []bool{true}, isNull: []bool{false}, name: "baz"}}}},
			&Series{
				values: &valueContainer{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "qux"},
				labels: []*valueContainer{{slice: []string{"0", "true"}, isNull: []bool{false, false}, name: "bar"}}},
		},
		{"fail - different number of levels",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "qux"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "bar"}}},
			args{&Series{
				values: &valueContainer{slice: []int{2}, isNull: []bool{false}, name: "corge"},
				labels: []*valueContainer{
					{slice: []bool{true}, isNull: []bool{false}, name: "baz"},
					{slice: []bool{true}, isNull: []bool{false}, name: "baz"},
				}}},
			&Series{
				err: errors.New("Append(): other Series must have same number of label levels as original Series (2 != 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Append(tt.args.other); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Append() = %v, want %v", got, tt.want)
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
			args{nil}, []Element{{Val: float64(1), IsNull: false}}},
		{"label level",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}},
			args{[]int{0}}, []Element{{Val: "foo", IsNull: false}}},
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

func TestSeries_Relabel(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		levelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "*0"},
				{slice: []float64{1}, isNull: []bool{false}, name: "*1"}}},
			args{[]string{"*0", "*1"}},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []int{0}, isNull: []bool{false}, name: "*1"}}},
		},
		{"fail", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "*0"},
				{slice: []float64{1}, isNull: []bool{false}, name: "*1"}}},
			args{[]string{"*0", "corge"}},
			&Series{
				err: errors.New("Relabel(): `name` (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Relabel(tt.args.levelNames); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Relabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SetLevelNames(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values *valueContainer
		err    error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}}},
			args{[]string{"bar"}},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"}}},
		},
		{"fail - too many", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}}},
			args{[]string{"bar", "qux"}},
			&Series{
				err: errors.New("SetLevelNames(): number of `levelNames` must match number of levels in Series (2 != 1)")},
		},
		{"fail - too few", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []float64{1}, isNull: []bool{false}, name: "*1"}}},
			args{[]string{"qux"}},
			&Series{
				err: errors.New("SetLevelNames(): number of `levelNames` must match number of levels in Series (1 != 2)")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &Series{
				labels: tt.fields.labels,
				values: tt.fields.values,
				err:    tt.fields.err,
			}
			if got := df.SetLevelNames(tt.args.colNames); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.SetLevelNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SetName(t *testing.T) {
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
			if got := s.SetName(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SetName() = %v, want %v", got, tt.want)
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
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"normal",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "baz"},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}}}},
			"baz"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Name(); got != tt.want {
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
		{"sort values as float by default",
			fields{
				values: &valueContainer{slice: []float64{3, 1, 2}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{nil},
			&Series{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{1, 2, 0}, isNull: []bool{false, false, false}}}}},
		{"sort string descending",
			fields{
				values: &valueContainer{slice: []string{"bar", "foo"}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{[]Sorter{Sorter{DType: String, Descending: true}}},
			&Series{
				values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{1, 0}, isNull: []bool{false, false}}}}},
		{"sort labels as string then as float",
			fields{
				values: &valueContainer{slice: []string{"baz", "foo", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]Sorter{Sorter{DType: String}, Sorter{ContainerName: "*0", Descending: true}}},
			&Series{
				values: &valueContainer{slice: []string{"baz", "baz", "foo"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{2, 0, 1}, isNull: []bool{false, false, false}}}}},
		{"fail: bad label level name",
			fields{
				values: &valueContainer{slice: []string{"baz", "foo", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]Sorter{{ContainerName: "foo", Descending: true}}},
			&Series{
				err: errors.New("Sort(): cannot use label level: `name` (foo) not found")}},
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
			args{[]FilterFn{{F64: func(val float64) bool {
				if val > 1 {
					return true
				}
				return false
			}}}}, []int{2}},
		{"float and string intersection",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []string{"bar", "foo", "baz"}, isNull: []bool{false, false, false}}}},
			args{[]FilterFn{
				{F64: func(val float64) bool {
					if val > 1 {
						return true
					}
					return false
				}},
				{ContainerName: "*0", String: func(val string) bool {
					if strings.Contains(val, "a") {
						return true
					}
					return false
				}},
			}}, []int{2}},
		{"all values",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{nil}, []int{0, 1, 2}},
		{"fail: no filter function",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]FilterFn{{ContainerName: "*0"}}}, []int{-999}},
		{"fail: no matching col",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]FilterFn{{ContainerName: "corge"}}}, []int{-999}},
		{"fail: failure in multi filter",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{[]FilterFn{{ContainerName: "*0"}, {ContainerName: "corge"}}}, []int{-999}},
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

func TestSeries_GT(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"gt",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{1},
			[]int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.GT(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GT() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_GTE(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"gte",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{1},
			[]int{0, 1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.GTE(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GTE() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_LT(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"lt",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2},
			[]int{0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.LT(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.LT() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_LTE(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"lte",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2},
			[]int{0, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.LTE(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.LTE() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_FloatEQ(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"eq",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2},
			[]int{1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.FloatEQ(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.FloatEQ() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_FloatNEQ(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"neq",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{2},
			[]int{0, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.FloatNEQ(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.FloatNEQ() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_EQ(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"eq",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{"foo"},
			[]int{0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.EQ(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.EQ() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_NEQ(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"eq",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{"foo"},
			[]int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.NEQ(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.NEQ() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Contains(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"contains",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{"ba"},
			[]int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Contains(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Contains() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Before(t *testing.T) {
	sample := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"before",
			fields{
				values: &valueContainer{slice: []time.Time{sample, sample.AddDate(0, 0, 1), sample.AddDate(0, 0, 2)}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{sample.AddDate(0, 0, 1)},
			[]int{0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Before(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Before() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_BeforeOrEqual(t *testing.T) {
	sample := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"beforeOrEqual",
			fields{
				values: &valueContainer{slice: []time.Time{sample, sample.AddDate(0, 0, 1), sample.AddDate(0, 0, 2)}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{sample.AddDate(0, 0, 1)},
			[]int{0, 1},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.BeforeOrEqual(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.BeforeOrEqual() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_After(t *testing.T) {
	sample := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"after",
			fields{
				values: &valueContainer{slice: []time.Time{sample, sample.AddDate(0, 0, 1), sample.AddDate(0, 0, 2)}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{sample.AddDate(0, 0, 1)},
			[]int{2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.After(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.After() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_AfterOrEqual(t *testing.T) {
	sample := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
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
		{"after",
			fields{
				values: &valueContainer{slice: []time.Time{sample, sample.AddDate(0, 0, 1), sample.AddDate(0, 0, 2)}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{sample.AddDate(0, 0, 1)},
			[]int{1, 2},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.AfterOrEqual(tt.args.comparison); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.AfterOrEqual() = %v, want %v", got, tt.want)
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

func TestSeries_LookupAdvanced(t *testing.T) {
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
		{"fail - leftOn but not rightOn", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: nil},
			&Series{err: errors.New("LookupAdvanced(): if either leftOn or rightOn is empty, both must be empty")},
		},
		{"fail - no matching left key", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"corge"}, rightOn: []string{"foo"}},
			&Series{err: errors.New("LookupAdvanced(): `name` (corge) not found")},
		},
		{"fail - no matching right key", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"corge"}},
			&Series{err: errors.New("LookupAdvanced(): `name` (corge) not found")},
		},
		{"fail - unsupported how", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "other",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&Series{err: errors.New("LookupAdvanced(): `how`: must be `left`, `right`, or `inner`")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.LookupAdvanced(tt.args.other, tt.args.how, tt.args.leftOn, tt.args.rightOn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.LookupAdvanced() = %v, want %v", got.err, tt.want.err)
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
			args{ApplyFn{F64: func(v float64) float64 { return v * 2 }, ContainerName: "*0"}},
			&Series{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []float64{0, 2}, isNull: []bool{false, false}}}}},
		{"apply with prior null",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{true, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{F64: func(v float64) float64 { return v * 2 }, ContainerName: ""}},
			&Series{
				values: &valueContainer{slice: []float64{0, 2}, isNull: []bool{true, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"fail: no function supplied",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{ContainerName: "*0"}},
			&Series{
				err: errors.New("Apply(): no apply function provided")}},
		{"fail: no matching col",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{ContainerName: "corge", F64: func(float64) float64 { return 1 }}},
			&Series{
				err: errors.New("Apply(): `name` (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Apply(tt.args.function); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Apply() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestSeries_ApplyFormat(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		lambda ApplyFormatFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"apply to series values by default",
			fields{
				values: &valueContainer{slice: []float64{0, .25}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFormatFn{F64: func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }}},
			&Series{
				values: &valueContainer{slice: []string{"0.0", "0.2"}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"apply to level",
			fields{
				values: &valueContainer{slice: []float64{0, .25}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"}}},
			args{ApplyFormatFn{ContainerName: "foo",
				F64: func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }}},
			&Series{
				values: &valueContainer{slice: []float64{0, 0.25}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []string{"0.0", "1.0"}, isNull: []bool{false, false}, name: "foo"}}}},
		{"fail: no function supplied",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFormatFn{ContainerName: "*0"}},
			&Series{
				err: errors.New("ApplyFormat(): no apply function provided")}},
		{"fail: no matching col",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFormatFn{ContainerName: "corge", F64: func(float64) string { return "foo" }}},
			&Series{
				err: errors.New("ApplyFormat(): `name` (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.ApplyFormat(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.ApplyFormat() = %v, want %v", got, tt.want)
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

func TestSeries_Count(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"pass", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Count(); got != tt.want {
				t.Errorf("Series.Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Min(t *testing.T) {
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
		{"pass", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 1},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Min(); got != tt.want {
				t.Errorf("Series.Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Max(t *testing.T) {
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
		{"pass", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Max(); got != tt.want {
				t.Errorf("Series.Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_GroupBy(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		names []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedSeries
	}{
		{"group by all levels, with repeats", fields{
			values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
			labels: []*valueContainer{
				{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
				{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
			}},
			args{nil},
			&GroupedSeries{
				rowIndices:  [][]int{{0, 1}, {2}, {3}},
				orderedKeys: []string{"0|foo", "1|foo", "2|bar"},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "a"},
					{slice: []string{"foo", "foo", "bar"}, isNull: []bool{false, false, false}, name: "b"}},
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
						{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"}},
				},
			}},
		{"group by specific level", fields{
			values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
			labels: []*valueContainer{
				{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
				{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
			}},
			args{[]string{"b"}},
			&GroupedSeries{
				rowIndices:  [][]int{{0, 1, 2}, {3}},
				orderedKeys: []string{"foo", "bar"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "b"}},
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
						{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
					}},
			}},
		{"fail - no matching level", fields{
			values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
			labels: []*valueContainer{
				{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
				{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
			}},
			args{[]string{"corge"}},
			&GroupedSeries{
				err: errors.New("GroupBy(): `name` (corge) not found"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.GroupBy(tt.args.names...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GroupBy() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestSeries_Shift(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		n int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "a"},
				}},
			args{1},
			&Series{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{true, false}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "a"},
				}},
		},
		{"overwrite n to max",
			fields{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "a"},
				}},
			args{5},
			&Series{
				values: &valueContainer{slice: []float64{0, 0}, isNull: []bool{true, true}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "a"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Shift(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Shift() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestSeries_Where(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		filters []FilterFn
		ifTrue  interface{}
		ifFalse interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{[]FilterFn{
				{ContainerName: "qux", F64: func(v float64) bool {
					if v > 1 {
						return true
					}
					return false
				}},
				{String: func(v string) bool {
					if strings.Contains(v, "ba") {
						return true
					}
					return false
				}},
			}, "yes", 0},
			&Series{
				values: &valueContainer{slice: []interface{}{0, 0, "yes"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Where(tt.args.filters, tt.args.ifTrue, tt.args.ifFalse); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Where() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Cut(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		bins    []float64
		andLess bool
		andMore bool
		labels  []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{1, 2}, andLess: false, andMore: true, labels: nil},
			&Series{
				values: &valueContainer{slice: []string{"", "1-2", ">2"}, isNull: []bool{true, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
		{"fail - too many labels", fields{
			values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{1, 2}, andLess: false, andMore: false, labels: []string{"foo", "bar"}},
			&Series{
				err: errors.New("Cut(): number of bin edges (+ includeLess + includeMore), must be one more than number of supplied labels: (2 + 0 + 0) != (2 + 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Cut(tt.args.bins, tt.args.andLess, tt.args.andMore, tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Cut() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestSeries_CumSum(t *testing.T) {
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
		{"pass", fields{
			values: &valueContainer{slice: []float64{3, 2, 0}, isNull: []bool{false, false, true}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			&Series{
				values: &valueContainer{slice: []float64{3, 5, 5}, isNull: []bool{false, false, false}, name: "cumsum"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.CumSum(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.CumSum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Rank(t *testing.T) {
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
		{"pass", fields{
			values: &valueContainer{slice: []float64{3, 2, 0}, isNull: []bool{false, false, true}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			&Series{
				values: &valueContainer{slice: []float64{2, 1, -999}, isNull: []bool{false, false, true}, name: "rank"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Rank(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Rank() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_PercentileCut(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		bins   []float64
		labels []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1, 3, 5}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{0, .5, 1}, labels: []string{"Bottom 50%", "Top 50%"}},
			&Series{
				values: &valueContainer{slice: []string{"Bottom 50%", "Bottom 50%", "Top 50%"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
		{"fail - too many labels", fields{
			values: &valueContainer{slice: []float64{1, 3, 5}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{0, .5, 1}, labels: []string{"Bottom 50%", "Too Many Labels", "Top 50%"}},
			&Series{
				err: errors.New("PercentileCut(): number of bin edges (+ includeLess + includeMore), must be one more than number of supplied labels: (3 + 0 + 0) != (3 + 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.PercentileCut(tt.args.bins, tt.args.labels); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.PercentileCut() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Earliest(t *testing.T) {
	date := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{"pass", fields{values: &valueContainer{
			slice: []time.Time{date, date.AddDate(0, 0, 1)}, isNull: []bool{false, false}}}, date},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Earliest(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Earliest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Latest(t *testing.T) {
	date := time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   time.Time
	}{
		{"pass", fields{values: &valueContainer{
			slice: []time.Time{date, date.AddDate(0, 0, 1)}, isNull: []bool{false, false}}}, date.AddDate(0, 0, 1)},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Latest(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.Latest() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SliceFloat64(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		labelLevel []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []float64
	}{
		{"default values", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{nil},
			[]float64{1, 0},
		},
		{"label level 1", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{0}},
			[]float64{0, 1},
		},
		{"fail - index out of range", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{10}},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.SliceFloat64(tt.args.labelLevel...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SliceFloat64() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SliceString(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		labelLevel []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{"default values", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{nil},
			[]string{"1", "0"},
		},
		{"label level 1", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{0}},
			[]string{"0", "1"},
		},
		{"fail - index out of range", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{10}},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.SliceString(tt.args.labelLevel...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SliceString() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SliceNulls(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		labelLevel []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []bool
	}{
		{"default values", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{nil},
			[]bool{false, true},
		},
		{"label level 1", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{0}},
			[]bool{false, false},
		},
		{"fail - index out of range", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{10}},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.SliceNulls(tt.args.labelLevel...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SliceNulls() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestSeries_SliceTime(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		labelLevel []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []time.Time
	}{
		{"default values", fields{
			values: &valueContainer{slice: []string{"2020/1/1"}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{nil},
			[]time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		{"label level 1", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []string{"2020/1/1"}, isNull: []bool{false}, name: "qux"}}},
			args{[]int{0}},
			[]time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
		{"fail - index out of range", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			args{[]int{10}},
			nil,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.SliceTime(tt.args.labelLevel...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.SliceTime() = %v, want %v", got, tt.want)
			}
		})
	}
}
