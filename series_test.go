package tada

import (
	"bytes"
	"errors"
	"io"
	"log"
	"os"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ptiger10/tablediff"
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
		{"[]float64, default values", args{slice: nil, labels: []interface{}{[]string{"bar"}}},
			&Series{values: &valueContainer{slice: []int{0}, isNull: []bool{false}, name: "0"},
				labels: []*valueContainer{{slice: []string{"bar"}, isNull: []bool{false}, name: "*0"}}}},
		{"unsupported input: nil slice, nil labels", args{slice: nil},
			&Series{err: errors.New("constructing new Series: slice and labels cannot both be nil")}},
		{"unsupported input: empty slice", args{slice: []float64{}},
			&Series{err: errors.New("constructing new Series: slice: empty slice: cannot be empty")}},
		{"unsupported label input: scalar", args{slice: []float64{1}, labels: []interface{}{"foo"}},
			&Series{err: errors.New("constructing new Series: labels: error at position 0: unsupported kind (string); must be slice")}},
		{"fail - different lengths", args{slice: []float64{1}, labels: []interface{}{[]int{0, 1}}},
			&Series{err: errors.New("constructing new Series: labels: position 0: slice does not match required length (2 != 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewSeries(tt.args.slice, tt.args.labels...); !EqualSeries(got, tt.want) {
				t.Errorf("NewSeries() = %v, want %v", got, tt.want)
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
			if !EqualSeries(got, tt.want) {
				t.Errorf("Series.Copy() = %v, want %v", got, tt.want)
			}
			if !seriesIsDistinct(got, s) {
				t.Errorf("Series.Copy() retained reference to original, want copy")
			}
		})
	}
}

func TestSeries_Cast(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		containerAsType map[string]DType
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []int{1}, name: "foo", isNull: []bool{false}},
			labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}},
			args{map[string]DType{"": Float64}},
			&Series{
				values: &valueContainer{slice: []float64{1}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}}},
		{"fail", fields{
			values: &valueContainer{slice: []int{1}, name: "foo", isNull: []bool{false}},
			labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}},
			args{map[string]DType{"corge": Float64}},
			&Series{
				err: errors.New("Cast(): name (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			s.Cast(tt.args.containerAsType)
			if !EqualSeries(s, tt.want) {
				t.Errorf("Series.Cast() -> %v, want %v", s, tt.want)
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
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("Series.ToDataFrame() = %v, want %v", got, tt.want)
			}
			got.labels[0] = &valueContainer{slice: []float64{10}, name: "baz", isNull: []bool{false}}
			if reflect.DeepEqual(got.labels, s.labels) {
				t.Errorf("Series.ToDataFrame() retained reference to original labels")
			}
		})
	}
}

func TestSeries_EqualsCSV(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		r             io.Reader
		includeLabels bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		want1   *tablediff.Differences
		wantErr bool
	}{
		{name: "pass",
			fields: fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}}},
			args:    args{r: strings.NewReader("*0, foo\n 0, 1"), includeLabels: true},
			want:    true,
			want1:   nil,
			wantErr: false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got, got1, err := s.EqualsCSV(tt.args.includeLabels, tt.args.r)
			if (err != nil) != tt.wantErr {
				t.Errorf("Series.EqualsCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("Series.EqualsCSV() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("Series.EqualsCSV() got1 = %v, want %v", got1, tt.want1)
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
		options []WriteOption
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
			args{},
			[][]string{{"bar", "foo"}, {"1", "1"}}, false},
		{"with nulls", fields{
			values: &valueContainer{slice: []float64{0}, name: "foo", isNull: []bool{true}},
			labels: []*valueContainer{{slice: []int{1}, name: "bar", isNull: []bool{false}}}},
			args{},
			[][]string{{"bar", "foo"}, {"1", "n/a"}}, false},
		{"fail - empty", fields{
			values: nil,
			labels: nil},
			args{},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got, err := s.ToCSV(tt.args.options...)
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
			if got := s.Subset(tt.args.index); !EqualSeries(got, tt.want) {
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
			if got := s.SubsetLabels(tt.args.index); !EqualSeries(got, tt.want) {
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
			if got := s.Head(tt.args.rows); !EqualSeries(got, tt.want) {
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
			if got := s.Tail(tt.args.rows); !EqualSeries(got, tt.want) {
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
			if got := s.Range(tt.args.first, tt.args.last); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Range() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_FillNull(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		how NullFiller
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"fill forward",
			fields{
				values: &valueContainer{slice: []string{"foo", ""}, isNull: []bool{false, true}, name: "qux"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{true, false}, name: "*0"}}},
			args{NullFiller{FillForward: true}},
			&Series{
				values: &valueContainer{slice: []string{"foo", "foo"}, isNull: []bool{false, false}, name: "qux"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{true, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.FillNull(tt.args.how); !EqualSeries(got, tt.want) {
				t.Errorf("Series.FillNull() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_DropNull(t *testing.T) {
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
			if got := s.DropNull(); !EqualSeries(got, tt.want) {
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
			if got := s.Null(); !EqualSeries(got, tt.want) {
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
			args{"corge", "baz"},
			&Series{err: errors.New("WithLabels(): cannot rename column: name (corge) not found")},
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
			if got := s.WithLabels(tt.args.name, tt.args.arg); !EqualSeries(got, tt.want) {
				t.Errorf("Series.WithLabels() = %v, want %v", got, tt.want)
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
			if got := s.WithValues(tt.args.input); !EqualSeries(got, tt.want) {
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
				values: &valueContainer{slice: []float64{2}, isNull: []bool{false}, name: "corge"},
				labels: []*valueContainer{{slice: []bool{true}, isNull: []bool{false}, name: "baz"}}}},
			&Series{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
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
			if got := s.Append(tt.args.other); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Append() = %v, want %v", got, tt.want)
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
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "*0"},
				{slice: []float64{1}, isNull: []bool{false}, name: "*1"}}},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Relabel(); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Relabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SetLabelNames(t *testing.T) {
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
				err: errors.New("SetLabelNames(): number of levelNames must match number of levels in Series (2 != 1)")},
		},
		{"fail - too few", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []float64{1}, isNull: []bool{false}, name: "*1"}}},
			args{[]string{"qux"}},
			&Series{
				err: errors.New("SetLabelNames(): number of levelNames must match number of levels in Series (1 != 2)")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &Series{
				labels: tt.fields.labels,
				values: tt.fields.values,
				err:    tt.fields.err,
			}
			if got := df.SetLabelNames(tt.args.colNames); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.SetLabelNames() = %v, want %v", got, tt.want)
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
			if got := s.SetName(tt.args.name); !EqualSeries(got, tt.want) {
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

func TestSeries_DropLabels(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
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
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{
					{slice: []string{"foo"}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"bar"}, isNull: []bool{false}, name: "bar"},
				}},
			args{"bar"},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{{slice: []string{"foo"}, isNull: []bool{false}, name: "foo"}}}},
		{"fail",
			fields{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}},
				labels: []*valueContainer{
					{slice: []string{"foo"}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"bar"}, isNull: []bool{false}, name: "bar"},
				}},
			args{"corge"},
			&Series{
				err: errors.New("DropLabels(): name (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.DropLabels(tt.args.name); !EqualSeries(got, tt.want) {
				t.Errorf("Series.DropLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_DropRow(t *testing.T) {
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
				err: errors.New("DropRow(): index out of range (3 > 2)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.DropRow(tt.args.index); !EqualSeries(got, tt.want) {
				t.Errorf("Series.DropRow() = %v, want %v", got, tt.want)
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
		{"sort values as Float64 by default",
			fields{
				values: &valueContainer{slice: []float64{3, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{nil},
			&Series{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{1, 2, 0}, isNull: []bool{false, false, false}, name: "*0"}}}},
		{"sort string descending",
			fields{
				values: &valueContainer{slice: []string{"bar", "foo"}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{[]Sorter{{DType: String, Descending: true}}},
			&Series{
				values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{1, 0}, isNull: []bool{false, false}}}}},
		{"sort first labels (descending), then values (ascending)",
			fields{
				values: &valueContainer{slice: []string{"baz", "foo", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]Sorter{{DType: String, Descending: false}, {Name: "*0", Descending: true}}},
			&Series{
				values: &valueContainer{slice: []string{"baz", "baz", "foo"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{2, 0, 1}, isNull: []bool{false, false, false}}}}},
		{"fail: bad label level name",
			fields{
				values: &valueContainer{slice: []string{"baz", "foo", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{[]Sorter{{Name: "corge", Descending: true}}},
			&Series{
				err: errors.New("Sort(): position 0: name (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Sort(tt.args.by...); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Sort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_FilterIndex(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		lambda FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"Float64 filter - default",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{FilterFn{Float64: func(val float64) bool {
				return val > 1
			}}},
			[]int{2},
		},
		{"Float64 filter - no matches",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{FilterFn{Float64: func(val float64) bool {
				return val > 5
			}}},
			[]int{},
		},
		{"Float64 filter - error",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{FilterFn{}},
			nil,
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
			if got := s.FilterIndex(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.FilterIndex() = %v, want %v", got, tt.want)
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
		filters map[string]FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"Float64 filter - default",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{"": {Float64: func(val float64) bool {
				return val > 1
			}}}},
			&Series{
				values: &valueContainer{slice: []float64{3}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{name: "*0", slice: []int{2}, isNull: []bool{false}}}},
		},
		{"Float64 filter - no matches",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{"": {Float64: func(val float64) bool {
				return val > 5
			}}}},
			&Series{
				values: &valueContainer{slice: []float64{}, isNull: []bool{}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{}, isNull: []bool{}, name: "*0"}}},
		},
		{"Float64 and string intersection",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				labels: []*valueContainer{{name: "*0", slice: []string{"bar", "foo", "baz"}, isNull: []bool{false, false, false}}}},
			args{map[string]FilterFn{
				"foo": {Float64: func(val float64) bool {
					if val > 1 {
						return true
					}
					return false
				}},
				"*0": {String: func(val string) bool {
					if strings.Contains(val, "a") {
						return true
					}
					return false
				}},
			}},
			&Series{
				values: &valueContainer{slice: []float64{3}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []string{"baz"}, isNull: []bool{false}, name: "*0"}}}},
		{"all values",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{nil},
			&Series{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			}},
		{"fail: no filter function",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{"*0": {}}},
			&Series{err: errors.New("Filter(): no filter function provided")},
		},
		{"fail: no matching col",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, true, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{"corge": {Float64: func(float64) bool { return true }}}},
			&Series{err: errors.New("Filter(): name (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Filter(tt.args.filters); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Filter() = %v, want %v", got, tt.want)
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
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "qux"},
					labels: []*valueContainer{{name: "bar", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"bar"}},
			&Series{values: &valueContainer{slice: []float64{30, 0}, isNull: []bool{false, true}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}}}},
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
				labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
					cache: []string{"bar", "baz"}}}},
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
					{name: "waldo", slice: []string{"baz", "bar"}, isNull: []bool{false, false},
						cache: []string{"baz", "bar"}},
					{name: "corge", slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"}}}},
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
		{"fail - no key supplied, and no matching key", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "corge", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: nil, rightOn: nil},
			&Series{err: errors.New("LookupAdvanced(): no matching keys between containers")},
		},
		{"fail - no matching left key", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"corge"}, rightOn: []string{"foo"}},
			&Series{err: errors.New("LookupAdvanced(): leftOn: name (corge) not found")},
		},
		{"fail - no matching right key", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"corge"}},
			&Series{err: errors.New("LookupAdvanced(): rightOn: name (corge) not found")},
		},
		{"fail - unsupported how", fields{
			values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "other",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&Series{err: errors.New("LookupAdvanced(): how: must be left, right, or inner")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.LookupAdvanced(tt.args.other, tt.args.how, tt.args.leftOn, tt.args.rightOn); !EqualSeries(got, tt.want) {
				t.Errorf("Series.LookupAdvanced() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Merge(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		other *Series
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"matching label key *0",
			fields{values: &valueContainer{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
			args{&Series{
				values: &valueContainer{slice: []string{"c"}, isNull: []bool{false}, name: "bar"},
				labels: []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "bar"},
				},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0",
						cache: []string{"0", "1"},
					}},
				colLevelNames: []string{"*0"}},
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
			if got := s.Merge(tt.args.other); !EqualDataFrames(got, tt.want) {
				t.Errorf("Series.Merge() = %v, want %v", got, tt.want)
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
		{"apply to series values",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{Float64: func(v float64) float64 { return v * 2 }}},
			&Series{
				values: &valueContainer{slice: []float64{0, 2}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"apply with null",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{true, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{Float64: func(v float64) float64 { return v * 2 }}},
			&Series{
				values: &valueContainer{slice: []float64{0, 2}, isNull: []bool{true, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"fail: no function supplied",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFn{}},
			&Series{
				err: errors.New("Apply(): no apply function provided")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Apply(tt.args.function); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Apply() = %v, want %v", got, tt.want)
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
			args{ApplyFormatFn{Float64: func(v float64) string { return strconv.FormatFloat(v, 'f', 1, 64) }}},
			&Series{
				values: &valueContainer{slice: []string{"0.0", "0.2"}, isNull: []bool{false, false}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}}}}},
		{"fail: no function supplied",
			fields{
				values: &valueContainer{slice: []float64{0, 1}, isNull: []bool{false, false}},
				labels: []*valueContainer{{name: "*0", slice: []int{0, 1}, isNull: []bool{false, false}}}},
			args{ApplyFormatFn{}},
			&Series{
				err: errors.New("ApplyFormat(): no apply function provided")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.ApplyFormat(tt.args.lambda); !EqualSeries(got, tt.want) {
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
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
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
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Add(tt.args.other, tt.args.ignoreMissing); !EqualSeries(got, tt.want) {
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
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
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
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Subtract(tt.args.other, tt.args.ignoreMissing); !EqualSeries(got, tt.want) {
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
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
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
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Multiply(tt.args.other, tt.args.ignoreMissing); !EqualSeries(got, tt.want) {
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
		other       *Series
		ignoreNulls bool
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
				ignoreNulls: true},
			&Series{
				values: &valueContainer{slice: []float64{1, .5}, isNull: []bool{false, false}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false},
						cache: []string{"0", "1"},
					}}},
		},
		{"missing as null - divide by 0",
			fields{
				values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}}}},
			args{
				other: &Series{values: &valueContainer{slice: []float64{0, 2, 10}, isNull: []bool{false, false, false}},
					labels: []*valueContainer{{slice: []int{0, 1, 10}, isNull: []bool{false, false, false}}}},
				ignoreNulls: false},
			&Series{
				values: &valueContainer{slice: []float64{0, 1, 0}, isNull: []bool{true, false, true}},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2}, isNull: []bool{false, false, false},
						cache: []string{"0", "1", "2"},
					}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Divide(tt.args.other, tt.args.ignoreNulls); !EqualSeries(got, tt.want) {
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
		{"count Float64", fields{values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}}}, 2},
		{"count string", fields{values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, false}}}, 2},
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
						{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a",
							cache: []string{"0", "0", "1", "2"}},
						{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b",
							cache: []string{"foo", "foo", "foo", "bar"}}},
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
						{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b",
							cache: []string{"foo", "foo", "foo", "bar"}},
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
				err: errors.New("GroupBy(): name (corge) not found"),
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.GroupBy(tt.args.names...); !equalGroupedSeries(got, tt.want) {
				t.Errorf("Series.GroupBy() = %v, want %v", got, tt.want)
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
			if got := s.Shift(tt.args.n); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Shift() = %v, want %v", got, tt.want)
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
		name    string
		filters map[string]FilterFn
		ifTrue  interface{}
		ifFalse interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *Series
		wantErr bool
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				name: "foo",
				filters: map[string]FilterFn{"qux": {Float64: func(v float64) bool {
					return v > 1
				}},
					"": {String: func(v string) bool {
						return strings.Contains(v, "ba")
					}},
				},
				ifTrue:  "yes",
				ifFalse: 0},
			&Series{
				values: &valueContainer{slice: []interface{}{0, 0, "yes"}, isNull: []bool{false, false, false}, name: ""},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			false},
		{"pass - nulls",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				name: "foo",
				filters: map[string]FilterFn{"qux": {Float64: func(v float64) bool {
					return v > 1
				}},
					"": {String: func(v string) bool {
						return strings.Contains(v, "ba")
					}},
				},
				ifTrue:  "yes",
				ifFalse: ""},
			&Series{
				values: &valueContainer{slice: []interface{}{"", "", "yes"}, isNull: []bool{true, true, false}, name: ""},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			false},
		{"fail - bad container name",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{"foo", map[string]FilterFn{"corge": {Float64: func(v float64) bool { return true }}}, "yes", 0},
			nil, true},
		{"fail - unsupported ifTrue",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{"foo", map[string]FilterFn{"qux": {Float64: func(v float64) bool { return true }}}, complex64(1), 0},
			nil, true},
		{"fail - unsupported ifFalse",
			fields{
				values: &valueContainer{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{"foo", map[string]FilterFn{"qux": {Float64: func(v float64) bool { return false }}}, 0, complex64(1)},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got, err := s.Where(tt.args.filters, tt.args.ifTrue, tt.args.ifFalse)
			if (err != nil) != tt.wantErr {
				t.Errorf("Series.Where() err = %v, want %v", err, tt.wantErr)
				return
			}
			if !EqualSeries(got, tt.want) {
				t.Errorf("Series.Where() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Bin(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		bins   []float64
		config *Binner
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
				bins: []float64{1, 2}, config: &Binner{AndLess: false, AndMore: true, Labels: nil}},
			&Series{
				values: &valueContainer{slice: []string{"", "1-2", ">2"}, isNull: []bool{true, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
		{"pass - nil binner", fields{
			values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{1, 2}, config: nil},
			&Series{
				values: &valueContainer{slice: []string{"", "1-2", ""}, isNull: []bool{true, false, true}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
		{"fail - too many labels", fields{
			values: &valueContainer{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{1, 2}, config: &Binner{AndLess: false, AndMore: false, Labels: []string{"foo", "bar"}}},
			&Series{
				err: errors.New("Bin(): number of bin edges (+ includeLess + includeMore), must be one more than number of supplied labels: (2 + 0 + 0) != (2 + 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Bin(tt.args.bins, tt.args.config); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Bin() = %v, want %v", got, tt.want)
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
			if got := s.CumSum(); !EqualSeries(got, tt.want) {
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
			if got := s.Rank(); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Rank() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_PercentileBin(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		bins   []float64
		config *Binner
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
				bins: []float64{0, .5, 1}, config: &Binner{Labels: []string{"Bottom 50%", "Top 50%"}}},
			&Series{
				values: &valueContainer{slice: []string{"Bottom 50%", "Bottom 50%", "Top 50%"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
		{"pass - nil config", fields{
			values: &valueContainer{slice: []float64{1, 3, 5}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{0, .5, 1}, config: nil},
			&Series{
				values: &valueContainer{slice: []string{"0-0.5", "0-0.5", "0.5-1"}, isNull: []bool{false, false, false}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}}},
		{"fail - too many labels", fields{
			values: &valueContainer{slice: []float64{1, 3, 5}, isNull: []bool{false, false, false}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				bins: []float64{0, .5, 1}, config: &Binner{Labels: []string{"Bottom 50%", "Medium 50%", "Top 50%"}}},
			&Series{
				err: errors.New("PercentileBin(): number of bin edges (+ includeLess + includeMore), must be one more than number of supplied labels: (3 + 0 + 0) != (3 + 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.PercentileBin(tt.args.bins, tt.args.config); !EqualSeries(got, tt.want) {
				t.Errorf("Series.PercentileBin() = %v, want %v", got, tt.want)
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

func TestSeries_GetValuesFloat64(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   []float64
	}{
		{"default values", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			[]float64{1, 0},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got := s.GetValuesFloat64()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GetValuesFloat64() = %v, want %v", got, tt.want)
			}
			got[0] = 10
			if reflect.DeepEqual(got, s.GetValuesFloat64()) {
				t.Errorf("Series.GetValuesFloat64() retained reference to original, want copy")
			}
		})
	}
}

func TestSeries_GetValuesString(t *testing.T) {
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
		want   []string
	}{
		{"default values", fields{
			values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			[]string{"foo", "bar"},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got := s.GetValuesString()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GetValuesString() = %v, want %v", got, tt.want)
			}
			got[0] = "baz"
			if reflect.DeepEqual(got, s.GetValuesString()) {
				t.Errorf("Series.GetValuesString() retained reference to original, want copy")
			}
		})
	}
}

func TestSeries_GetValuesTime(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   []time.Time
	}{
		{"default values", fields{
			values: &valueContainer{slice: []string{"1/1/2020"}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			[]time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got := s.GetValuesTime()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GetValuesTime() = %v, want %v", got, tt.want)
			}
			got[0] = time.Date(2020, 2, 1, 0, 0, 0, 0, time.UTC)
			if reflect.DeepEqual(got, s.GetValuesTime()) {
				t.Errorf("Series.GetValuesTime() retained reference to original, want copy")
			}
		})
	}
}

func TestSeries_GetNulls(t *testing.T) {
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
		want   []bool
	}{
		{"default values", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			[]bool{false, true},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			got := s.GetNulls()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GetNulls() = %v, want %v", got, tt.want)
			}
			got[0] = true
			if reflect.DeepEqual(got, s.GetNulls()) {
				t.Errorf("Series.GetNulls() retained reference to original, want copy")
			}
		})
	}
}

func TestSeries_GetValues(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   interface{}
	}{
		{"default values", fields{
			values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			[]string{"foo", "bar"},
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
			got := s.GetValues()
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GetValues() = %v, want %v", got, tt.want)
			}
			got.([]string)[0] = ""
			if reflect.DeepEqual(got, s.GetValues()) {
				t.Errorf("Series.GetValues() retained reference to original, want copy")
			}
		})
	}
}

func TestSeries_GetLabels(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   []interface{}
	}{
		{"default values", fields{
			values: &valueContainer{slice: []string{"foo", "bar"}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			[]interface{}{
				[]int{0, 1},
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
			if got := s.GetLabels(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.GetLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Resample(t *testing.T) {
	d := time.Date(2020, 2, 2, 12, 30, 45, 100, time.UTC)
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	type args struct {
		by Resampler
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"default - values", fields{
			values: &valueContainer{slice: []time.Time{d}, name: "foo", isNull: []bool{false}},
			labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}},
			args{Resampler{ByYear: true}},
			&Series{
				values: &valueContainer{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}, name: "foo", isNull: []bool{false}},
				labels: []*valueContainer{{slice: []float64{1}, name: "bar", isNull: []bool{false}}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.Resample(tt.args.by); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Resample() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_LabelsToSeries(t *testing.T) {
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
		{"pass",
			fields{values: &valueContainer{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
			args{"*0"},
			&Series{
				values:     &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "0"},
				labels:     []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				sharedData: true,
			},
		},
		{"fail",
			fields{values: &valueContainer{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
			args{"corge"},
			&Series{
				err: errors.New("LabelsToSeries(): name (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.LabelsToSeries(tt.args.name); !EqualSeries(got, tt.want) {
				t.Errorf("Series.LabelsToSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_ValueCounts(t *testing.T) {
	type fields struct {
		values *valueContainer
		labels []*valueContainer
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]int
	}{
		{"default values", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			map[string]int{"1": 1}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values: tt.fields.values,
				labels: tt.fields.labels,
				err:    tt.fields.err,
			}
			if got := s.ValueCounts(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.ValueCounts() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_ListLabelNames(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"false", fields{
			values: &valueContainer{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "qux"}}},
			[]string{"qux"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.ListLabelNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.ListLabelNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Unique(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		includeLabels bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"values only", fields{
			values: &valueContainer{slice: []float64{1, 1, 2, 1}, isNull: []bool{false, false, false, false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "qux"}}},
			args{includeLabels: false},
			&Series{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 2}, isNull: []bool{false, false}, name: "qux"}}},
		},
		{"values and labels", fields{
			values: &valueContainer{slice: []float64{1, 1, 2, 2}, isNull: []bool{false, false, false, false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 0, 2, 2}, isNull: []bool{false, false, false, false}, name: "qux"}}},
			args{includeLabels: true},
			&Series{
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 2}, isNull: []bool{false, false}, name: "qux"}}},
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
			if got := s.Unique(tt.args.includeLabels); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Unique() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_At(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		index int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Element
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{0},
			Element{Val: float64(1), IsNull: false},
		},
		{"out of range", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{1},
			Element{},
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
			if got := s.At(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.At() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Type(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			"[]float64",
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
			if got := s.Type(); got.String() != tt.want {
				t.Errorf("Series.Type() = %v, want %v", got.String(), tt.want)
			}
		})
	}
}

func TestSeries_IndexOfLabel(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{"qux"}, 0,
		},
		{"fail - missing", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{"corge"}, -1,
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
			if got := s.IndexOfLabel(tt.args.name); got != tt.want {
				t.Errorf("Series.IndexOfLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_IndexOfRows(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		name  string
		value interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{"foo", 1}, []int{0},
		},
		{"fail", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{"corge", 1}, nil,
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
			if got := s.IndexOfRows(tt.args.name, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("Series.IndexOfRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_SwapLabels(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		i string
		j string
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
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			}},
			args{"qux", "bar"},
			&Series{
				values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "qux"},
					{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				}},
		},
		{"fail - i", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			}},
			args{"corge", "bar"},
			&Series{
				err: errors.New("SwapLabels(): i: name (corge) not found")},
		},
		{"fail - j", fields{
			values: &valueContainer{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			}},
			args{"qux", "corge"},
			&Series{
				err: errors.New("SwapLabels(): j: name (corge) not found")},
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
			if got := s.SwapLabels(tt.args.i, tt.args.j); !EqualSeries(got, tt.want) {
				t.Errorf("Series.SwapLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Percentile(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"},
				},
			},
			&Series{
				values: &valueContainer{slice: []float64{0, 0, .5}, isNull: []bool{true, false, false}, name: "percentile"},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"},
				}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.Percentile(); !EqualSeries(got, tt.want) {
				t.Errorf("Series.Percentile() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_NUnique(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   int
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{0, 1, 2, 2}, isNull: []bool{true, false, false, false}, name: "foo"},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "qux"},
				},
			},
			2},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.NUnique(); got != tt.want {
				t.Errorf("Series.NUnique() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_InPlace(t *testing.T) {
	EnableWarnings()
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name    string
		fields  fields
		wantLog bool
	}{
		{"log", fields{
			values: &valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"},
			},
			sharedData: true,
		}, true},
		{"do not log", fields{
			values: &valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"},
			},
			sharedData: false,
		}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			b := new(bytes.Buffer)
			log.SetOutput(b)
			s.InPlace()
			if (b.String() != "") != tt.wantLog {
				t.Errorf("Series.InPlace() got log message %v, want %v", b.String(), tt.wantLog)
			}
		})
	}
	log.SetOutput(os.Stdout)
	DisableWarnings()
}

func TestSeries_stringFunc(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		stringFunction func([]string, []bool, []int) (string, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{func([]string, []bool, []int) (string, bool) { return "foo", false }},
			"foo"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.stringFunc(tt.args.stringFunction); got != tt.want {
				t.Errorf("Series.stringFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_FilterByValue(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		filters map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			values: &valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{map[string]interface{}{"foo": 0}},
			&Series{
				values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
		},
		{"fail", fields{
			values: &valueContainer{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{map[string]interface{}{"corge": 0}},
			&Series{
				err: errors.New("FilterByValue(): name (corge) not found")},
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
			if got := s.FilterByValue(tt.args.filters); !EqualSeries(got, tt.want) {
				t.Errorf("Series.FilterByValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_NameOfLabel(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		n int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   string
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			args{0},
			"qux"},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			s := &Series{
				values:     tt.fields.values,
				labels:     tt.fields.labels,
				sharedData: tt.fields.sharedData,
				err:        tt.fields.err,
			}
			if got := s.NameOfLabel(tt.args.n); got != tt.want {
				t.Errorf("Series.NameOfLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_Iterator(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	tests := []struct {
		name   string
		fields fields
		want   *SeriesIterator
	}{
		{"pass",
			fields{
				values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}}},
			&SeriesIterator{
				current: -1,
				s: &Series{values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
					labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
				}},
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
			got := s.Iterator()
			if got.current != tt.want.current {
				t.Errorf("Series.Iterator() = %v, want %v", got.current, tt.want.current)
			}
			if !EqualSeries(got.s, tt.want.s) {
				t.Errorf("Series.Iterator() = %v, want %v", got.s, tt.want.s)
			}
		})
	}
}

func TestSeriesIterator_Next(t *testing.T) {
	type fields struct {
		current int
		s       *Series
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"not at end", fields{
			current: -1,
			s: &Series{values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
			}},
			true,
		},
		{"at end", fields{
			current: 0,
			s: &Series{values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
			}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := &SeriesIterator{
				current: tt.fields.current,
				s:       tt.fields.s,
			}
			if got := iter.Next(); got != tt.want {
				t.Errorf("SeriesIterator.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeriesIterator_Row(t *testing.T) {
	type fields struct {
		current int
		s       *Series
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]Element
	}{
		{"pass",
			fields{0, &Series{values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
			}},
			map[string]Element{"foo": {float64(0), false}, "qux": {int(0), false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := &SeriesIterator{
				current: tt.fields.current,
				s:       tt.fields.s,
			}
			if got := iter.Row(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("SeriesIterator.Row() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestSeries_HasLabels(t *testing.T) {
	type fields struct {
		values     *valueContainer
		labels     []*valueContainer
		sharedData bool
		err        error
	}
	type args struct {
		labelNames []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"pass",
			fields{values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
			},
			args{[]string{"qux"}},
			false,
		},
		{"fail",
			fields{values: &valueContainer{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
			},
			args{[]string{"corge"}},
			true,
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
			if err := s.HasLabels(tt.args.labelNames...); (err != nil) != tt.wantErr {
				t.Errorf("Series.HasLabels() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}
