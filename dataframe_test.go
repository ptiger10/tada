package tada

import (
	"bytes"
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/ptiger10/tablediff"
)

func TestMakeMultiLevelLabels(t *testing.T) {
	type args struct {
		values []interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{"pass", args{values: []interface{}{
			[]string{"foo", "bar"},
			[]float64{1, 2, 3}}},
			[]interface{}{
				[]string{"foo", "foo", "foo", "bar", "bar", "bar"},
				[]float64{1, 2, 3, 1, 2, 3},
			}, false},
		{"pass", args{values: []interface{}{
			[]float64{1, 2, 3},
			[]string{"foo", "bar"}}},
			[]interface{}{
				[]float64{1, 1, 2, 2, 3, 3},
				[]string{"foo", "bar", "foo", "bar", "foo", "bar"},
			}, false},
		{"fail - not slice", args{[]interface{}{"foo"}}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := MakeMultiLevelLabels(tt.args.values)
			if (err != nil) != tt.wantErr {
				t.Errorf("MakeMultiLevelLabels() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("MakeMultiLevelLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

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
		{"pass - supplied values and labels", args{
			[]interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
			[]interface{}{[]string{"a", "b"}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"pass - default labels", args{
			[]interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
			nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"pass - default values", args{
			nil,
			[]interface{}{[]string{"a", "b"}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "0"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail - slices and labels nil", args{nil, nil},
			&DataFrame{
				err: errors.New("NewSeries(): `slices` and `labels` cannot both be nil")},
		},
		{"fail - unsupported kind", args{
			[]interface{}{"foo"}, nil},
			&DataFrame{
				err: errors.New("NewDataFrame(): `slices`: error at position 0: unsupported kind (string); must be slice")},
		},
		{"fail - unsupported label kind", args{
			[]interface{}{[]float64{1}}, []interface{}{"foo"}},
			&DataFrame{
				err: errors.New("NewDataFrame(): `labels`: error at position 0: unsupported kind (string); must be slice")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := NewDataFrame(tt.args.slices, tt.args.labels...); !EqualDataFrames(got, tt.want) {
				t.Errorf("NewDataFrame() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Cast(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		colAsType map[string]DType
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass - set cache", fields{
			values: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "qux"},
			args{map[string]DType{"foo": Float64, "bar": String}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"1"}, isNull: []bool{false}, name: "bar", cache: []string{"1"}}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "qux"},
		},
		{"fail", fields{
			values: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "qux"},
			args{map[string]DType{"corge": Float64}},
			&DataFrame{
				err: fmt.Errorf("Cast(): `name` (corge) not found")},
		},
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
			df.Cast(tt.args.colAsType)
			if !EqualDataFrames(df, tt.want) {
				t.Errorf("DataFrame.Cast() -> %v, want %v", df, tt.want)
			}
		})
	}
}

func TestDataFrame_ToSeries(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"}},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
			&Series{
				values:     &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				labels:     []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				sharedData: true},
		},
		{"fail: two columns", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"}},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
			&Series{
				err: fmt.Errorf("ToSeries(): DataFrame must have a single column")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.ToSeries(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.ToSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Copy(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:   "baz", colLevelNames: []string{"*0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:   "baz", colLevelNames: []string{"*0"}},
		},
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
			got := df.Copy()
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Copy() = %v, want %v", got, tt.want)
			}
			gotDistinct := got.Copy()
			if !dataFrameEqualsDistinct(gotDistinct, got) {
				t.Errorf("DataFrame.Copy() retained reference to original")
			}
		})
	}
}

func TestDataFrame_Subset(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]int{0}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"fail - no matching index", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]int{10}},
			&DataFrame{err: fmt.Errorf(
				"Subset(): index out of range (10 > 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				colLevelNames: tt.fields.colLevelNames,
				name:          tt.fields.name,
				err:           tt.fields.err,
			}
			if got := df.Subset(tt.args.index); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Subset() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestReadCSV(t *testing.T) {
	type args struct {
		csv    [][]string
		config []func(*readConfig)
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header row, 2 columns, no index",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: nil},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"1 header row, 2 columns, no index, nil config",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: nil},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"column as major dimension",
			args{
				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6"}},
				config: []func(*readConfig){ReadOptionSwitchDims()}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"fail - no rows",
			args{csv: nil,
				config: nil},
			nil,
			true},
		{"fail - no columns",
			args{csv: [][]string{{}},
				config: nil},
			nil,
			true},
		{"fail - misaligned",
			args{csv: [][]string{{"foo"}, {"bar", "baz"}},
				config: nil},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadCSV(tt.args.csv, tt.args.config...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ReadCSV() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadCSVFromString(t *testing.T) {
	type args struct {
		data    string
		options []func(*readConfig)
	}
	tests := []struct {
		name    string
		args    args
		wantRet *DataFrame
		wantErr bool
	}{
		{"pass",
			args{"foo, bar\n 0, 1", nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"0"}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"1"}, isNull: []bool{false}, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - rows - misaligned",
			args{"foo\n bar, baz", nil},
			nil,
			true,
		},
		{"fail - columns - misaligned",
			args{"foo\n bar, baz", []func(*readConfig){ReadOptionSwitchDims()}},
			nil,
			true,
		},
		{"fail - empty",
			args{"", nil},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRet, err := ReadCSVFromString(tt.args.data, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCSVFromString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(gotRet, tt.wantRet) {
				t.Errorf("ReadCSVFromString() = %v, want %v", gotRet, tt.wantRet)
			}
		})
	}
}

func TestDataFrame_SubsetLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]int{1}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{10}, isNull: []bool{false}, name: "*10"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"fail - bad index", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]int{10}},
			&DataFrame{err: fmt.Errorf("SubsetLabels(): index out of range (10 > 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				colLevelNames: tt.fields.colLevelNames,
				name:          tt.fields.name,
				err:           tt.fields.err,
			}
			if got := df.SubsetLabels(tt.args.index); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SubsetLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SubsetCols(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]int{1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []int{10}, isNull: []bool{false}, name: "*10"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"fail - bad index", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]int{10}},
			&DataFrame{err: fmt.Errorf("SubsetCols(): index out of range (10 > 1)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				colLevelNames: tt.fields.colLevelNames,
				name:          tt.fields.name,
				err:           tt.fields.err,
			}
			if got := df.SubsetCols(tt.args.index); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SubsetCols() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Head(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		n int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{2},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "0"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:   "baz"}},
		{"overwrite n", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{5},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
				name:   "baz"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Head(tt.args.n); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Head() = %v, want %v", got, tt.want)
			}
		})
	}
}
func TestDataFrame_Tail(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		n int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{2},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "0"}},
				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}, name: "*0"}},
				name:   "baz"}},
		{"overwrite n", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{20},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
				name:   "baz"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Tail(tt.args.n); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Tail() = %v, want %v", got.labels[0], tt.want.labels[0])
			}
		})
	}
}
func TestDataFrame_Range(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
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
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{1, 2},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"bar"}, isNull: []bool{false}, name: "0"}},
				labels: []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				name:   "baz"}},
		{"fail - first > last", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{3, 2},
			&DataFrame{err: fmt.Errorf("Range(): first is greater than last (3 > 2)")}},
		{"fail - first out of range", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{3, 3},
			&DataFrame{err: fmt.Errorf("Range(): first index out of range (3 > 2)")}},
		{"fail - last out of range", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			name: "baz"},
			args{2, 4},
			&DataFrame{err: fmt.Errorf("Range(): last index out of range (4 > 3)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Range(tt.args.first, tt.args.last); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Range() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_FilterCols(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	type args struct {
		lambda func(string) bool
		level  int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
					{slice: []float64{1}, isNull: []bool{false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{func(s string) bool {
				if strings.Contains(s, "ba") {
					return true
				}
				return false
			}, 0},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
					{slice: []float64{1}, isNull: []bool{false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail - out of range", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				{slice: []float64{1}, isNull: []bool{false}, name: "baz"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}}},
			args{func(s string) bool {
				if strings.Contains(s, "ba") {
					return true
				}
				return false
			}, 10},
			&DataFrame{
				err: fmt.Errorf("FilterCols(): `level` out of range: 10 >= 0")},
		},
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
			if got := df.FilterCols(tt.args.lambda, tt.args.level); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.FilterCols() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_WithCol(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		name  string
		input interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"rename column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"foo", "qux"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name: "bar"},
		},
		{"replace column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"foo", []float64{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{10}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name: "bar"},
		},
		{"append column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"baz", []float64{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{10}, isNull: []bool{false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name: "bar"},
		},
		{"replace with Series", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"baz", &Series{
				values: &valueContainer{slice: []float64{10}, isNull: []bool{false}, name: "baz"},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{10}, isNull: []bool{false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name: "bar"},
		},
		{"fail - bad input", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"baz", []complex64{10}},
			&DataFrame{err: fmt.Errorf("WithCol(): unable to calculate null values ([]complex64 not supported)")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.WithCol(tt.args.name, tt.args.input); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.WithCol() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestDataFrame_WithLabels(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		name  string
		input interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"rename column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"*0", "qux"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
				name: "bar"},
		},
		{"replace column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"*0", []int{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{10}, isNull: []bool{false}, name: "*0"}},
				name: "bar"},
		},
		{"append column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"baz", []float64{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []float64{10}, isNull: []bool{false}, name: "baz"}},
				name: "bar"},
		},
		{"fail - bad input", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "bar"},
			args{"baz", []complex64{10}},
			&DataFrame{err: fmt.Errorf("WithLabels(): unable to calculate null values ([]complex64 not supported)")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.WithLabels(tt.args.name, tt.args.input); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.WithLabels() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestDataFrame_DropNull(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		subset []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"all", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{nil},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{2}, isNull: []bool{false}, name: "0"},
				{slice: []string{"bar"}, isNull: []bool{false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{2}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"subset", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]string{"0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"", "bar"}, isNull: []bool{true, false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"fail - bad column", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]string{"corge"}},
			&DataFrame{err: fmt.Errorf("DropNull(): `name` (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				colLevelNames: tt.fields.colLevelNames,
				name:          tt.fields.name,
				err:           tt.fields.err,
			}
			if got := df.DropNull(tt.args.subset...); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.DropNull() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestDataFrame_Null(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		subset []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"all", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{nil},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "0"},
				{slice: []string{"foo", ""}, isNull: []bool{false, true}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"subset", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]string{"0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{true}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"fail - bad column", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]string{"corge"}},
			&DataFrame{
				err: fmt.Errorf("Null(): `name` (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				colLevelNames: tt.fields.colLevelNames,
				name:          tt.fields.name,
				err:           tt.fields.err,
			}
			if got := df.Null(tt.args.subset...); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Null() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SetLabels(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"bar"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				name: "baz"}},
		{"fail - too many columns listed", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"bar", "foo"}},
			&DataFrame{
				err: fmt.Errorf("SetLabels(): number of colNames must be less than number of columns (2 >= 2)")},
		},
		{"fail - no matching col", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"corge"}},
			&DataFrame{
				err: fmt.Errorf("SetLabels(): `name` (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.SetLabels(tt.args.colNames...); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SetLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ResetLabels(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		index []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass - supplied level", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{1}, isNull: []bool{false}, name: "*1"}},
			name: "baz"},
			args{[]int{1}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "1"},
			},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name: "baz"}},
		{"pass - all levels", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{1}, isNull: []bool{false}, name: "*1"}},
			name: "baz"},
			args{nil},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{0}, isNull: []bool{false}, name: "0"},
				{slice: []int{1}, isNull: []bool{false}, name: "1"},
			},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name: "baz"}},
		{"fail - out of range ", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{1}, isNull: []bool{false}, name: "*1"}},
			name: "baz"},
			args{[]int{10}},
			&DataFrame{err: fmt.Errorf("ResetLabels(): index out of range (10 > 2)")}},
		{"fail - out of range after adjustment ", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{1}, isNull: []bool{false}, name: "*1"}},
			name: "baz"},
			args{[]int{1, 10}},
			&DataFrame{err: fmt.Errorf("ResetLabels(): index out of range (10 > 2)")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.ResetLabels(tt.args.index...); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.ResetLabels() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestDataFrame_Name(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name: "baz"},
			"baz"},
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
			if got := df.Name(); got != tt.want {
				t.Errorf("DataFrame.Name() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Relabel(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "baz"},
				{slice: []float64{1}, isNull: []bool{false}, name: "baz"}},
			colLevelNames: []string{"*0"}},
			&DataFrame{
				values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
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
			if got := df.Relabel(); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Relabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SetLabelNames(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"bar"}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "bar"}},
				name:          "baz",
				colLevelNames: []string{"*0"}},
		},
		{"fail - too many", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"bar", "qux"}},
			&DataFrame{
				err: fmt.Errorf("SetLabelNames(): number of `levelNames` must match number of levels in DataFrame (2 != 1)")},
		},
		{"fail - too few", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []float64{1}, isNull: []bool{false}, name: "*1"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"qux"}},
			&DataFrame{
				err: fmt.Errorf("SetLabelNames(): number of `levelNames` must match number of levels in DataFrame (1 != 2)")},
		},
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
			if got := df.SetLabelNames(tt.args.colNames); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SetLabelNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SetColNames(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"bar"}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:          "baz",
				colLevelNames: []string{"*0"}},
		},
		{"fail - too many", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"bar", "qux"}},
			&DataFrame{
				err: fmt.Errorf("SetColNames(): number of `colNames` must match number of columns in DataFrame (2 != 1)")},
		},
		{"fail - too few", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"qux"}},
			&DataFrame{
				err: fmt.Errorf("SetColNames(): number of `colNames` must match number of columns in DataFrame (1 != 2)")},
		},
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
			if got := df.SetColNames(tt.args.colNames); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SetColNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Filter(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		filters map[string]FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"float and string intersection", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{
				"foo": {Float64: func(val float64) bool {
					if val > 1 {
						return true
					}
					return false
				}},
				"bar": {String: func(val string) bool {
					if strings.Contains(val, "a") {
						return true
					}
					return false
				}},
			}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{2}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"bar"}, isNull: []bool{false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{2}, isNull: []bool{false}, name: "*0"}}},
		},
		{"no matches", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{
				"foo": {Float64: func(val float64) bool {
					return val >= 10
				}},
			}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{}, isNull: []bool{}, name: "foo"},
					{slice: []string{}, isNull: []bool{}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{}, isNull: []bool{}, name: "*0"}}},
		},
		{"no filters - all rows", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
					{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
		},
		{"fail - empty filter", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{2, 3, 4}, isNull: []bool{false, false, false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{"*0": {}}},
			&DataFrame{err: fmt.Errorf("Filter(): no filter function provided")}},
		{"fail - bad column name", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{2, 3, 4}, isNull: []bool{false, false, false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{map[string]FilterFn{"corge": {Float64: func(float64) bool { return true }}}},
			&DataFrame{err: fmt.Errorf("Filter(): `name` (corge) not found")}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Filter(tt.args.filters); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Filter() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Apply(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		lambdas map[string]ApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"float64", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{map[string]ApplyFn{"foo": {Float64: func(v float64) float64 {
				return v + 1
			}}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
					{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:   "baz"},
		},
		{"fail - no function", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{map[string]ApplyFn{"foo": {}}},
			&DataFrame{
				err: fmt.Errorf("Apply(): no apply function provided")},
		},
		{"fail - no matching column", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{map[string]ApplyFn{"corge": {Float64: func(float64) float64 { return 0 }}}},
			&DataFrame{
				err: fmt.Errorf("Apply(): `name` (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Apply(tt.args.lambdas); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ApplyFormat(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		lambdas map[string]ApplyFormatFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"float64", fields{
			values: []*valueContainer{
				{slice: []float64{.51}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{map[string]ApplyFormatFn{"foo": {Float64: func(v float64) string {
				return strconv.FormatFloat(v, 'f', 1, 64)
			}}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"0.5"}, isNull: []bool{false}, name: "foo"},
					{slice: []int{1}, isNull: []bool{false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				name:          "baz",
				colLevelNames: []string{"*0"}},
		},
		{"fail - no function", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{map[string]ApplyFormatFn{"foo": {}}},
			&DataFrame{
				err: fmt.Errorf("ApplyFormat(): no apply function provided")},
		},
		{"fail - no matching column", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{map[string]ApplyFormatFn{"corge": {Float64: func(float64) string { return "" }}}},
			&DataFrame{
				err: fmt.Errorf("ApplyFormat(): `name` (corge) not found")},
		},
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
			if got := df.ApplyFormat(tt.args.lambdas); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.ApplyFormat() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Sort(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		by []Sorter
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"float64 on one column - ascending", fields{
			values: []*valueContainer{
				{slice: []float64{0, 2, 1}, isNull: []bool{false, false, false}, name: "foo"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]Sorter{{Name: "foo", Descending: false}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 2, 1}, isNull: []bool{false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"fail - no Sorters", fields{
			values: []*valueContainer{
				{slice: []float64{0, 2, 1}, isNull: []bool{false, false, false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{nil},
			&DataFrame{
				err: fmt.Errorf("Sort(): must supply at least one Sorter")},
		},
		{"fail - bad colName", fields{
			values: []*valueContainer{
				{slice: []float64{0, 2, 1}, isNull: []bool{false, false, false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{[]Sorter{{Name: "corge"}}},
			&DataFrame{
				err: fmt.Errorf("Sort(): position 0: `name` (corge) not found")},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels:        tt.fields.labels,
				values:        tt.fields.values,
				colLevelNames: tt.fields.colLevelNames,
				name:          tt.fields.name,
				err:           tt.fields.err,
			}
			if got := df.Sort(tt.args.by...); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Sort() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_IterRows(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   []map[string]Element
	}{
		{"single label level, named values", fields{
			values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"}},
			labels: []*valueContainer{{name: "*0", slice: []string{"bar", ""}, isNull: []bool{false, true}}}},
			[]map[string]Element{
				{"foo": Element{float64(1), false}, "*0": Element{"bar", false}},
				{"foo": Element{float64(2), false}, "*0": Element{"", true}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.IterRows(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.IterRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Sum(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
			name:   "corge",
		},
			&Series{
				values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Sum(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Sum() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Mean(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
			name:   "corge",
		},
			&Series{
				values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "mean"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Mean(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Mean() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestDataFrame_Median(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 5}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{3, 4, 6}, isNull: []bool{false, false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "baz"}},
			name:   "corge",
		},
			&Series{
				values: &valueContainer{slice: []float64{2, 4}, isNull: []bool{false, false}, name: "median"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Median(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Median() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Std(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
			name:   "corge",
		},
			&Series{
				values: &valueContainer{slice: []float64{.5, .5}, isNull: []bool{false, false}, name: "std"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Std(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Std() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Count(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
		},
			&Series{
				values: &valueContainer{slice: []int{2, 2}, isNull: []bool{false, false}, name: "count"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Count(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Count() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Min(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
		},
			&Series{
				values: &valueContainer{slice: []float64{1, 3}, isNull: []bool{false, false}, name: "min"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Min(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Min() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Max(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
		},
			&Series{
				values: &valueContainer{slice: []float64{2, 4}, isNull: []bool{false, false}, name: "max"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
			}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Max(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Max() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Merge(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		other *DataFrame
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"matching label key *0",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values:        []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"anything"}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "bar"},
				},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0",
						cache: []string{"0", "1"}},
				},
				colLevelNames: []string{"*0"}},
		},
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
			if got := df.Merge(tt.args.other); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_LookupAdvanced(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	type args struct {
		other   *DataFrame
		how     string
		leftOn  []string
		rightOn []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"single label level, named keys, left join - other has more labels", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
		},
		{"single label level, named keys, left join - other has fewer labels", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
		},
		{"auto key match", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: nil, rightOn: nil},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
		},
		{"fail - leftOn but not rightOn", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: nil},
			&DataFrame{err: fmt.Errorf("LookupAdvanced(): if either leftOn or rightOn is empty, both must be empty")},
		},
		{"fail - bad leftOn", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"corge"}, rightOn: []string{"foo"}},
			&DataFrame{err: fmt.Errorf("LookupAdvanced(): `leftOn`: `name` (corge) not found")},
		},
		{"fail - bad rightOn", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "baz"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"corge"}},
			&DataFrame{err: fmt.Errorf("LookupAdvanced(): `rightOn`: `name` (corge) not found")},
		},
		{"fail - unsupported lookup", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "special",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&DataFrame{err: fmt.Errorf("LookupAdvanced(): `how`: must be `left`, `right`, or `inner`")},
		},
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
			if got := df.LookupAdvanced(tt.args.other, tt.args.how, tt.args.leftOn, tt.args.rightOn); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.LookupAdvanced() = %v, want %v", got, tt.want)

			}
		})
	}
}

func TestDataFrame_Transpose(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrame
	}{
		{"single column",
			fields{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
				labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1"}, isNull: []bool{false}, name: "bar"},
				{slice: []string{"2"}, isNull: []bool{false}, name: "baz"}},
				labels:        []*valueContainer{{slice: []string{"waldo"}, isNull: []bool{false}, name: "*0"}},
				name:          "qux",
				colLevelNames: []string{"foo"}}},
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
			if got := df.Transpose(); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Transpose() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_GroupBy(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		names []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *GroupedDataFrame
	}{
		{"group by all levels, with repeats", fields{
			values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}}},
			labels: []*valueContainer{
				{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
				{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
			}},
			args{nil},
			&GroupedDataFrame{
				orderedKeys: []string{"0|foo", "1|foo", "2|bar"},
				rowIndices:  [][]int{{0, 1}, {2}, {3}},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "a"},
					{slice: []string{"foo", "foo", "bar"}, isNull: []bool{false, false, false}, name: "b"},
				},
				df: &DataFrame{
					values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}}},
					labels: []*valueContainer{
						{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a",
							cache: []string{"0", "0", "1", "2"}},
						{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b",
							cache: []string{"foo", "foo", "foo", "bar"}},
					}},
			},
		},
		{"fail - no matching column", fields{
			values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}}},
			labels: []*valueContainer{
				{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
				{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
			}},
			args{[]string{"corge"}},
			&GroupedDataFrame{
				err: fmt.Errorf("GroupBy(): `name` (corge) not found"),
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.GroupBy(tt.args.names...); !equalGroupedDataFrames(got, tt.want) {
				t.Errorf("DataFrame.GroupBy() = %v, want %v", got, tt.want)

			}
		})
	}
}

func TestDataFrame_PromoteToColLevel(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"stack column - string", fields{
			values: []*valueContainer{
				{slice: []int{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			},
			colLevelNames: []string{"*0"},
		}, args{"year"},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "2018|foo"},
					{slice: []string{"", "", "c", "d"}, isNull: []bool{true, true, false, false}, name: "2019|foo"}},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
				},
				colLevelNames: []string{"year", "*0"},
			}},
		{"stack column - nulls", fields{
			values: []*valueContainer{
				{slice: []int{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"a", "b", "c", "n/a"}, isNull: []bool{false, false, false, true}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			},
			colLevelNames: []string{"*0"},
		}, args{"year"},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "2018|foo"},
					{slice: []string{"", "", "c", "n/a"}, isNull: []bool{true, true, false, true}, name: "2019|foo"}},
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
				},
				colLevelNames: []string{"year", "*0"},
			}},
		{"stack labels with repeats - int", fields{
			values: []*valueContainer{
				{slice: []int{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []string{"A", "B", "B", "C"}, isNull: []bool{false, false, false, false}, name: "bar"},
				{slice: []int{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
			},
			colLevelNames: []string{"*0"},
		}, args{"year"},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{1, 2, 0}, isNull: []bool{false, false, true}, name: "2018|foo"},
					{slice: []int{0, 3, 4}, isNull: []bool{true, false, false}, name: "2019|foo"}},
				labels: []*valueContainer{
					{slice: []string{"A", "B", "C"}, isNull: []bool{false, false, false}, name: "bar"},
				},
				colLevelNames: []string{"year", "*0"},
			}},
		{"fail - no matching name", fields{
			values: []*valueContainer{
				{slice: []int{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []string{"A", "B", "B", "C"}, isNull: []bool{false, false, false, false}, name: "bar"},
				{slice: []int{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
			},
			colLevelNames: []string{"*0"},
		}, args{"corge"},
			&DataFrame{
				err: fmt.Errorf("PromoteToColLevel(): `name` (corge) not found")},
		},
		{"fail - only column", fields{
			values: []*valueContainer{
				{slice: []int{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []string{"A", "B", "B", "C"}, isNull: []bool{false, false, false, false}, name: "bar"},
				{slice: []int{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
			},
			colLevelNames: []string{"*0"},
		}, args{"foo"},
			&DataFrame{
				err: fmt.Errorf("PromoteToColLevel(): cannot stack only column")},
		},
		{"fail - only label level", fields{
			values: []*valueContainer{
				{slice: []int{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []string{"A", "B", "B", "C"}, isNull: []bool{false, false, false, false}, name: "bar"},
			},
			colLevelNames: []string{"*0"},
		}, args{"bar"},
			&DataFrame{
				err: fmt.Errorf("PromoteToColLevel(): cannot stack only label level")},
		},
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
			if got := df.PromoteToColLevel(tt.args.name); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.PromoteToColLevel() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestDataFrame_PivotTable(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		labels  string
		columns string
		values  string
		aggFn   string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"sum", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "sum"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 7}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "sum_foo"}},
		{"mean", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "mean"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 3.5}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "mean_foo"}},
		{"median", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "median"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 3.5}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "median_foo"}},
		{"std", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "std"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{0, 0}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 0.5}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "std_foo"}},
		{"count", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo",
		},
			args{labels: "type", columns: "year", values: "amount", aggFn: "count"},
			&DataFrame{values: []*valueContainer{
				{slice: []int{1, 1}, isNull: []bool{false, false}, name: "2018"},
				{slice: []int{0, 2}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "count_foo"}},
		{"min", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "min"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 3}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "min_foo"}},
		{"max", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "max"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 4}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "max_foo"}},
		{"fail - no matching index level", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "corge", columns: "year", values: "amount", aggFn: "std"},
			&DataFrame{
				err: fmt.Errorf("PivotTable(): `labels`: `name` (corge) not found")}},
		{"fail - no matching columns", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "corge", values: "amount", aggFn: "std"},
			&DataFrame{
				err: fmt.Errorf("PivotTable(): `columns`: `name` (corge) not found")}},
		{"fail - no matching values", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "year", values: "corge", aggFn: "std"},
			&DataFrame{
				err: fmt.Errorf("PivotTable(): `values`: `name` (corge) not found")}},
		{"fail - unsupported aggfunc", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "year", values: "amount", aggFn: "other"},
			&DataFrame{
				err: fmt.Errorf("PivotTable(): `aggFunc`: unsupported (other)")}},
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
			if got := df.PivotTable(tt.args.labels, tt.args.columns, tt.args.values, tt.args.aggFn); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.PivotTable() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_dropColLevel(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		level int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values:        []*valueContainer{{name: "foo|bar"}},
			colLevelNames: []string{"qux", "quux"}},
			args{1},
			&DataFrame{
				values:        []*valueContainer{{name: "foo"}},
				colLevelNames: []string{"qux"},
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
			df.dropColLevel(tt.args.level)
		})
	}
}

type testMatrix struct {
	values [][]float64
}

func (mat testMatrix) Dims() (r, c int) {
	return len(mat.values), len(mat.values[0])
}

func (mat testMatrix) At(i, j int) float64 {
	return mat.values[i][j]
}

func TestReadMatrix(t *testing.T) {
	type args struct {
		mat Matrix
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{name: "matrix with same signature as gonum mat/matrix",
			args: args{mat: testMatrix{values: [][]float64{{1, 2}}}},
			want: &DataFrame{
				values: []*valueContainer{
					{slice: []string{"1"}, isNull: []bool{false}, name: "0"},
					{slice: []string{"2"}, isNull: []bool{false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := ReadMatrix(tt.args.mat); !EqualDataFrames(got, tt.want) {
				t.Errorf("ReadMatrix() = %v, want %v", got, tt.want)
			}
		})
	}
}

type testStruct struct {
	Name string
	Age  int
}

func TestReadStruct(t *testing.T) {
	type args struct {
		slice interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"pass", args{[]testStruct{{"foo", 1}, {"bar", 2}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "Name"},
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "Age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - bad input", args{"foo"},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadStruct(tt.args.slice)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ReadStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestWriteMockCSV(t *testing.T) {
	want1 := `corge,qux
.5,foo
.9,baz
.5,foo
`
	randSeed = 3
	type args struct {
		src        [][]string
		outputRows int
		config     []func(*readConfig)
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		wantErr bool
	}{
		{"pass", args{src: [][]string{{"corge", "qux"}, {"1.5", "foo"}, {"2.5", "foo"}}, config: nil, outputRows: 3},
			want1, false},
		{"fail - no rows", args{src: nil, config: nil, outputRows: 3},
			"", true},
		{"fail - no cols", args{src: [][]string{{}}, config: nil, outputRows: 3},
			"", true},
		{"columns as major dim",
			args{src: [][]string{{"corge", "1.5", "2.5"}, {"qux", "foo", "foo"}},
				outputRows: 3,
				config:     []func(*readConfig){ReadOptionSwitchDims()}},
			want1, false},
		{"fail - no rows", args{src: nil, outputRows: 3},
			"", true},
		{"fail - no cols", args{src: [][]string{{}}, outputRows: 3},
			"", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			if err := WriteMockCSV(tt.args.src, w, tt.args.outputRows, tt.args.config...); (err != nil) != tt.wantErr {
				t.Errorf("WriteMockCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotW := w.String(); gotW != tt.wantW {
				t.Errorf("WriteMockCSV() = %v, want %v", gotW, tt.wantW)
			}
		})
	}
}

func TestDataFrame_ListColNames(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"pass", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo|bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			[]string{"foo|bar"}},
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
			if got := df.ListColNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ListColNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ListColNamesAtLevel(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		level int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []string
	}{
		{"pass", fields{
			values:        []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo|bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0", "*1"},
		},
			args{0},
			[]string{"foo"}},
		{"fail", fields{
			values:        []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo|bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0", "*1"},
			name:          "baz"},
			args{10},
			nil},
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
			if got := df.ListColNamesAtLevel(tt.args.level); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ListColNamesAtLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ListLabelNames(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []string
	}{
		{"pass", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			[]string{"*0"}},
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
			if got := df.ListLabelNames(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ListLabelNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_HasCols(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		colNames []string
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"foo", "bar"}}, false},
		{"fail", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"foo", "corge"}}, true},
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
			if err := df.HasCols(tt.args.colNames...); (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.HasCols() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataFrame_EqualsCSV(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		csv          [][]string
		ignoreLabels bool
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
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args:    args{[][]string{{"*0", "foo"}, {"0", "1"}}, false},
			want:    true,
			want1:   nil,
			wantErr: false},
		{name: "pass - ignore labels",
			fields: fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args:    args{[][]string{{"foo"}, {"1"}}, true},
			want:    true,
			want1:   nil,
			wantErr: false},
		{name: "fail - misaligned",
			fields: fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args:    args{[][]string{{"*0", "foo"}, {"0"}}, false},
			want:    false,
			want1:   nil,
			wantErr: true},
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
			got, got1, err := df.EqualsCSV(tt.args.csv, tt.args.ignoreLabels)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.EqualsCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DataFrame.EqualsCSV() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("DataFrame.EqualsCSV() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func TestDataFrame_EqualsCSVString(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		data         string
		ignoreLabels bool
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
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args:    args{"*0, foo\n 0, 1", false},
			want:    true,
			want1:   nil,
			wantErr: false},
		{name: "pass - ignore labels",
			fields: fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args:    args{"foo\n 1", true},
			want:    true,
			want1:   nil,
			wantErr: false},
		{name: "fail - misaligned",
			fields: fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args:    args{"*0, foo\n 0, 1, 2", false},
			want:    false,
			want1:   nil,
			wantErr: true},
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
			got, got1, err := df.EqualsCSVFromString(tt.args.data, tt.args.ignoreLabels)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.EqualsCSVString() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DataFrame.EqualsCSVString() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("DataFrame.EqualsCSV() got1 = %#v, want %#v", got1, tt.want1)
			}
		})
	}
}
func TestImportCSV(t *testing.T) {
	type args struct {
		path   string
		config []func(*readConfig)
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header, 0 labels - nil config",
			args{"test_files/1_header_0_labels.csv", nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "Name"},
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "Age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}}, false},
		{"fail - no file",
			args{"missing.csv", nil},
			nil, true},
		{"fail - bad delimiter",
			args{"test_files/bad_delimiter.csv", nil},
			nil, true},
		{"fail - empty",
			args{"test_files/empty.csv", nil},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ImportCSV(tt.args.path, tt.args.config...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ImportCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ImportCSV() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestReadInterface(t *testing.T) {
	type args struct {
		data   [][]interface{}
		config []func(*readConfig)
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header row, 2 columns, no index",
			args{
				data:   [][]interface{}{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: nil},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"1 header row, 2 columns, no index, nil config",
			args{
				data:   [][]interface{}{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: nil},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"column as major dimension",
			args{
				data:   [][]interface{}{{"foo", "1", "2"}, {"bar", "5", "6"}},
				config: []func(*readConfig){ReadOptionSwitchDims()}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"fail - no rows",
			args{data: nil,
				config: nil},
			nil, true},
		{"fail - no columns",
			args{data: [][]interface{}{{}},
				config: nil},
			nil, true},
		{"fail - misaligned",
			args{data: [][]interface{}{{"foo"}, {"bar", "baz"}},
				config: nil},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadInterface(tt.args.data, tt.args.config...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadInterface() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ReadInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ToCSV(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{false},
			[][]string{{"*0", "foo"}, {"0", "a"}, {"1", "b"}}, false},
		{"fail",
			fields{values: nil,
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{false},
			nil, true},
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
			got := df.ToCSV(tt.args.ignoreLabels)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ToCSV() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ExportCSV(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		file         string
		ignoreLabels bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		wantErr bool
	}{
		{"pass", fields{values: []*valueContainer{
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"test_files/output.csv", false}, false},
		{"fail - no df", fields{values: nil,
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"test_files/output.csv", false}, true},
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
			if err := df.ExportCSV(tt.args.file, tt.args.ignoreLabels); (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.ExportCSV() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataFrame_ToInterface(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		ignoreLabels bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    [][]interface{}
		wantErr bool
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{false},
			[][]interface{}{{"*0", "foo"}, {"0", "a"}, {"1", "b"}}, false},
		{"fail",
			fields{values: nil,
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{false},
			nil, true},
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
			got := df.ToInterface(tt.args.ignoreLabels)

			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ToInterface() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Err(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"pass",
			fields{err: fmt.Errorf("foo")},
			true},
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
			if err := df.Err(); (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.Err() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestDataFrame_XS(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		filters map[string]interface{}
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{
				values:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{map[string]interface{}{"foo": "a"}},
			&DataFrame{
				values:        []*valueContainer{{slice: []string{"a"}, isNull: []bool{false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail",
			fields{
				values:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{map[string]interface{}{"corge": "a"}},
			&DataFrame{
				err: fmt.Errorf("XS(): `name` (corge) not found")},
		},
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
			if got := df.XS(tt.args.filters); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.XS() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SelectLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"*0"},
			&Series{
				values:     &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "0"},
				labels:     []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				sharedData: true,
			},
		},
		{"fail",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"corge"},
			&Series{
				err: fmt.Errorf("SelectLabels(): `name` (corge) not found")},
		},
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
			if got := df.SelectLabels(tt.args.name); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.SelectLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Col(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"foo"},
			&Series{
				values:     &valueContainer{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
				labels:     []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				sharedData: true,
			},
		},
		{"fail",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"corge"},
			&Series{
				err: fmt.Errorf("Col(): `name` (corge) not found")},
		},
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
			if got := df.Col(tt.args.name); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Col() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Cols(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		names []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{[]string{"foo"}},
			&DataFrame{
				values: []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			},
		},
		{"fail",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{[]string{"foo", "corge"}},
			&DataFrame{
				err: fmt.Errorf("Cols(): `name` (corge) not found")},
		},
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
			if got := df.Cols(tt.args.names...); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Cols() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_DropRow(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		index int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{0},
			&DataFrame{
				values:        []*valueContainer{{slice: []string{"b"}, isNull: []bool{false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{10},
			&DataFrame{
				err: fmt.Errorf("DropRow(): index out of range (10 > 1)")},
		},
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
			if got := df.DropRow(tt.args.index); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.DropRow() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestDataFrame_Append(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		other *DataFrame
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values:        []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "anything"}},
				labels:        []*valueContainer{{slice: []int{2}, isNull: []bool{false}, name: "anything"}},
				colLevelNames: []string{"anything"}}},
			&DataFrame{
				values:        []*valueContainer{{slice: []string{"a", "b", "c"}, isNull: []bool{false, false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail - wrong number of levels",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values: []*valueContainer{
					{slice: []string{"c"}, isNull: []bool{false}, name: "anything"}},
				labels: []*valueContainer{
					{slice: []int{2}, isNull: []bool{false}, name: "anything"},
					{slice: []int{2}, isNull: []bool{false}, name: "anything"},
				},
				colLevelNames: []string{"anything"}}},
			&DataFrame{
				err: fmt.Errorf("Append(): other DataFrame must have same number of label levels as original DataFrame (2 != 1)")},
		},
		{"fail - wrong num columns",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values: []*valueContainer{
					{slice: []string{"c"}, isNull: []bool{false}, name: "anything"},
					{slice: []int{2}, isNull: []bool{false}, name: "anything"}},
				labels: []*valueContainer{
					{slice: []int{2}, isNull: []bool{false}, name: "anything"}},
				colLevelNames: []string{"anything"}}},
			&DataFrame{
				err: fmt.Errorf("Append(): other DataFrame must have same number of columns as original DataFrame (2 != 1)")},
		},
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
			if got := df.Append(tt.args.other); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Append() = %v, want %v", got.err, tt.want.err)
			}
		})
	}
}

func TestDataFrame_DropCol(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{values: []*valueContainer{
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
		},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"foo"},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail", fields{values: []*valueContainer{
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
		},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"corge"},
			&DataFrame{
				err: errors.New("DropCol(): `name` (corge) not found")},
		},
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
			if got := df.DropCol(tt.args.name); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.DropCol() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_DropLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		name string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"},
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
			},
			colLevelNames: []string{"*0"}},
			args{"foo"},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail", fields{
			values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"},
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
			},
			colLevelNames: []string{"*0"}},
			args{"corge"},
			&DataFrame{
				err: errors.New("DropLabels(): `name` (corge) not found")},
		},
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
			if got := df.DropLabels(tt.args.name); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.DropLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_DeduplicateNames(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrame
	}{
		{"normal", fields{values: []*valueContainer{
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
			{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
		},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			colLevelNames: []string{"*0"}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo_1"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"}},
				colLevelNames: []string{"*0"}},
		},
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
			if got := df.DeduplicateNames(); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.DeduplicateNames() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_FillNull(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		how map[string]NullFiller
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{

		{"pass", fields{
			values: []*valueContainer{
				{slice: []int{10, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{map[string]NullFiller{"foo": {FillZero: true}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "baz",
				colLevelNames: []string{"*0"}},
		},
		{"fail - no matching column", fields{
			values: []*valueContainer{
				{slice: []int{10, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{map[string]NullFiller{"corge": {FillZero: true}}},
			&DataFrame{
				err: fmt.Errorf("FillNull(): `name` (corge) not found")},
		},
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
			if got := df.FillNull(tt.args.how); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.FillNull() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestConcatSeries(t *testing.T) {
	type args struct {
		series []*Series
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"pass", args{
			[]*Series{
				{values: &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"},
					}},
				{values: &valueContainer{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"},
					}}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - wrong number of rows", args{
			[]*Series{
				{values: &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"},
					}},
				{values: &valueContainer{slice: []int{3}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"a"}, isNull: []bool{false}, name: "*0"},
					}}}},
			nil,
			true,
		},
		{"fail - wrong number of levels", args{
			[]*Series{
				{values: &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"},
					}},
				{values: &valueContainer{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"},
						{slice: []string{"c", "d"}, isNull: []bool{false, false}, name: "*0"},
					}}}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ConcatSeries(tt.args.series...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ConcatSeries() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ConcatSeries() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_At(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		row    int
		column int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   Element
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{0, 0},
			Element{Val: 0, IsNull: true},
		},
		{"fail - row out of range", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{10, 0},
			Element{},
		},
		{"fail - column out of range", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{0, 10},
			Element{},
		},
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
			if got := df.At(tt.args.row, tt.args.column); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.At() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_IndexOfContainer(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		name    string
		columns bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   int
	}{
		{"pass - search labels", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
			colLevelNames: []string{"*0"}},
			args{"qux", false},
			0},
		{"pass - search columns", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
			colLevelNames: []string{"*0"}},
			args{"foo", true},
			0},
		{"fail - columns", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
			colLevelNames: []string{"*0"}},
			args{"corge", false},
			-1},
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
			if got := df.IndexOfContainer(tt.args.name, tt.args.columns); got != tt.want {
				t.Errorf("DataFrame.IndexOfContainer() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_IndexOfRows(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
		{"pass - search column", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
			colLevelNames: []string{"*0"}},
			args{"foo", 1},
			[]int{1}},
		{"fail", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
			colLevelNames: []string{"*0"}},
			args{"corge", 1},
			nil},
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
			if got := df.IndexOfRows(tt.args.name, tt.args.value); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.IndexOfRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SwapLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		i string
		j string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			},
			colLevelNames: []string{"*0"},
		},
			args{"qux", "bar"},
			&DataFrame{
				values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "qux"},
					{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
			},
		},
		{"fail - i", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			}},
			args{"corge", "bar"},
			&DataFrame{
				err: errors.New("SwapLabels(): `i`: `name` (corge) not found")},
		},
		{"fail - j", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			}},
			args{"qux", "corge"},
			&DataFrame{
				err: errors.New("SwapLabels(): `j`: `name` (corge) not found")},
		},
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
			if got := df.SwapLabels(tt.args.i, tt.args.j); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SwapLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SliceLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   []interface{}
	}{
		{"pass", fields{values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
		},
			[]interface{}{
				[]int{1},
				[]int{0},
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
			if got := df.SliceLabels(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.SliceLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_NUnique(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 1}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "baz"}},
		},
			&Series{
				values: &valueContainer{slice: []int{1, 2}, isNull: []bool{false, false}, name: "nunique"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
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
			if got := df.NUnique(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.NUnique() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_NameOfLabel(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
				values:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
				colLevelNames: []string{"*0"}},
			args{0},
			"qux"},
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
			if got := df.NameOfLabel(tt.args.n); got != tt.want {
				t.Errorf("DataFrame.NameOfLabel() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_NameOfCol(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
				values:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "qux"}},
				colLevelNames: []string{"*0"}},
			args{0},
			"foo"},
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
			if got := df.NameOfCol(tt.args.n); got != tt.want {
				t.Errorf("DataFrame.NameOfCol() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SumColumns(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		name     string
		colNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass",
			fields{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"sum", []string{"foo", "qux"}},
			&Series{
				values: &valueContainer{slice: []float64{0, 3}, isNull: []bool{true, false}, name: "sum"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			}},
		{"fail - bad name",
			fields{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"", []string{"corge", "qux"}},
			&Series{
				err: fmt.Errorf("SumColumns(): `name` (corge) not found"),
			}},
		{"fail - no columns",
			fields{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"", nil},
			&Series{
				err: fmt.Errorf("SumColumns(): `colNames` cannot be empty"),
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
			if got := df.SumColumns(tt.args.name, tt.args.colNames...); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.SumColumns() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_listNamesAtLevel(t *testing.T) {
	type args struct {
		columns   []*valueContainer
		level     int
		numLevels int
	}
	tests := []struct {
		name    string
		args    args
		want    []string
		wantErr bool
	}{
		{"pass", args{[]*valueContainer{{name: "foo|bar"}, {name: "bar|baz"}}, 0, 2}, []string{"foo", "bar"}, false},
		{"fail - wrong numLevels", args{[]*valueContainer{{name: "foo|bar"}, {name: "bar|baz"}}, 3, 2}, nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := listNamesAtLevel(tt.args.columns, tt.args.level, tt.args.numLevels)
			if (err != nil) != tt.wantErr {
				t.Errorf("listNamesAtLevel() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("listNamesAtLevel() = %v, want %v", got, tt.want)
			}
		})
	}
}
