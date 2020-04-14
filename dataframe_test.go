package tada

import (
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"reflect"
	"strings"
	"testing"
	"time"

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
				values:        []*valueContainer{},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail - slices and labels nil", args{nil, nil},
			&DataFrame{
				err: errors.New("constructing new DataFrame: slices and labels cannot both be nil")},
		},
		{"fail - unsupported kind", args{
			[]interface{}{"foo"}, nil},
			&DataFrame{
				err: errors.New("constructing new DataFrame: slices: position 0: setting null values from interface{}: unsupported kind (string); must be slice")},
		},
		{"fail - unsupported label kind", args{
			[]interface{}{[]float64{1}}, []interface{}{"foo"}},
			&DataFrame{
				err: errors.New("constructing new DataFrame: labels: position 0: setting null values from interface{}: unsupported kind (string); must be slice")},
		},
		{"fail - wrong length labels", args{
			[]interface{}{[]int{0}},
			[]interface{}{[]string{"a", "b"}}},
			&DataFrame{
				err: errors.New("constructing new DataFrame: labels: position 0: slice does not match required length (2 != 1)")},
		},
		{"fail - wrong length columns", args{
			[]interface{}{[]int{0}, []string{"a", "b"}}, nil},
			&DataFrame{
				err: errors.New("constructing new DataFrame: columns: position 1: slice does not match required length (2 != 1)")},
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

func TestDataFrame_Err_String(t *testing.T) {
	type fields struct {
		values        []*valueContainer
		labels        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		want   string
	}{
		{"pass",
			fields{
				err: fmt.Errorf("foo")},
			"Error: foo"},
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
			if df.String() != tt.want {
				t.Errorf("DataFrame.Err().String() -> %v, want %v", df, tt.want)
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
				err: fmt.Errorf("type casting: name (corge) not found")},
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

func TestDataFrame_Series(t *testing.T) {
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
				err: fmt.Errorf("converting to Series: DataFrame must have a single column")},
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
			if got := df.Series(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Series() = %v, want %v", got, tt.want)
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
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.Copy() retained reference to original, want copy")
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
				"subsetting rows: index out of range [10] with length 2")}},
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

func TestReadCSVFromRecords(t *testing.T) {
	type args struct {
		csv    [][]string
		config []ReadOption
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
				config: []ReadOption{ReadOptionSwitchDims()}},
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
			got, err := ReadCSVFromRecords(tt.args.csv, tt.args.config...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadCSVFromRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ReadCSVFromRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

type badReader struct{}

func (r badReader) Read([]byte) (int, error) {
	return 0, fmt.Errorf("foo")
}

func TestReadCSV_String(t *testing.T) {
	type args struct {
		r      io.Reader
		config []ReadOption
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header, 0 labels - nil config",
			args{strings.NewReader("Name, Age\n foo, 1\n bar, 2"), nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "Name"},
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "Age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}}, false},
		{"fail - bad reader",
			args{badReader{}, nil},
			nil, true},
		{"fail - bad delimiter",
			args{strings.NewReader("Name, Age\n foo, 1\n bar, 2"), []ReadOption{ReadOptionDelimiter(0)}},
			nil, true},
		{"fail - empty",
			args{strings.NewReader(""), nil},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadCSV(tt.args.r, tt.args.config...)
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

func TestReadCSV_File(t *testing.T) {
	type args struct {
		path   string
		config []ReadOption
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"1 header, 0 labels - nil config",
			args{"test_csv/1_header_0_labels.csv", nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "Name"},
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "Age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}}, false},
		{"fail - bad delimiter",
			args{"test_csv/bad_delimiter.csv", nil},
			nil, true},
		{"fail - empty",
			args{"test_csv/empty.csv", nil},
			nil, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			f, err := os.Open(tt.args.path)
			if err != nil {
				t.Fatal(err)
			}
			got, err := ReadCSV(f, tt.args.config...)
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
			&DataFrame{err: fmt.Errorf("subsetting labels: index out of range [10] with length 2")}},
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
			&DataFrame{err: fmt.Errorf("subsetting columns: index out of range [10] with length 2")}},
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
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
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
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{1, 2},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"bar"}, isNull: []bool{false}, name: "0"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"fail - first > last", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{3, 2},
			&DataFrame{err: fmt.Errorf("range: first is greater than last (3 > 2)")}},
		{"fail - first out of range", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{3, 3},
			&DataFrame{err: fmt.Errorf("range: first index out of range [3] with length 3")}},
		{"fail - last out of range", fields{
			values: []*valueContainer{
				{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}, name: "0"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"},
			},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{2, 4},
			&DataFrame{err: fmt.Errorf("range: last index out of range [4] with max index 4 (length + 1)")}},
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
				err: fmt.Errorf("filtering columns: level out of range: 10 >= 0")},
		},
		{"fail - no lambda provided", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				{slice: []float64{1}, isNull: []bool{false}, name: "baz"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}}},
			args{nil, 0},
			&DataFrame{
				err: fmt.Errorf("filtering columns: must provide lambda function")},
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
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
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
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"foo", "qux"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "qux"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"replace column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"foo", []float64{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{10}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"append column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"baz", []float64{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{10}, isNull: []bool{false}, name: "baz"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"replace with Series", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
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
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"fail - bad input", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"baz", []complex64{10}},
			&DataFrame{err: fmt.Errorf("setting column: unable to calculate null values ([]complex64 not supported)")},
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
			got := df.WithCol(tt.args.name, tt.args.input)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.WithCol() = %v, want %v", got, tt.want)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.WithCol() changed underlying values, want copy")
			}
		})
	}
}

func TestDataFrame_WithLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
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
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"*0", "qux"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "qux"}},
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"replace column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"*0", []int{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{10}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"append column", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"baz", []float64{10}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []float64{10}, isNull: []bool{false}, name: "baz"}},
				colLevelNames: []string{"*0"},
				name:          "bar"},
		},
		{"fail - bad input", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "bar"},
			args{"baz", []complex64{10}},
			&DataFrame{err: fmt.Errorf("setting labels: unable to calculate null values ([]complex64 not supported)")},
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
			got := df.WithLabels(tt.args.name, tt.args.input)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.WithLabels() = %v, want %v", got, tt.want)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.WithLabels() changed underlying values, want copy")
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
			&DataFrame{err: fmt.Errorf("dropping null rows: name (corge) not found")},
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
			got := df.DropNull(tt.args.subset...)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.DropNull() = %v, want %v", got, tt.want)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.DropNull() changed underlying values, want copy")
			}
		})
	}
}

func TestDataFrame_IsNull(t *testing.T) {
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
				err: fmt.Errorf("getting null rows: name (corge) not found")},
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
			got := df.IsNull(tt.args.subset...)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.IsNull() = %v, want %v", got, tt.want)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.IsNull() changed underlying values, want copy")
			}
		})
	}
}

func TestDataFrame_SetAsLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
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
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"},
		},
			args{[]string{"bar"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"fail - too many columns listed", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"bar", "foo"}},
			&DataFrame{
				err: fmt.Errorf("setting column as labels: number of colNames must be less than number of columns (2 >= 2)")},
		},
		{"fail - no matching col", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"}},
			args{[]string{"corge"}},
			&DataFrame{
				err: fmt.Errorf("setting column as labels: name (corge) not found")},
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
			got := df.SetAsLabels(tt.args.colNames...)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SetAsLabels() = %v, want %v", got, tt.want)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.SetAsLabels() changed underlying values, want copy")
			}
		})
	}
}

func TestDataFrame_ResetLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
	}
	type args struct {
		index []string
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
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]string{"*1"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "1"},
			},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"pass - all levels", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{1}, isNull: []bool{false}, name: "*1"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{nil},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{0}, isNull: []bool{false}, name: "0"},
				{slice: []int{1}, isNull: []bool{false}, name: "1"},
			},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"}},
		{"fail - out of range ", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{1}, isNull: []bool{false}, name: "*1"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{[]string{"corge"}},
			&DataFrame{err: fmt.Errorf("resetting labels to columns: name (corge) not found")}},
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
			got := df.ResetLabels(tt.args.index...)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.ResetLabels() = %v, want %v", got.err, tt.want.err)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.ResetLabels() changed underlying values, want copy")
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
				err: fmt.Errorf("setting label names: number of levelNames must match number of levels in DataFrame (2 != 1)")},
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
				err: fmt.Errorf("setting label names: number of levelNames must match number of levels in DataFrame (1 != 2)")},
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
				err: fmt.Errorf("setting column names: number of colNames must match number of columns in DataFrame (2 != 1)")},
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
				err: fmt.Errorf("setting column names: number of colNames must match number of columns in DataFrame (1 != 2)")},
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
		labels        []*valueContainer
		values        []*valueContainer
		colLevelNames []string
		name          string
		err           error
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
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{map[string]FilterFn{
				"foo": func(val interface{}) bool { return val.(float64) > 1 },
				"bar": func(val interface{}) bool { return strings.Contains(val.(string), "a") },
			}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{2}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"bar"}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{2}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"no matches", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{map[string]FilterFn{
				"foo": func(val interface{}) bool { return val.(float64) >= 10 },
			}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{}, isNull: []bool{}, name: "foo"},
					{slice: []string{}, isNull: []bool{}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{}, isNull: []bool{}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"no filters - all rows", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
					{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
		},
		{"fail - empty filter", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{2, 3, 4}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{map[string]FilterFn{"*0": nil}},
			&DataFrame{err: fmt.Errorf("filtering rows: no filter function provided")}},
		{"fail - bad column name", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []float64{2, 3, 4}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{map[string]FilterFn{"corge": func(interface{}) bool { return true }}},
			&DataFrame{err: fmt.Errorf("filtering rows: name (corge) not found")}},
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
			got := df.Filter(tt.args.filters)
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Filter() = %v, want %v", got, tt.want)
			}
			if !dataFrameIsDistinct(got, df) {
				t.Errorf("DataFrame.Filter() changed underlying values, want copy")
			}
		})
	}
}

func TestDataFrame_FilterIndex(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		container string
		filterFn  FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"foo", func(v interface{}) bool { return v.(float64) > 1 }},
			[]int{2},
		},
		{"no matching rows", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"foo", func(v interface{}) bool { return v.(float64) > 10 }},
			[]int{},
		},
		{"fail - bad col name", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{"corge", func(v interface{}) bool { return v.(float64) > 1 }},
			nil,
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
			if got := df.FilterIndex(tt.args.container, tt.args.filterFn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.FilterIndex() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Where(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
				values: []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}},
			},
			args{
				name: "foo",
				filters: map[string]FilterFn{"qux": func(val interface{}) bool { return val.(int) > 1 },
					"": func(val interface{}) bool { return strings.Contains(val.(string), "ba") },
				},
				ifTrue:  "yes",
				ifFalse: 0},
			&Series{
				values: &valueContainer{slice: []interface{}{0, 0, "yes"}, isNull: []bool{false, false, false}, name: ""},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			false},
		{"pass - nulls",
			fields{
				values: []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{
				name: "foo",
				filters: map[string]FilterFn{
					"qux": func(val interface{}) bool { return val.(int) > 1 },
					"":    func(val interface{}) bool { return strings.Contains(val.(string), "ba") },
				},
				ifTrue:  "yes",
				ifFalse: ""},
			&Series{
				values: &valueContainer{slice: []interface{}{"", "", "yes"}, isNull: []bool{true, true, false}, name: ""},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			false},
		{"fail - bad container name",
			fields{
				values: []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{"foo", map[string]FilterFn{"corge": func(val interface{}) bool { return true }}, "yes", 0},
			nil, true},
		{"fail - unsupported ifTrue",
			fields{
				values: []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{"foo", map[string]FilterFn{"qux": func(val interface{}) bool { return true }}, complex64(1), 0},
			nil, true},
		{"fail - unsupported ifFalse",
			fields{
				values: []*valueContainer{{slice: []string{"foo", "bar", "baz"}, isNull: []bool{false, false, false}}},
				labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "qux"}}},
			args{"foo", map[string]FilterFn{"qux": func(val interface{}) bool { return false }}, 0, complex64(1)},
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
			got, err := df.Where(tt.args.filters, tt.args.ifTrue, tt.args.ifFalse)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.Where() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Where() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Apply(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
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
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{map[string]ApplyFn{"foo": func(slice interface{}, isNull []bool) interface{} {
				vals := slice.([]float64)
				ret := make([]float64, len(vals))
				for i := range ret {
					ret[i] = vals[i] * 2
				}
				return ret
			}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{2}, isNull: []bool{false}, name: "foo"},
					{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"fail - wrong length", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []int{1}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{map[string]ApplyFn{"foo": func(slice interface{}, isNull []bool) interface{} {
				return []int{1, 2, 3}
			}}},
			&DataFrame{
				err: fmt.Errorf("applying lambda function: constructing new values: new slice is not same length as original slice (3 != 1)")},
		},
		{"fail - no function", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{map[string]ApplyFn{"foo": nil}},
			&DataFrame{
				err: fmt.Errorf("applying lambda function: no apply function provided")},
		},
		{"fail - no matching column", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{map[string]ApplyFn{"corge": func(interface{}, []bool) interface{} { return 0 }}},
			&DataFrame{
				err: fmt.Errorf("applying lambda function: name (corge) not found")},
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
			if got := df.Apply(tt.args.lambdas); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SetRows(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		lambda    ApplyFn
		container string
		rows      []int
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{
				func(slice interface{}, isNull []bool) interface{} {
					vals := slice.([]float64)
					ret := make([]float64, len(vals))
					for i := range ret {
						ret[i] = vals[i] * 2
					}
					return ret
				},
				"foo", []int{1},
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 4, 3}, isNull: []bool{false, false, false}, name: "foo"},
					{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"pass - change null", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{true, true, true}, name: "foo"},
				{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{
				func(slice interface{}, isNull []bool) interface{} {
					vals := slice.([]float64)
					ret := make([]float64, len(vals))
					for i := range ret {
						isNull[i] = false
						ret[i] = vals[i] * 2
					}
					return ret
				},
				"foo", []int{1},
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 4, 3}, isNull: []bool{true, false, true}, name: "foo"},
					{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
		},
		{"fail - bad name", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{
				func(slice interface{}, isNull []bool) interface{} {
					vals := slice.([]float64)
					ret := make([]float64, len(vals))
					for i := range ret {
						ret[i] = vals[i] * 2
					}
					return ret
				},
				"corge", []int{1},
			},
			&DataFrame{
				err: fmt.Errorf("applying lambda to rows: name (corge) not found")},
		},
		{"fail - wrong length", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{
				func(slice interface{}, isNull []bool) interface{} {
					return []int{0, 1}
				},
				"foo", []int{1},
			},
			&DataFrame{
				err: fmt.Errorf("applying lambda to rows: constructing new values: new slice is not same length as original slice (2 != 1)")},
		},
		{"fail - no function", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []int{1, 2, 3}, isNull: []bool{false, false, false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "baz"},
			args{
				nil,
				"foo", []int{1},
			},
			&DataFrame{
				err: fmt.Errorf("applying lambda to rows: no apply function provided")},
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
			if got := df.SetRows(tt.args.lambda, tt.args.container, tt.args.rows); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.SetRows() = %v, want %v", got, tt.want)
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
				err: fmt.Errorf("sorting rows: must supply at least one Sorter")},
		},
		{"fail - bad colName", fields{
			values: []*valueContainer{
				{slice: []float64{0, 2, 1}, isNull: []bool{false, false, false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{[]Sorter{{Name: "corge"}}},
			&DataFrame{
				err: fmt.Errorf("sorting rows: position 0: name (corge) not found")},
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

func TestDataFrame_StdDev(t *testing.T) {
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
				values: &valueContainer{slice: []float64{.5, .5}, isNull: []bool{false, false}, name: "stdDev"},
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
			if got := df.StdDev(); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.StdDev() = %v, want %v", got, tt.want)
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
		other   *DataFrame
		options []JoinOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"matching label key *0",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values:        []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				name:          "bar",
				colLevelNames: []string{"*1"}},
				nil},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"", "c"}, isNull: []bool{true, false}, name: "bar"},
				},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0",
						cache: []string{"0", "1"}},
				},
				name:          "foo",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"right merge",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values:        []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				name:          "bar",
				colLevelNames: []string{"*1"}},
				[]JoinOption{JoinOptionHow("right")},
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"c"}, isNull: []bool{false}, name: "bar"},
					{slice: []string{"b"}, isNull: []bool{false}, name: "foo"},
				},
				labels: []*valueContainer{
					{slice: []int{1}, isNull: []bool{false}, name: "*0",
						cache: []string{"1"},
					}},
				name:          "bar",
				colLevelNames: []string{"*1"}},
			false,
		},
		{"inner merge", // resets cache when it drops null rows
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values:        []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				name:          "bar",
				colLevelNames: []string{"*1"}},
				[]JoinOption{JoinOptionHow("inner")},
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"b"}, isNull: []bool{false}, name: "foo"},
					{slice: []string{"c"}, isNull: []bool{false}, name: "bar"},
				},
				labels: []*valueContainer{
					{slice: []int{1}, isNull: []bool{false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - no shared merge key ",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{&DataFrame{
				values:        []*valueContainer{{slice: []string{"c"}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{1}, isNull: []bool{false}, name: "corge"}},
				colLevelNames: []string{"anything"}},
				nil},
			nil, true,
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
			got, err := df.Merge(tt.args.other, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.Merge() error = %v, want %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Merge() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Lookup(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	type args struct {
		other   *DataFrame
		options []JoinOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"single label level, supplied keys, left join - other has more labels", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				options: []JoinOption{JoinOptionLeftOn([]string{"foo"}), JoinOptionRightOn([]string{"foo"})}},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"single label level, supplied keys, left join - other has fewer labels", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"bar"}, isNull: []bool{false, false, false}}}},
				options: []JoinOption{JoinOptionLeftOn([]string{"foo"}), JoinOptionRightOn([]string{"foo"})}},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"auto key match", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "foo"}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}, name: "foo"}}},
				options: nil},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false},
						cache: []string{"bar", "baz"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"auto key match - right join", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "foo"}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}, name: "foo"}}},
				options: []JoinOption{JoinOptionHow("right")}},
			&DataFrame{values: []*valueContainer{{slice: []float64{0, 0, 1}, isNull: []bool{true, true, false}, name: "waldo"}},
				labels: []*valueContainer{
					{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false},
						cache: []string{"qux", "quux", "bar"}},
				},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - leftOn but not rightOn", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				options: []JoinOption{JoinOptionLeftOn([]string{"foo"})}},
			nil,
			true,
		},
		{"fail - bad leftOn", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				options: []JoinOption{JoinOptionLeftOn([]string{"corge"}), JoinOptionRightOn([]string{"foo"})}},
			nil,
			true,
		},
		{"fail - bad rightOn", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "baz"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				options: []JoinOption{JoinOptionLeftOn([]string{"foo"}), JoinOptionRightOn([]string{"corge"})}},
			nil,
			true,
		},
		{"fail - unsupported lookup", fields{
			values:        []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels:        []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:          "qux",
			colLevelNames: []string{"*0"}},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				options: []JoinOption{JoinOptionHow("other")}},
			nil,
			true,
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
			got, err := df.Lookup(tt.args.other, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.Lookup() error = %v, want %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Lookup() = %v, want %v", got, tt.want)

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
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
				labels:        []*valueContainer{{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "foo"}},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []interface{}{1}, isNull: []bool{false}, name: "bar"},
				{slice: []interface{}{2}, isNull: []bool{false}, name: "baz"}},
				labels:        []*valueContainer{{slice: []string{"waldo"}, isNull: []bool{false}, name: "*0"}},
				name:          "qux",
				colLevelNames: []string{"foo"}}},
		{"two columns",
			fields{
				values: []*valueContainer{
					{slice: []float64{1, 0}, isNull: []bool{false, true}, name: "waldo"},
					{slice: []int{3, 4}, isNull: []bool{false, false}, name: "fred"},
				},
				labels:        []*valueContainer{{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "foo"}},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []interface{}{1.0, 3}, isNull: []bool{false, false}, name: "bar"},
				{slice: []interface{}{0.0, 4}, isNull: []bool{true, false}, name: "baz"}},
				labels:        []*valueContainer{{slice: []string{"waldo", "fred"}, isNull: []bool{false, false}, name: "*0"}},
				name:          "qux",
				colLevelNames: []string{"foo"}}},
		{"two labels",
			fields{
				values: []*valueContainer{
					{slice: []interface{}{1, ""}, isNull: []bool{false, true}, name: "waldo"},
				},
				labels: []*valueContainer{
					{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "foo"},
					{slice: []int{3, 4}, isNull: []bool{false, false}, name: "fred"}},
				name:          "qux",
				colLevelNames: []string{"*0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []interface{}{1}, isNull: []bool{false}, name: "bar|3"},
				{slice: []interface{}{""}, isNull: []bool{true}, name: "baz|4"}},
				labels:        []*valueContainer{{slice: []string{"waldo"}, isNull: []bool{false}, name: "*0"}},
				name:          "qux",
				colLevelNames: []string{"foo", "fred"}}},
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
				err: fmt.Errorf("group by: name (corge) not found"),
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
				{slice: []string{"a", "b", "c", "null"}, isNull: []bool{false, false, false, true}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
			},
			colLevelNames: []string{"*0"},
		}, args{"year"},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "2018|foo"},
					{slice: []string{"", "", "c", "null"}, isNull: []bool{true, true, false, true}, name: "2019|foo"}},
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
				err: fmt.Errorf("promoting to column level: name (corge) not found")},
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
				err: fmt.Errorf("promoting to column level: cannot stack only column")},
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
				err: fmt.Errorf("promoting to column level: cannot stack only label level")},
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
		name    string
		fields  fields
		args    args
		want    *DataFrame
		wantErr bool
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
				name:          "sum_foo"},
			false,
		},
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
				name:          "mean_foo"},
			false,
		},
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
				name:          "median_foo"},
			false,
		},
		{"stdDev", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"},
			name:          "foo"},
			args{labels: "type", columns: "year", values: "amount", aggFn: "stdDev"},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{0, 0}, isNull: []bool{false, false}, name: "2018"},
				{slice: []float64{0, 0.5}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "stdDev_foo"},
			false,
		},
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
				name:          "count_foo"},
			false,
		},
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
				name:          "min_foo"},
			false,
		},
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
				name:          "max_foo"},
			false,
		},
		{"fail - no matching index level", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "corge", columns: "year", values: "amount", aggFn: "stdDev"},
			nil, true},
		{"fail - no matching columns", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "corge", values: "amount", aggFn: "stdDev"},
			nil, true},
		{"fail - no matching values", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "year", values: "corge", aggFn: "stdDev"},
			nil, true},
		{"fail - unsupported aggfunc", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "year", values: "amount", aggFn: "other"},
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
			got, err := df.PivotTable(tt.args.labels, tt.args.columns, tt.args.values, tt.args.aggFn)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.PivotTable() error = %v, want %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
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
					{slice: []float64{1}, isNull: []bool{false}, name: "0"},
					{slice: []float64{2}, isNull: []bool{false}, name: "1"}},
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

func TestReadStructSlice(t *testing.T) {
	type args struct {
		slice interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"pass", args{[]testStruct{{"foo", 1}, {"", 2}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", ""}, isNull: []bool{false, true}, name: "Name"},
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "Age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - bad input", args{"foo"},
			nil,
			true,
		},
		{"fail - unsupported value", args{[]testStructUnsupported{{complex64(1)}}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadStructSlice(tt.args.slice)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadStructSlice() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("ReadStructSlice() = %v, want %v", got, tt.want)
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
		r          io.Reader
		outputRows int
		config     []ReadOption
	}
	tests := []struct {
		name    string
		args    args
		wantW   string
		wantErr bool
	}{
		{"pass",
			args{
				r:          strings.NewReader("corge, qux\n 1.5, foo\n 2.5, foo"),
				config:     nil,
				outputRows: 3},
			want1, false},
		{"columns as major dim",
			args{
				r:          strings.NewReader("corge, 1.5, 2.5\n qux, foo, foo"),
				outputRows: 3,
				config:     []ReadOption{ReadOptionSwitchDims()}},
			want1, false},
		{"fail - no data", args{r: strings.NewReader(""), config: nil, outputRows: 3},
			"", true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &bytes.Buffer{}
			if err := WriteMockCSV(w, tt.args.outputRows, tt.args.r, tt.args.config...); (err != nil) != tt.wantErr {
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

func TestDataFrame_HasLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
		{"pass", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
			},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"*0"}}, false},
		{"fail", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"corge"}}, true},
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
			if err := df.HasLabels(tt.args.labelNames...); (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.HasLabels() error = %v, wantErr %v", err, tt.wantErr)
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
		r             io.Reader
		includeLabels bool
		options       []ReadOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		want1   *tablediff.Differences
		wantErr bool
	}{
		{name: "pass - read in labels",
			fields: fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args: args{r: strings.NewReader("*0, foo\n 0, 1"), includeLabels: true,
				options: []ReadOption{ReadOptionLabels(1)}},
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
			args:    args{r: strings.NewReader("foo\n 1"), includeLabels: false},
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
			args:    args{r: strings.NewReader("*0, foo\n 0"), includeLabels: false},
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
			got, got1, err := df.EqualsCSV(tt.args.includeLabels, tt.args.r)
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

func TestDataFrame_CSVRecords(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
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
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{},
			[][]string{{"*0", "foo"}, {"0", "a"}, {"1", "b"}}, false},
		{"pass - ignore labels",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{[]WriteOption{WriteOptionExcludeLabels()}},
			[][]string{{"foo"}, {"a"}, {"b"}}, false},
		{"fail",
			fields{values: nil,
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{},
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
			got := df.CSVRecords(tt.args.options...)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.CSVRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_InterfaceRecords(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		options []WriteOption
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   [][]interface{}
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", ""}, isNull: []bool{false, true}, name: "foo"},
				{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, name: "bar"},
			},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{nil},
			[][]interface{}{{"*0", 0, 1}, {"foo", "a", "(null)"}, {"bar", 1, "qux"}}},
		{"pass - exclude labels",
			fields{values: []*valueContainer{
				{slice: []string{"a", ""}, isNull: []bool{false, true}, name: "foo"},
				{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, name: "bar"},
			},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{[]WriteOption{WriteOptionExcludeLabels()}},
			[][]interface{}{{"foo", "a", "(null)"}, {"bar", 1, "qux"}}},
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
			if got := df.InterfaceRecords(tt.args.options...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.InterfaceRecords() = %v, want %v", got, tt.want)
			}
		})
	}
}

type badWriter struct{}

func (w badWriter) Write([]byte) (int, error) {
	return 0, fmt.Errorf("foo")
}

func TestDataFrame_WriteCSV(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		w       io.Writer
		options []WriteOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"pass",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{new(bytes.Buffer), nil},
			"*0,foo\n0,a\n1,b\n",
			false},
		{"pass - delimiter",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{new(bytes.Buffer), []WriteOption{WriteOptionDelimiter('|')}},
			"*0|foo\n0|a\n1|b\n",
			false},
		{"pass - exclude labels",
			fields{values: []*valueContainer{
				{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "foo"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{new(bytes.Buffer), []WriteOption{WriteOptionExcludeLabels()}},
			"foo\na\nb\n",
			false},
		{"fail - bad writer", fields{values: nil,
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{badWriter{}, nil}, "", true},
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
			w := tt.args.w
			err := df.WriteCSV(w, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.WriteCSV() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if tt.wantErr == false {
				if w.(*bytes.Buffer).String() != tt.want {
					t.Errorf("DataFrame.WriteCSV() -> w = %v, want %v", w.(*bytes.Buffer).String(), tt.want)
				}
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

func TestDataFrame_FilterByValue(t *testing.T) {
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
				err: fmt.Errorf("filter by value: name (corge) not found")},
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
			if got := df.FilterByValue(tt.args.filters); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.FilterByValue() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_LabelsAsSeries(t *testing.T) {
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
				err: fmt.Errorf("converting labels to Series: name (corge) not found")},
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
			if got := df.LabelsAsSeries(tt.args.name); !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.LabelsAsSeries() = %v, want %v", got, tt.want)
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
				err: fmt.Errorf("getting column: name (corge) not found")},
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
				err: fmt.Errorf("getting columns: name (corge) not found")},
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
				err: fmt.Errorf("dropping row: index out of range [10] with length 2")},
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
				err: fmt.Errorf("appending rows: other must have same number of label levels as original (2 != 1)")},
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
				err: fmt.Errorf("appending rows: other must have same number of columns as original (2 != 1)")},
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
				err: errors.New("dropping column: name (corge) not found")},
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
				err: errors.New("dropping labels: name (corge) not found")},
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
				err: fmt.Errorf("filling null rows: name (corge) not found")},
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
				{values: &valueContainer{slice: []int{3}, isNull: []bool{false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"a"}, isNull: []bool{false, false}, name: "*0"},
					}}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					{slice: []int{3, 0}, isNull: []bool{false, true}, name: "qux"},
				},
				labels: []*valueContainer{
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, cache: []string{"a", "b"}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - no shared key", args{
			[]*Series{
				{values: &valueContainer{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
					labels: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"},
					}},
				{values: &valueContainer{slice: []int{3}, isNull: []bool{false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"a"}, isNull: []bool{false}, name: "corge"},
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
		want   *Element
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{0, 0},
			&Element{Val: 0, IsNull: true},
		},
		{"fail - row out of range", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{10, 0},
			nil,
		},
		{"fail - column out of range", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []int{3, 4}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{0, 10},
			nil,
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
			got := df.At(tt.args.row, tt.args.column)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.At() = %v, want %v", got, tt.want)
			}
			if got != nil {
				got.Val = "foobar"
				if got == df.At(tt.args.row, tt.args.column) {
					t.Errorf("DataFrame.At() retained reference to underlying value, want copy")
				}
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
				err: errors.New("swapping labels:i: name (corge) not found")},
		},
		{"fail - j", fields{
			values: []*valueContainer{{slice: []float64{1}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{
				{slice: []int{1}, isNull: []bool{false}, name: "bar"},
				{slice: []int{0}, isNull: []bool{false}, name: "qux"},
			}},
			args{"qux", "corge"},
			&DataFrame{
				err: errors.New("swapping labels:j: name (corge) not found")},
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

func TestDataFrame_GetLabels(t *testing.T) {
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
			if got := df.GetLabels(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.GetLabels() = %v, want %v", got, tt.want)
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

func TestDataFrame_SumCols(t *testing.T) {
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
		name    string
		fields  fields
		args    args
		want    *Series
		wantErr bool
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
			},
			false,
		},
		{"fail - bad name",
			fields{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"", []string{"corge", "qux"}},
			nil,
			true,
		},
		{"fail - no columns",
			fields{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
			args{"", nil},
			nil,
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
			got, err := df.SumCols(tt.args.name, tt.args.colNames...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.SumCols() error = %v, want %v", err, tt.wantErr)
				return
			}
			if !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.SumCols() = %v, want %v", got, tt.want)
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

func TestDataFrame_Iterator(t *testing.T) {
	type fields struct {
		values        []*valueContainer
		labels        []*valueContainer
		name          string
		colLevelNames []string
		err           error
	}
	tests := []struct {
		name   string
		fields fields
		want   *DataFrameIterator
	}{
		{"pass",
			fields{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}},
			&DataFrameIterator{
				current: -1,
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
						{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
					},
					labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
					name:          "foo",
					colLevelNames: []string{"*0"},
				}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				values:        tt.fields.values,
				labels:        tt.fields.labels,
				name:          tt.fields.name,
				colLevelNames: tt.fields.colLevelNames,
				err:           tt.fields.err,
			}
			got := df.Iterator()
			if got.current != tt.want.current {
				t.Errorf("DataFrame.Iterator() = %v, want %v", got.current, tt.want.current)
			}
			if !EqualDataFrames(got.df, tt.want.df) {
				t.Errorf("DataFrame.Iterator() = %v, want %v", got.df, tt.want.df)
			}
		})
	}
}

func TestDataFrameIterator_Next(t *testing.T) {
	type fields struct {
		current int
		df      *DataFrame
	}
	tests := []struct {
		name   string
		fields fields
		want   bool
	}{
		{"not at end", fields{
			current: -1,
			df: &DataFrame{
				values: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}}},
			true,
		},
		{"at end", fields{
			current: 1,
			df: &DataFrame{values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
			},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:          "foo",
				colLevelNames: []string{"*0"}}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := &DataFrameIterator{
				current: tt.fields.current,
				df:      tt.fields.df,
			}
			if got := iter.Next(); got != tt.want {
				t.Errorf("DataFrameIterator.Next() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrameIterator_Row(t *testing.T) {
	type fields struct {
		current int
		df      *DataFrame
	}
	tests := []struct {
		name   string
		fields fields
		want   map[string]Element
	}{
		{"pass",
			fields{
				current: 0,
				df: &DataFrame{
					values: []*valueContainer{
						{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
						{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
					},
					labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
					name:          "foo",
					colLevelNames: []string{"*0"}}},
			map[string]Element{"foo": {int(0), true}, "qux": {float64(1), false}, "*0": {int(0), false}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			iter := &DataFrameIterator{
				current: tt.fields.current,
				df:      tt.fields.df,
			}
			if got := iter.Row(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrameIterator.Row() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Resample(t *testing.T) {
	d := time.Date(2020, 2, 15, 0, 0, 0, 0, time.UTC)
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		how map[string]Resampler
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []time.Time{d}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"2019-12-30"}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"},
		},
			args{map[string]Resampler{"foo": {ByYear: true}, "bar": {ByMonth: true}}},
			&DataFrame{values: []*valueContainer{
				{slice: []time.Time{time.Date(2020, 1, 1, 0, 0, 0, 0, time.UTC)}, isNull: []bool{false}, name: "foo"},
				{slice: []time.Time{time.Date(2019, 12, 1, 0, 0, 0, 0, time.UTC)}, isNull: []bool{false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:          "baz",
				colLevelNames: []string{"*0"}},
		},
		{"fail - bad column", fields{
			values: []*valueContainer{
				{slice: []time.Time{d}, isNull: []bool{false}, name: "foo"},
				{slice: []string{"2019-12-30"}, isNull: []bool{false}, name: "bar"}},
			labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:          "baz",
			colLevelNames: []string{"*0"},
		},
			args{map[string]Resampler{"corge": {ByYear: true}}},
			&DataFrame{err: fmt.Errorf("resample: name (corge) not found")},
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
			if got := df.Resample(tt.args.how); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Resample() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SetNulls(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		n     int
		nulls []bool
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    []bool
		wantErr bool
	}{
		{"pass", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:          "foo",
			colLevelNames: []string{"*0"}},
			args{0, []bool{true, true}},
			[]bool{true, true},
			false,
		},
		{"fail - out of range", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:          "foo",
			colLevelNames: []string{"*0"}},
			args{10, []bool{true, true}},
			[]bool{false, false},
			true,
		},
		{"fail - wrong shape", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
			},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:          "foo",
			colLevelNames: []string{"*0"}},
			args{0, []bool{false}},
			[]bool{false, false},
			true,
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
			err := df.SetNulls(tt.args.n, tt.args.nulls)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.SetNulls() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if err == nil {
				mergedLabelsAndCols := append(df.labels, df.values...)
				if !reflect.DeepEqual(mergedLabelsAndCols[tt.args.n].isNull, tt.want) {
					t.Errorf("DataFrame.SetNulls() values.isNull -> = %v, want %v", mergedLabelsAndCols[tt.args.n].isNull, tt.want)
				}
			}
		})
	}
}

func TestDataFrame_HasType(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		sliceType string
	}
	tests := []struct {
		name            string
		fields          fields
		args            args
		wantLabelIndex  []int
		wantColumnIndex []int
	}{
		{"labels and values", fields{
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "qux"},
				{slice: []int{0, 1}, isNull: []bool{true, false}, name: "foo"},
			},
			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:          "foo",
			colLevelNames: []string{"*0"}},
			args{"[]int"},
			[]int{0}, []int{0, 2},
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
			gotLabelIndex, gotColumnIndex := df.HasType(tt.args.sliceType)
			if !reflect.DeepEqual(gotLabelIndex, tt.wantLabelIndex) {
				t.Errorf("DataFrame.HasType() gotLabelIndex = %v, want %v", gotLabelIndex, tt.wantLabelIndex)
			}
			if !reflect.DeepEqual(gotColumnIndex, tt.wantColumnIndex) {
				t.Errorf("DataFrame.HasType() gotColumnIndex = %v, want %v", gotColumnIndex, tt.wantColumnIndex)
			}
		})
	}
}

type testSchema struct {
	Foo  []int `tada:"foo"`
	skip []float64
	Bar  []float64 `tada:"bar"`
}

type testSchema2 struct {
	Foo  []int
	skip []float64
	Bar  []float64
}

type testSchema3 struct {
	Foo     []int     `tada:"foo"`
	NullMap [][]bool  `tada:"isNull"`
	Bar     []float64 `tada:"bar"`
}

type testSchema4 struct {
	Foo     []int
	NullMap [][]int `tada:"isNull"`
}

type testSchema5 struct {
	Foo []int
	Bar []float64
	Baz []string
}

type testSchema6 struct {
	Foo []int         `tada:"foo"`
	Bar []interface{} `tada:"bar"`
}

func TestReadStruct(t *testing.T) {
	type args struct {
		strct   interface{}
		options []ReadOption
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"pass - default labels",
			args{
				testSchema{
					Foo: []int{1, 2},
					Bar: []float64{3, 4},
				}, nil},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          ""},
			false},
		{"pass - pointer with labels",
			args{
				&testSchema{
					Foo: []int{1, 2},
					Bar: []float64{3, 4},
				}, nil},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          ""},
			false},
		{"pass - default labels - no tags",
			args{
				testSchema2{
					Foo: []int{1, 2},
					Bar: []float64{3, 4},
				}, nil},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "Foo"},
					{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "Bar"},
				},
				colLevelNames: []string{"*0"},
				name:          ""},
			false},
		{"pass - supplied labels",
			args{
				testSchema{
					Foo: []int{1, 2},
					Bar: []float64{3, 4},
				}, []ReadOption{ReadOptionLabels(1)}},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"}},
				values: []*valueContainer{
					{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          ""},
			false},
		{"pass - null table",
			args{
				testSchema3{
					Foo:     []int{0, 2},
					Bar:     []float64{3, 4},
					NullMap: [][]bool{{true, false}, {false, false}},
				}, nil},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []int{0, 2}, isNull: []bool{true, false}, name: "foo"},
					{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          ""},
			false},
		{"pass - null table - with index",
			args{
				testSchema3{
					Foo:     []int{0, 2},
					Bar:     []float64{3, 4},
					NullMap: [][]bool{{true, false}, {false, false}},
				}, []ReadOption{ReadOptionLabels(1)}},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0, 2}, isNull: []bool{true, false}, name: "foo"}},
				values: []*valueContainer{
					{slice: []float64{3, 4}, isNull: []bool{false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          ""},
			false},
		{"fail - null table of wrong type",
			args{
				testSchema4{
					Foo:     []int{0, 2},
					NullMap: [][]int{{0, 1}, {1, 2}},
				}, nil},
			nil,
			true},
		{"fail - null table with wrong length",
			args{
				testSchema3{
					Foo:     []int{0, 2},
					Bar:     []float64{3, 4},
					NullMap: [][]bool{{true, false}, {false}},
				}, nil},
			nil,
			true},
		{"fail - nil values",
			args{
				testSchema{
					Foo: []int{1, 2},
				}, nil},
			nil,
			true},
		{"fail - not struct",
			args{
				[]int{}, nil},
			nil,
			true},
		{"fail - uneven lengths",
			args{
				testSchema{
					Foo: []int{1, 2},
					Bar: []float64{3, 4, 5},
				}, nil},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadStruct(tt.args.strct, tt.args.options...)
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

func TestDataFrame_Struct(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		structPointer interface{}
		options       []WriteOption
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{"pass - match tag names", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema{}, nil},
			&testSchema{
				Foo: []int{0, 1},
				Bar: []float64{0, 1},
			},
			false,
		},
		{"pass - match exported names", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "Foo"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "Bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema2{}, nil},
			&testSchema2{
				Foo: []int{0, 1},
				Bar: []float64{0, 1},
			},
			false,
		},
		{"pass - ignore label names", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			values: []*valueContainer{
				{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"},
				{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema{}, []WriteOption{WriteOptionExcludeLabels()}},
			&testSchema{
				Foo: []int{0, 1},
				Bar: []float64{0, 1},
			},
			false,
		},
		{"pass - null table", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema3{}, nil},
			&testSchema3{
				Foo:     []int{0, 1},
				NullMap: [][]bool{{false, false}, {true, false}},
				Bar:     []float64{0, 1},
			},
			false,
		},
		{"fail - not pointer", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "Bar"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "Foo"},
			},
			colLevelNames: []string{"*0"}},
			args{testSchema{}, nil},
			testSchema{},
			true,
		},

		{"fail - not pointer to struct", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "Bar"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "Foo"},
			},
			colLevelNames: []string{"*0"}},
			args{&[]float64{}, nil},
			&[]float64{},
			true,
		},
		{"fail - not enough containers", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "Foo"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "Bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema5{}, nil},
			&testSchema5{
				Foo: []int{0, 1},
				Bar: []float64{0, 1},
			},
			true,
		},
		{"fail - wrong order", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "Bar"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "Foo"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema{}, nil},
			&testSchema{},
			true,
		},
		{"fail - does not match exported name or tag name", fields{
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "corge"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema{}, nil},
			&testSchema{},
			true,
		},
		{"fail - does not match field type", fields{
			labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, name: "foo"}},
			values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "bar"},
			},
			colLevelNames: []string{"*0"}},
			args{&testSchema{}, nil},
			&testSchema{},
			true,
		},
		{"fail - null table of wrong type",
			fields{
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "Foo"}},
				values: []*valueContainer{
					{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "Bar"},
				},
				colLevelNames: []string{"*0"}},
			args{&testSchema4{}, nil},
			&testSchema4{Foo: []int{0, 1}},
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
			err := df.Struct(tt.args.structPointer, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.Struct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(tt.args.structPointer, tt.want) {
				t.Errorf("DataFrame.Struct() -> %v, want %v", tt.args.structPointer, tt.want)
			}
		})
	}
}

func TestReadInterfaceRecords(t *testing.T) {
	type args struct {
		records [][]interface{}
		options []ReadOption
	}
	tests := []struct {
		name    string
		args    args
		wantRet *DataFrame
		wantErr bool
	}{
		{"pass - major dim rows - 1 header",
			args{
				[][]interface{}{
					{"foo", "bar"},
					{float64(1), float64(2)},
				}, nil},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []interface{}{float64(1)}, isNull: []bool{false}, name: "foo"},
					{slice: []interface{}{float64(2)}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"pass - major dim cols - 1 header",
			args{
				[][]interface{}{
					{"foo", float64(1)},
					{"bar", float64(2)},
				}, []ReadOption{ReadOptionSwitchDims()}},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []interface{}{float64(1)}, isNull: []bool{false}, name: "foo"},
					{slice: []interface{}{float64(2)}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"pass - major dim cols - 1 header - not string",
			args{
				[][]interface{}{
					{0, float64(1)},
					{1, float64(2)},
				}, []ReadOption{ReadOptionSwitchDims()}},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []interface{}{float64(1)}, isNull: []bool{false}, name: "0"},
					{slice: []interface{}{float64(2)}, isNull: []bool{false}, name: "1"},
				},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"pass - major dim cols - 1 label",
			args{
				[][]interface{}{
					{"foo", float64(1)},
					{"bar", float64(2)},
				}, []ReadOption{ReadOptionSwitchDims(), ReadOptionLabels(1)}},
			&DataFrame{
				labels: []*valueContainer{
					{slice: []interface{}{float64(1)}, isNull: []bool{false}, name: "foo"}},
				values: []*valueContainer{
					{slice: []interface{}{float64(2)}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"pass - major dim cols - 0 headers",
			args{
				[][]interface{}{
					{float64(1)},
					{float64(2)},
				}, []ReadOption{ReadOptionSwitchDims(), ReadOptionHeaders(0)}},
			&DataFrame{
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				values: []*valueContainer{
					{slice: []interface{}{float64(1)}, isNull: []bool{false}, name: "0"},
					{slice: []interface{}{float64(2)}, isNull: []bool{false}, name: "1"},
				},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - no records",
			args{
				[][]interface{}{}, nil},
			nil, true,
		},
		{"fail - first record empty",
			args{
				[][]interface{}{{}, {0}}, nil},
			nil, true,
		},
		{"fail - unevenly shaped",
			args{
				[][]interface{}{{"foo"}, {1, 2}}, nil},
			nil, true,
		},
		{"fail - unsupported type - no labels",
			args{
				[][]interface{}{{"foo"}, {[]complex64{1}}}, nil},
			nil, true,
		},
		{"fail - unsupported type - with labels",
			args{
				[][]interface{}{{"foo", "bar"}, {[]complex64{1}, []complex64{2}}}, []ReadOption{ReadOptionLabels(1)}},
			nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRet, err := ReadInterfaceRecords(tt.args.records, tt.args.options...)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadInterfaceRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(gotRet, tt.wantRet) {
				t.Errorf("ReadInterfaceRecords() = %v, want %v", gotRet, tt.wantRet)
			}
		})
	}
}

func TestDataFrame_ReorderCols(t *testing.T) {
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
		{"pass",
			fields{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "foo"}},
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
					{slice: []float64{3}, isNull: []bool{false}, name: "baz"},
					{slice: []float64{5}, isNull: []bool{false}, name: "qux"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{[]string{"qux", "baz"}},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "foo"}},
				values: []*valueContainer{
					{slice: []float64{5}, isNull: []bool{false}, name: "qux"},
					{slice: []float64{3}, isNull: []bool{false}, name: "baz"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo",
			},
		},
		{"fail - bad column name",
			fields{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "foo"}},
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
					{slice: []float64{3}, isNull: []bool{false}, name: "baz"},
					{slice: []float64{5}, isNull: []bool{false}, name: "qux"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{[]string{"qux", "corge"}},
			&DataFrame{
				err: fmt.Errorf("reordering columns: colNames (index 1): name (corge) not found"),
			},
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
			if got := df.ReorderCols(tt.args.colNames); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.ReorderCols() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_ReorderLabels(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		levelNames []string
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "foo"},
					{slice: []float64{3}, isNull: []bool{false}, name: "baz"},
					{slice: []float64{5}, isNull: []bool{false}, name: "qux"},
				},
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{[]string{"qux", "baz"}},
			&DataFrame{
				labels: []*valueContainer{
					{slice: []float64{5}, isNull: []bool{false}, name: "qux"},
					{slice: []float64{3}, isNull: []bool{false}, name: "baz"},
				},
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo",
			},
		},
		{"fail - bad column name",
			fields{
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "foo"},
					{slice: []float64{3}, isNull: []bool{false}, name: "baz"},
					{slice: []float64{5}, isNull: []bool{false}, name: "qux"},
				},
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{[]string{"qux", "corge"}},
			&DataFrame{
				err: fmt.Errorf("reordering labels: levelNames (index 1): name (corge) not found"),
			},
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
			if got := df.ReorderLabels(tt.args.levelNames); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.ReorderLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Shuffle(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		seed int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"pass",
			fields{
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values: []*valueContainer{
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{1},
			&DataFrame{
				labels: []*valueContainer{
					{slice: []int{0, 1, 3, 2}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values: []*valueContainer{
					{slice: []float64{1, 2, 4, 3}, isNull: []bool{false, false, false, false}, name: "bar"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
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
			if got := df.Shuffle(tt.args.seed); !EqualDataFrames(got, tt.want) {
				t.Errorf("DataFrame.Shuffle() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Reduce(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		name   string
		lambda ReduceFn
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
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values: []*valueContainer{
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "bar"},
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{true, false, false, false}, name: "baz"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{"custom_sum", func(slice interface{}, isNull []bool) (value interface{}, null bool) {
				vals := slice.([]float64)
				var sum float64
				for i := range vals {
					if isNull[i] {
						return 0.0, true
					}
					sum += vals[i]
				}
				return sum, false
			}},
			&Series{
				values: &valueContainer{slice: []float64{10, 0}, isNull: []bool{false, true}, name: "custom_sum"},
				labels: []*valueContainer{
					{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "*0"}}},
			false,
		},
		{"pass - multi level columns",
			fields{
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values: []*valueContainer{
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "2019|bar"},
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{true, false, false, false}, name: "2019|baz"},
				},
				colLevelNames: []string{"year", "class"},
				name:          "foo"},
			args{"custom_sum", func(slice interface{}, isNull []bool) (value interface{}, null bool) {
				vals := slice.([]float64)
				var sum float64
				for i := range vals {
					if isNull[i] {
						return 0.0, true
					}
					sum += vals[i]
				}
				return sum, false
			}},
			&Series{
				values: &valueContainer{slice: []float64{10, 0}, isNull: []bool{false, true}, name: "custom_sum"},
				labels: []*valueContainer{
					{slice: []string{"2019", "2019"}, isNull: []bool{false, false}, name: "year"},
					{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "class"}}},
			false,
		},
		{"fail - mixed types",
			fields{
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values: []*valueContainer{
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "bar"},
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{true, false, false, false}, name: "baz"},
				},
				colLevelNames: []string{"year", "class"},
				name:          "foo"},
			args{"custom_sum", func(slice interface{}, isNull []bool) (value interface{}, null bool) {
				vals := slice.([]float64)
				var sum float64
				for i := range vals {
					if isNull[i] {
						return "nil", true
					}
					sum += vals[i]
				}
				return sum, false
			}},
			nil, true,
		},
		{"fail - no function",
			fields{
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values: []*valueContainer{
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "2019|bar"},
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{true, false, false, false}, name: "2019|baz"},
				},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{"custom_sum", nil},
			nil, true,
		},
		{"fail - no columns",
			fields{
				labels: []*valueContainer{
					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "foo"},
				},
				values:        []*valueContainer{},
				colLevelNames: []string{"*0"},
				name:          "foo"},
			args{"custom_sum", func(slice interface{}, isNull []bool) (value interface{}, null bool) { return nil, true }},
			nil, true,
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
			got, err := df.Reduce(tt.args.name, tt.args.lambda)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.Reduce() error = %v, want %v", err, tt.wantErr)
				return
			}
			if !EqualSeries(got, tt.want) {
				t.Errorf("DataFrame.Reduce() = %v, want %v", got, tt.want)
			}
		})
	}
}
