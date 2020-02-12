package tada

import (
	"errors"
	"fmt"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/d4l3k/messagediff"
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
					{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}},
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
				values: &valueContainer{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}}},
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
			if got := df.ToSeries(); !reflect.DeepEqual(got, tt.want) {
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
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Copy() = %v, want %v", got, tt.want)
			}
			got.values[0].isNull[0] = true
			if reflect.DeepEqual(got, df) {
				t.Errorf("DataFrame.Copy() = retained reference to original values")
			}
			got = df.Copy()
			got.err = errors.New("foo")
			if reflect.DeepEqual(got, df) {
				t.Errorf("DataFrame.Copy() retained reference to original error")
			}
			got = df.Copy()
			got.name = "qux"
			if reflect.DeepEqual(got, df) {
				t.Errorf("DataFrame.Copy() retained reference to original name")
			}
			got = df.Copy()
			got.colLevelNames[0] = "*1"
			if reflect.DeepEqual(got, df) {
				t.Errorf("DataFrame.Copy() retained reference to original col level names")
			}
		})
	}
}

func TestReadCSVByRows(t *testing.T) {
	type args struct {
		csv    [][]string
		config *ReadConfig
	}
	tests := []struct {
		name string
		args args
		want *DataFrame
	}{
		{"1 header row, 2 columns, no index",
			args{
				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
				config: &ReadConfig{NumHeaderRows: 1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := readCSVByRows(tt.args.csv, tt.args.config); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readCSVByRows() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestDataFrame_Subset(t *testing.T) {
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
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "1"}},
			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
			name:   "baz"},
			args{[]int{0}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
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
			if got := df.Subset(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Subset() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SubsetLabels(t *testing.T) {
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
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"},
			},
			name: "baz"},
			args{[]int{1}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels: []*valueContainer{{slice: []int{10}, isNull: []bool{false}, name: "*10"}},
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
			if got := df.SubsetLabels(tt.args.index); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.SubsetLabels() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_SubsetCols(t *testing.T) {
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
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"},
			},
			name: "baz"},
			args{[]int{1}},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, name: "*0"},
					{slice: []int{10}, isNull: []bool{false}, name: "*10"}},
				name: "baz"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.SubsetCols(tt.args.index); !reflect.DeepEqual(got, tt.want) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Head(tt.args.n); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Head() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Tail(tt.args.n); !reflect.DeepEqual(got, tt.want) {
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
				{slice: []string{"bar", "baz"}, isNull: []bool{false, false}, name: "0"}},
				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}, name: "*0"}},
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
			if got := df.Range(tt.args.first, tt.args.last); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Range() = %v, want %v", got.labels[0], tt.want.labels[0])
			}
		})
	}
}

func TestDataFrame_FilterCols(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
	}
	type args struct {
		lambda func(string) bool
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"single level", fields{
			values: []*valueContainer{
				{slice: []float64{1}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"},
				{slice: []float64{1}, isNull: []bool{false}, name: "baz"}},
			labels: []*valueContainer{
				{slice: []int{0}, isNull: []bool{false}, name: "*0"},
				{slice: []int{10}, isNull: []bool{false}, name: "*10"}}},
			args{func(s string) bool {
				if strings.Contains(s, "ba") {
					return true
				}
				return false
			}},
			[]int{1, 2}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.FilterCols(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.WithCol(tt.args.name, tt.args.input); !reflect.DeepEqual(got, tt.want) {
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.WithLabels(tt.args.name, tt.args.input); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.WithLabels() = %v, want %v", got.values[0], tt.want.values[0])
			}
		})
	}
}

func TestDataFrame_Valid(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
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
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{nil},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{2}, isNull: []bool{false}, name: "0"},
				{slice: []string{"bar"}, isNull: []bool{false}, name: "1"}},
				labels: []*valueContainer{{slice: []int{2}, isNull: []bool{false}, name: "*0"}},
				name:   "baz"},
		},
		{"subset", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "0"},
				{slice: []string{"", "bar"}, isNull: []bool{true, false}, name: "1"}},
				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}, name: "*0"}},
				name:   "baz"},
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
			if got := df.DropNull(tt.args.subset...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.DropNull() = %v, want %v", got.values, tt.want.values)
			}
		})
	}
}

func TestDataFrame_Null(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
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
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{nil},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{0, 1}, isNull: []bool{true, false}, name: "0"},
				{slice: []string{"foo", ""}, isNull: []bool{false, true}, name: "1"}},
				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				name:   "baz"},
		},
		{"subset", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{true, false, false}, name: "0"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, true, false}, name: "1"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{[]string{"0"}},
			&DataFrame{values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{true}, name: "0"},
				{slice: []string{"foo"}, isNull: []bool{false}, name: "1"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:   "baz"},
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
			if got := df.Null(tt.args.subset...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Null() = %v, want %v", got.values[0], tt.want.values[0])
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.SetLabels(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.SetLabels() = %v, want %v", got.values[0], tt.want.values[0])
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
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
		{"normal", fields{
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.ResetLabels(tt.args.index...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ResetLabels() = %v, want %v", got.values[1], tt.want.values[1])
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
		filters []FilterFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   []int
	}{
		{"multiple", fields{
			values: []*valueContainer{
				{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"},
				{slice: []string{"foo", "", "bar"}, isNull: []bool{false, false, false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}}},
			args{[]FilterFn{
				{F64: func(v float64) bool {
					if v < 2 {
						return true
					}
					return false
				}, ColName: "foo"},
				{String: func(v string) bool {
					if strings.Contains(v, "oo") {
						return true
					}
					return false
				}, ColName: "bar"},
			}}, []int{0}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := &DataFrame{
				labels: tt.fields.labels,
				values: tt.fields.values,
				name:   tt.fields.name,
				err:    tt.fields.err,
			}
			if got := df.Filter(tt.args.filters...); !reflect.DeepEqual(got, tt.want) {
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
		lambda ApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"float64 on all columns", fields{
			values: []*valueContainer{
				{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
				{slice: []float64{1}, isNull: []bool{false}, name: "bar"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{ApplyFn{F64: func(v float64) float64 {
				return v * 2
			}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{0}, isNull: []bool{false}, name: "foo"},
					{slice: []float64{2}, isNull: []bool{false}, name: "bar"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:   "baz"},
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
			if got := df.Apply(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Sort(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
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
		{"float64 on one column", fields{
			values: []*valueContainer{
				{slice: []float64{0, 2, 1}, isNull: []bool{false, false, false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0, 1, 2}, isNull: []bool{false, false, false}, name: "*0"}},
			name:   "baz"},
			args{[]Sorter{{ColName: "foo"}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{0, 1, 2}, isNull: []bool{false, false, false}, name: "foo"}},
				labels: []*valueContainer{{slice: []int{0, 2, 1}, isNull: []bool{false, false, false}, name: "*0"}},
				name:   "baz"},
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
			if got := df.Sort(tt.args.by...); !reflect.DeepEqual(got, tt.want) {
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
			if got := df.Sum(); !reflect.DeepEqual(got, tt.want) {
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
			if got := df.Mean(); !reflect.DeepEqual(got, tt.want) {
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
			if got := df.Median(); !reflect.DeepEqual(got, tt.want) {
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
			if got := df.Std(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Std() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_Lookup(t *testing.T) {
	type fields struct {
		labels []*valueContainer
		values []*valueContainer
		name   string
		err    error
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
		{"single label level, named keys, left join", fields{
			values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}, name: "waldo"}},
			labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
			name:   "qux"},
			args{
				other: &DataFrame{values: []*valueContainer{{slice: []float64{10, 20, 30}, isNull: []bool{false, false, false}, name: "corge"}},
					labels: []*valueContainer{{name: "foo", slice: []string{"qux", "quux", "bar"}, isNull: []bool{false, false, false}}}},
				how:    "left",
				leftOn: []string{"foo"}, rightOn: []string{"foo"}},
			&DataFrame{values: []*valueContainer{{slice: []float64{30, 0}, isNull: []bool{false, true}, name: "corge"}},
				labels: []*valueContainer{{name: "foo", slice: []string{"bar", "baz"}, isNull: []bool{false, false}}},
				name:   "qux"},
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
			if got := df.Lookup(tt.args.other, tt.args.how, tt.args.leftOn, tt.args.rightOn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Lookup() = %v, want %v", got.labels[0], tt.want.labels[0])
				t.Errorf(messagediff.PrettyDiff(got, tt.want))

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
			if got := df.Transpose(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.Transpose() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
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
		want   GroupedDataFrame
	}{
		{"group by all levels, with repeats", fields{
			values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}}},
			labels: []*valueContainer{
				{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
				{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
			}},
			args{nil},
			GroupedDataFrame{
				groups:      map[string][]int{"0|foo": []int{0, 1}, "1|foo": []int{2}, "2|bar": []int{3}},
				orderedKeys: []string{"0|foo", "1|foo", "2|bar"},
				df: &DataFrame{
					values: []*valueContainer{{slice: []float64{1, 2}, isNull: []bool{false, false}}},
					labels: []*valueContainer{
						{slice: []int{0, 0, 1, 2}, isNull: []bool{false, false, false, false}, name: "a"},
						{slice: []string{"foo", "foo", "foo", "bar"}, isNull: []bool{false, false, false, false}, name: "b"},
					}},
				labelNames: []string{"a", "b"},
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
			if got := df.GroupBy(tt.args.names...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.GroupBy() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))

			}
		})
	}
}

func TestDataFrame_toCSVByRows(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		colLevelNames []string
		err           error
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
		{name: "one col level",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args: args{ignoreLabels: false},
			want: [][]string{
				{"*0", "foo", "bar"},
				{"0", "1", "a"},
				{"1", "2", "b"},
			},
			wantErr: false},
		{name: "two col levels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo|baz"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar|qux"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"}},
				colLevelNames: []string{"*0", "*1"},
			},
			args: args{ignoreLabels: false},
			want: [][]string{
				{"", "foo", "bar"},
				{"*0", "baz", "qux"},
				{"0", "1", "a"},
				{"1", "2", "b"},
			}},
		{name: "two label levels",
			fields: fields{
				values: []*valueContainer{
					{slice: []int{1, 2}, isNull: []bool{false, false}, name: "foo"},
					{slice: []string{"a", "b"}, isNull: []bool{false, false}, name: "bar"}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, name: "*0"},
					{slice: []int{10, 11}, isNull: []bool{false, false}, name: "*1"},
				},
				colLevelNames: []string{"*0"},
			},
			want: [][]string{
				{"*0", "*1", "foo", "bar"},
				{"0", "10", "1", "a"},
				{"1", "11", "2", "b"},
			},
			wantErr: false},
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
			got, err := df.toCSVByRows(tt.args.ignoreLabels)
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.toCSVByRows() = %v, want %v", got, tt.want)
			}
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.toCSVByRows() err = %v, want %v", err, tt.wantErr)
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
		{"stack column", fields{
			values: []*valueContainer{
				{slice: []int{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false}, name: "foo"}},
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
					{slice: []string{"0", "1", "2", "3"}, isNull: []bool{false, false, false, false}, name: "*0"},
				},
				colLevelNames: []string{"year", "*0"},
			}},
		{"stack repeat labels", fields{
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
					{slice: []string{"1", "2", ""}, isNull: []bool{false, false, true}, name: "2018|foo"},
					{slice: []string{"", "3", "4"}, isNull: []bool{true, false, false}, name: "2019|foo"}},
				labels: []*valueContainer{
					{slice: []string{"A", "B", "C"}, isNull: []bool{false, false, false}, name: "bar"},
				},
				colLevelNames: []string{"year", "*0"},
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
			if got := df.PromoteToColLevel(tt.args.name); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.PromoteToColLevel() = %v, want %v", got.values[0], tt.want.values[0])
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

// func TestDataFrame_UnstackColLevel(t *testing.T) {
// 	type fields struct {
// 		labels        []*valueContainer
// 		values        []*valueContainer
// 		name          string
// 		err           error
// 		colLevelNames []string
// 	}
// 	type args struct {
// 		level int
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 		want   *DataFrame
// 	}{
// 		{"unstack column", fields{
// 			values: []*valueContainer{
// 				{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "2018|foo"},
// 				{slice: []string{"", "", "c", "d"}, isNull: []bool{true, true, false, false}, name: "2019|foo"}},
// 			labels: []*valueContainer{
// 				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
// 			},
// 			colLevelNames: []string{"year", "*0"},
// 		}, args{0},
// 			&DataFrame{
// 				values: []*valueContainer{
// 					{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false}, name: "foo"}},
// 				labels: []*valueContainer{
// 					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
// 					{slice: []string{"2018", "2018", "2019", "2019"}, isNull: []bool{false, false, false, false}, name: "year"},
// 				},
// 				colLevelNames: []string{"*0"},
// 			}},
// 		{"unstack column", fields{
// 			values: []*valueContainer{
// 				{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, true, true}, name: "2018|foo"},
// 				{slice: []string{"", "", "c", "d"}, isNull: []bool{true, true, false, false}, name: "2019|foo"}},
// 			labels: []*valueContainer{
// 				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
// 			},
// 			colLevelNames: []string{"year", "bar"},
// 		}, args{1},
// 			&DataFrame{
// 				values: []*valueContainer{
// 					{slice: []string{"a", "b", "", ""}, isNull: []bool{false, false, false, false}, name: "2018"},
// 					{slice: []string{"", "", "c", "d"}, isNull: []bool{false, false}, name: "2019"}},
// 				labels: []*valueContainer{
// 					{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"},
// 					{slice: []string{"foo", "foo", "foo", "foo"}, isNull: []bool{false, false}, name: "bar"}},
// 				colLevelNames: []string{"*0"}},
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			df := &DataFrame{
// 				labels:        tt.fields.labels,
// 				values:        tt.fields.values,
// 				name:          tt.fields.name,
// 				err:           tt.fields.err,
// 				colLevelNames: tt.fields.colLevelNames,
// 			}
// 			if got := df.UnstackColLevel(tt.args.level); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("DataFrame.UnstackColLevel() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

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
		{"normal", fields{
			values: []*valueContainer{
				{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "amount"},
				{slice: []float64{2018, 2018, 2019, 2019}, isNull: []bool{false, false, false, false}, name: "year"},
				{slice: []string{"A", "B", "B", "B"}, isNull: []bool{false, false, false, false}, name: "type"}},
			labels: []*valueContainer{
				{slice: []int{0, 1, 2, 3}, isNull: []bool{false, false, false, false}, name: "*0"}},
			colLevelNames: []string{"*0"}},
			args{labels: "type", columns: "year", values: "amount", aggFn: "sum"},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"1", "2"}, isNull: []bool{false, false}, name: "2018"},
				{slice: []string{"", "7"}, isNull: []bool{true, false}, name: "2019"},
			},
				labels: []*valueContainer{
					{slice: []string{"A", "B"}, isNull: []bool{false, false}, name: "type"}},
				colLevelNames: []string{"year"},
				name:          "sum"}},
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
			if got := df.PivotTable(tt.args.labels, tt.args.columns, tt.args.values, tt.args.aggFn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.PivotTable() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
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

func TestDataFrame_ApplyFormat(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		lambda ApplyFormatFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *DataFrame
	}{
		{"float64 on all columns", fields{
			values: []*valueContainer{{slice: []float64{.51}, isNull: []bool{false}, name: "foo"}},
			labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
			name:   "baz"},
			args{ApplyFormatFn{ColName: "foo", F64: func(v float64) string {
				return strconv.FormatFloat(v, 'f', 1, 64)
			}}},
			&DataFrame{
				values: []*valueContainer{{slice: []string{"0.5"}, isNull: []bool{false}, name: "foo"}},
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, name: "*0"}},
				name:   "baz"},
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
			if got := df.ApplyFormat(tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("DataFrame.ApplyFormat() = %v, want %v", got, tt.want)
			}
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
			if got := ReadMatrix(tt.args.mat); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadMatrix() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
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
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := ReadStruct(tt.args.slice)
			if (err != nil) != tt.wantErr {
				t.Errorf("ReadStruct() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("ReadStruct() = %v, want %v", got, tt.want)
			}
		})
	}
}
