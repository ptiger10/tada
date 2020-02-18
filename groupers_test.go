package tada

import (
	"errors"
	"reflect"
	"testing"

	"github.com/d4l3k/messagediff"
)

func TestGroupedSeries_Err(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	tests := []struct {
		name    string
		fields  fields
		wantErr bool
	}{
		{"error", fields{err: errors.New("foo")}, true},
		{"no error", fields{
			groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
			orderedKeys: []string{"foo", "bar"},
			levelNames:  []string{"*0"},
			series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}}, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if err := g.Err(); (err != nil) != tt.wantErr {
				t.Errorf("GroupedSeries.Err() error = %v, wantErr %v", err, tt.wantErr)
			}
		})
	}
}

func TestGroupedSeries_stringFunc(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		rowIndices  [][]int
		newLabels   []*valueContainer
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	type args struct {
		name string
		fn   func(val []string, isNull []bool, index []int) (string, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				rowIndices:  [][]int{{0, 1}, {2, 3}},
				newLabels:   []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}},
				orderedKeys: []string{"foo", "bar"},
				levelNames:  []string{"*0"},
				series: &Series{values: &valueContainer{slice: []string{"a", "b", "c", "d"}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args: args{"first", func(vals []string, isNull []bool, index []int) (string, bool) {
				for _, i := range index {
					return vals[i], false
				}
				return "", true
			}},
			want: &Series{values: &valueContainer{slice: []string{"a", "c"}, isNull: []bool{false, false}, name: "first"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				rowIndices:  tt.fields.rowIndices,
				newLabels:   tt.fields.newLabels,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.stringFunc(tt.args.name, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.stringFunc() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Sum(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "single level - not aligned",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
				orderedKeys: []string{"foo", "bar"},
				levelNames:  []string{"*0"},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "single level - aligned",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
				orderedKeys: []string{"foo", "bar"},
				levelNames:  []string{"*0"},
				aligned:     true,
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{
				slice: []float64{3, 3, 7, 7}, isNull: []bool{false, false, false, false}, name: "qux_sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
				}}},
		{
			name: "two levels - not aligned",
			fields: fields{
				groups:      map[string][]int{"foo|0": []int{0, 1}, "bar|0": []int{2, 3}},
				orderedKeys: []string{"foo|0", "bar|0"},
				levelNames:  []string{"*0", "*1"},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"},
						{slice: []int{0, 0, 0, 0}, isNull: []bool{false, false, false, false}, name: "*1"}}}},
			want: &Series{values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"},
					{slice: []string{"0", "0"}, isNull: []bool{false, false}, name: "*1"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Sum(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Sum() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestGroupedSeries_Mean(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		levelNames  []string
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
				levelNames:  []string{"*0"},
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "mean"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				levelNames:  tt.fields.levelNames,
				series:      tt.fields.series,
				err:         tt.fields.err,
			}
			if got := g.Mean(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Mean() = %v, want %v", got.labels, tt.want.labels)
			}
		})
	}
}

func TestGroupedSeries_Median(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		levelNames  []string
		series      *Series
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{
			name: "even",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
				orderedKeys: []string{"foo", "bar"},
				levelNames:  []string{"*0"},
				series: &Series{
					values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"},
							isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{1.5, 3.5}, isNull: []bool{false, false}, name: "median"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
		{
			name: "odd",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1, 2}, "bar": []int{3, 4, 5}},
				orderedKeys: []string{"foo", "bar"},
				levelNames:  []string{"*0"},
				series: &Series{
					values: &valueContainer{
						slice: []float64{1, 2, 4, 5, 6, 8}, isNull: []bool{false, false, false, false, false, false}},
					labels: []*valueContainer{{
						slice:  []string{"foo", "foo", "foo", "bar", "bar", "bar"},
						isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{2, 6}, isNull: []bool{false, false}, name: "median"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				levelNames:  tt.fields.levelNames,
				series:      tt.fields.series,
				err:         tt.fields.err,
			}
			if got := g.Median(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Median() = %v, want %v", got.labels, tt.want.labels)
			}
		})
	}
}

func TestGroupedSeries_Std(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		levelNames  []string
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
				levelNames:  []string{"*0"},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4},
					isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []float64{.5, .5}, isNull: []bool{false, false}, name: "std"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				levelNames:  tt.fields.levelNames,
				series:      tt.fields.series,
				err:         tt.fields.err,
			}
			if got := g.Std(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Std() = %v, want %v", got.labels, tt.want.labels)
			}
		})
	}
}

func TestGroupedDataFrame_Sum(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		df          *DataFrame
		levelNames  []string
		err         error
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
		{
			name: "single level, all colNames",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
				orderedKeys: []string{"foo", "bar"},
				df: &DataFrame{values: []*valueContainer{
					{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "corge"},
					{slice: []float64{5, 6, 7, 8}, isNull: []bool{false, false, false, false}, name: "waldo"},
				},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "baz"}}},
				levelNames: []string{"baz"}},
			args: args{nil},
			want: &DataFrame{values: []*valueContainer{
				{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "corge"},
				{slice: []float64{11, 15}, isNull: []bool{false, false}, name: "waldo"},
			},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "baz"}},
				name:   "sum"}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedDataFrame{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				df:          tt.fields.df,
				levelNames:  tt.fields.levelNames,
				err:         tt.fields.err,
			}
			if got := g.Sum(tt.args.colNames...); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedDataFrame.Sum() = %v, want %v", got.values[0], tt.want.values[0])
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}

func TestGroupedSeries_First(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *Series
	}{
		{name: "single level",
			fields: fields{
				groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
				orderedKeys: []string{"foo", "bar"},
				levelNames:  []string{"*0"},
				series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}},
					labels: []*valueContainer{
						{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			want: &Series{values: &valueContainer{slice: []string{"1", "3"}, isNull: []bool{false, false}, name: "first"},
				labels: []*valueContainer{{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				err:         tt.fields.err,
			}
			if got := g.First(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.First() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_alignedMath(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	type args struct {
		name string
		fn   func(val []float64, isNull []bool, index []int) (float64, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"pass", fields{
			groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
			orderedKeys: []string{"foo", "bar"},
			levelNames:  []string{"*0"},
			series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"qux_sum", sum},
			&Series{values: &valueContainer{slice: []float64{3, 3, 7, 7}, isNull: []bool{false, false, false, false}, name: "qux_sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.alignedMath(tt.args.name, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.alignedMath() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Align(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	tests := []struct {
		name   string
		fields fields
		want   *GroupedSeries
	}{
		{"pass", fields{orderedKeys: []string{"foo"}, aligned: false}, &GroupedSeries{orderedKeys: []string{"foo"}, aligned: true}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Align(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Align() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_Apply(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	type args struct {
		name   string
		lambda GroupApplyFn
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"aligned", fields{
			groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
			orderedKeys: []string{"foo", "bar"},
			levelNames:  []string{"*0"},
			aligned:     true,
			series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"qux_sum", GroupApplyFn{F64: func(vals []float64) float64 {
				var sum float64
				for i := range vals {
					sum += vals[i]
				}
				return sum
			}}},
			&Series{values: &valueContainer{slice: []float64{3, 3, 7, 7}, isNull: []bool{false, false, false, false}, name: "qux_sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.Apply(tt.args.name, tt.args.lambda); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.Apply() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestGroupedSeries_mathFunc(t *testing.T) {
	type fields struct {
		groups      map[string][]int
		orderedKeys []string
		series      *Series
		levelNames  []string
		aligned     bool
		err         error
	}
	type args struct {
		name string
		fn   func(val []float64, isNull []bool, index []int) (float64, bool)
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   *Series
	}{
		{"sum", fields{
			groups:      map[string][]int{"foo": []int{0, 1}, "bar": []int{2, 3}},
			orderedKeys: []string{"foo", "bar"},
			levelNames:  []string{"*0"},
			aligned:     false,
			series: &Series{values: &valueContainer{slice: []float64{1, 2, 3, 4}, isNull: []bool{false, false, false, false}, name: "qux"},
				labels: []*valueContainer{
					{slice: []string{"foo", "foo", "bar", "bar"}, isNull: []bool{false, false, false, false}, name: "*0"}}}},
			args{"sum", func(val []float64, isNull []bool, index []int) (float64, bool) {
				var sum float64
				for _, i := range index {
					sum += val[i]
				}
				return sum, false
			}},
			&Series{values: &valueContainer{slice: []float64{3, 7}, isNull: []bool{false, false}, name: "sum"},
				labels: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, name: "*0"}}}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			g := &GroupedSeries{
				groups:      tt.fields.groups,
				orderedKeys: tt.fields.orderedKeys,
				series:      tt.fields.series,
				levelNames:  tt.fields.levelNames,
				aligned:     tt.fields.aligned,
				err:         tt.fields.err,
			}
			if got := g.mathFunc(tt.args.name, tt.args.fn); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("GroupedSeries.mathFunc() = %v, want %v", got, tt.want)
				t.Errorf(messagediff.PrettyDiff(got, tt.want))
			}
		})
	}
}
