package tada

import (
	"math"
	"reflect"
	"testing"
)

func Test_readNestedInterfaceByRowsInferType(t *testing.T) {
	type args struct {
		rows [][]interface{}
	}
	tests := []struct {
		name       string
		args       args
		wantRet    []interface{}
		wantIsNull [][]bool
		wantErr    bool
	}{
		{"pass",
			args{
				[][]interface{}{
					{"foo", 0},
					{"bar", 1},
				},
			},
			[]interface{}{
				[]string{"foo", "bar"},
				[]int{0, 1},
			},
			[][]bool{{false, false}, {false, false}},
			false,
		},
		{"pass - mixed types as []interface{}",
			args{
				[][]interface{}{
					{"foo", 0},
					{"bar", .5},
				},
			},
			[]interface{}{
				[]string{"foo", "bar"},
				[]interface{}{0, .5},
			},
			[][]bool{{false, false}, {false, false}},
			false,
		},
		{"pass - mixed types as []interface{} - with null",
			args{
				[][]interface{}{
					{"foo", 0},
					{"bar", ""},
					{"baz", .5},
				},
			},
			[]interface{}{
				[]string{"foo", "bar", "baz"},
				[]interface{}{0, nil, .5},
			},
			[][]bool{{false, false, false}, {false, true, false}},
			false,
		},
		{"pass - nulls after first row", args{
			[][]interface{}{
				{"foo", 1},
				{"bar", nil},
			},
		},
			[]interface{}{
				[]string{"foo", "bar"},
				[]int{1, 0},
			},
			[][]bool{{false, false}, {false, true}},
			false,
		},
		{"pass - nulls in first row", args{
			[][]interface{}{
				{"foo", nil},
				{"bar", 1},
			},
		},
			[]interface{}{
				[]string{"foo", "bar"},
				[]int{0, 1},
			},
			[][]bool{{false, false}, {true, false}},
			false,
		},
		{"pass - all nulls, as string", args{
			[][]interface{}{
				{"foo", ""},
				{"bar", nil},
			},
		},
			[]interface{}{
				[]string{"foo", "bar"},
				[]string{"", ""},
			},
			[][]bool{{false, false}, {true, true}},
			false,
		},
		{"pass - all nulls, as float", args{
			[][]interface{}{
				{"foo", math.NaN()},
				{"bar", nil},
			},
		},
			[]interface{}{
				[]string{"foo", "bar"},
				[]float64{0, 0},
			},
			[][]bool{{false, false}, {true, true}},
			false,
		},
		{"pass - all nils, as []interface", args{
			[][]interface{}{
				{"foo", nil},
				{"bar", nil},
			},
		},
			[]interface{}{
				[]string{"foo", "bar"},
				[]interface{}{nil, nil},
			},
			[][]bool{{false, false}, {true, true}},
			false,
		},
		{"fail - no rows", args{
			[][]interface{}{},
		},
			nil,
			nil,
			true,
		},
		{"fail - different length rows", args{
			[][]interface{}{
				{"foo"},
				{"bar", 1},
			},
		},
			nil,
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			gotRet, gotIsNull, err := readNestedInterfaceByRowsInferType(tt.args.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("readNestedInterfaceByRowsInferType() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(gotRet, tt.wantRet) {
				t.Errorf("readNestedInterfaceByRowsInferType() gotRet = %v, want %v", gotRet, tt.wantRet)
			}
			if !reflect.DeepEqual(gotIsNull, tt.wantIsNull) {
				t.Errorf("readNestedInterfaceByRowsInferType() gotIsNull = %v, want %v", gotIsNull, tt.wantIsNull)
			}
		})
	}
}

func Test_readNestedInterfaceByRows(t *testing.T) {
	type args struct {
		rows [][]interface{}
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{"pass", args{
			[][]interface{}{
				{"foo", 0},
				{"bar", "baz"},
			},
		},
			[]interface{}{
				[]interface{}{"foo", "bar"},
				[]interface{}{0, "baz"},
			},
			false,
		},

		{"fail - no rows", args{
			[][]interface{}{},
		},
			nil,
			true,
		},
		{"fail - different length rows", args{
			[][]interface{}{
				{"foo"},
				{"bar", 1},
			},
		},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readNestedInterfaceByRows(tt.args.rows)
			if (err != nil) != tt.wantErr {
				t.Errorf("readNestedInterfaceByRows() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readNestedInterfaceByRows() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_readNestedInterfaceByCols(t *testing.T) {
	type args struct {
		columns         [][]interface{}
		requireSameType bool
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{"pass", args{
			[][]interface{}{
				{"foo", 0},
				{"bar", 1},
			}, false},
			[]interface{}{[]interface{}{"foo", 0}, []interface{}{"bar", 1}},
			false,
		},
		{"fail - no columns", args{
			[][]interface{}{}, true},
			nil,
			true,
		},
		{"fail - different length columns", args{
			[][]interface{}{
				{"foo", "baz"},
				{0},
			}, true},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readNestedInterfaceByCols(tt.args.columns)
			if (err != nil) != tt.wantErr {
				t.Errorf("readNestedInterfaceByCols() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("readNestedInterfaceByCols() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_transposeNestedNulls(t *testing.T) {
	type args struct {
		isNull [][]bool
	}
	tests := []struct {
		name    string
		args    args
		want    [][]bool
		wantErr bool
	}{
		{"pass", args{
			[][]bool{{false, false, true}, {true, false, false}},
		},
			[][]bool{{false, true}, {false, false}, {true, false}},
			false,
		},
		{"nil empty", args{
			[][]bool{},
		},
			nil,
			false,
		},
		{"fail - wrong shape", args{
			[][]bool{{false, false}, {true}},
		},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := transposeNestedNulls(tt.args.isNull)
			if (err != nil) != tt.wantErr {
				t.Errorf("transposeNestedNulls() error = %v, want %v", got, tt.want)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("transposeNestedNulls() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStructTransposer_Transpose(t *testing.T) {
	type fields struct {
		Rows [][]interface{}
	}
	type args struct {
		structPointer interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{

		{"pass", fields{
			Rows: [][]interface{}{
				{0, float64(1)},
				{1, float64(2)},
			}},
			args{
				&testSchema{},
			},
			&testSchema{
				Foo: []int{0, 1},
				Bar: []float64{1, 2},
			},
			false,
		},
		{"pass - nulls", fields{
			Rows: [][]interface{}{
				{0, float64(1)},
				{1, ""},
			},
		},
			args{
				&testSchema3{},
			},
			&testSchema3{
				Foo:     []int{0, 1},
				Bar:     []float64{1, 0},
				NullMap: [][]bool{{false, false}, {false, true}},
			},
			false,
		},
		{"pass - []interface{}", fields{
			Rows: [][]interface{}{
				{0, "foo"},
				{1, 1},
			}},
			args{
				&testSchema6{},
			},
			&testSchema6{
				Foo: []int{0, 1},
				Bar: []interface{}{"foo", 1},
			},
			false,
		},
		{"fail - misshapen columns", fields{
			Rows: [][]interface{}{
				{0, 1},
				{1},
			},
		},
			args{
				&testSchema{},
			},
			&testSchema{},
			true,
		},
		{"fail - not pointer", fields{
			Rows: [][]interface{}{
				{0, float64(0)},
				{1, float64(1)},
			},
		},
			args{
				testSchema3{},
			},
			testSchema3{},
			true,
		},
		{"fail - not struct", fields{
			Rows: [][]interface{}{
				{0, float64(0)},
				{1, float64(1)},
			},
		},
			args{
				&[]float64{},
			},
			&[]float64{},
			true,
		},
		{"fail - struct has null tag, but it is wrong type to receive nulls", fields{
			Rows: [][]interface{}{
				{0},
				{1},
			},
		},
			args{
				&testSchema4{},
			},
			&testSchema4{Foo: []int{0, 1}},
			true,
		},
		{"fail - wrong number of columns for struct", fields{
			Rows: [][]interface{}{
				{0},
				{1},
			},
		},
			args{
				&testSchema{},
			},
			&testSchema{Foo: []int{0, 1}},
			true,
		},
		{"fail - wrong data type for struct", fields{
			Rows: [][]interface{}{
				{0, "foo"},
				{1, "bar"},
			}},
			args{
				&testSchema{},
			},
			&testSchema{Foo: []int{0, 1}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := StructTransposer(tt.fields.Rows)
			if err := st.Transpose(tt.args.structPointer); (err != nil) != tt.wantErr {
				t.Errorf("StructTransposer.Transpose() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(tt.args.structPointer, tt.want) {
				t.Errorf("StructTransposer.Transpose() -> %v, want %v", tt.args.structPointer, tt.want)

			}
		})
	}
}

func TestStructTransposer_Shuffle(t *testing.T) {
	type fields struct {
		Rows [][]interface{}
	}
	type args struct {
		seed int64
	}
	tests := []struct {
		name   string
		fields fields
		args   args
		want   [][]interface{}
	}{
		{"pass", fields{
			Rows: [][]interface{}{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
		},
			args{1},
			[][]interface{}{
				{2},
				{0},
				{1},
				{4},
				{3},
			},
		},
		{"pass - no nulls", fields{
			Rows: [][]interface{}{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
		},
			args{1},

			[][]interface{}{
				{2},
				{0},
				{1},
				{4},
				{3},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := StructTransposer(tt.fields.Rows)
			st.Shuffle(tt.args.seed)
			if !reflect.DeepEqual([][]interface{}(st), tt.want) {
				t.Errorf("StructTransposer.Shuffle() -> got %v, want %v", [][]interface{}(st), tt.want)
			}
		})
	}
}
