package tada

import (
	"reflect"
	"testing"
)

func Test_readNestedInterfaceByRows(t *testing.T) {
	type args struct {
		rows            [][]interface{}
		requireSameType bool
	}
	tests := []struct {
		name    string
		args    args
		want    []interface{}
		wantErr bool
	}{
		{"pass - same type", args{
			[][]interface{}{
				{"foo", 0},
				{"bar", 1},
			},
			true,
		},
			[]interface{}{
				[]string{"foo", "bar"},
				[]int{0, 1},
			},
			false,
		},
		{"pass - different types", args{
			[][]interface{}{
				{"foo", 0},
				{"bar", float64(1)},
			},
			false,
		},
			[]interface{}{
				[]interface{}{"foo", "bar"},
				[]interface{}{0, float64(1)},
			},
			false,
		},
		{"fail - no rows", args{
			[][]interface{}{},
			true,
		},
			nil,
			true,
		},
		{"fail - different length rows", args{
			[][]interface{}{
				{"foo"},
				{"bar", 1},
			},
			true,
		},
			nil,
			true,
		},
		{"fail - different types when same is required", args{
			[][]interface{}{
				{"foo", "baz"},
				{"bar", 1},
			},
			true,
		},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readNestedInterfaceByRows(tt.args.rows, tt.args.requireSameType)
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
		{"pass - same type", args{
			[][]interface{}{
				{"foo", "baz"},
				{0, 1},
			}, true},
			[]interface{}{[]string{"foo", "baz"}, []int{0, 1}},
			false,
		},
		{"pass - different types", args{
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
		{"fail - different types when same is required", args{
			[][]interface{}{
				{"foo", 0},
				{"bar", 1},
			}, true},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := readNestedInterfaceByCols(tt.args.columns, tt.args.requireSameType)
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
		Rows   [][]interface{}
		IsNull [][]bool
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
				{0, float64(0)},
				{1, float64(1)},
			},
			IsNull: [][]bool{{false, true}, {false, false}},
		},
			args{
				&testSchema3{},
			},
			&testSchema3{
				Foo:     []int{0, 1},
				Bar:     []float64{0, 1},
				NullMap: [][]bool{{false, false}, {true, false}},
			},
			false,
		},
		{"fail - inconsistent types", fields{
			Rows: [][]interface{}{
				{0, "foo"},
				{1, 1},
			}},
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
			IsNull: [][]bool{{false, true}, {false, false}},
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
			IsNull: [][]bool{{false, true}, {false, false}},
		},
			args{
				&[]float64{},
			},
			&[]float64{},
			true,
		},
		{"fail - wrong type of null field", fields{
			Rows: [][]interface{}{
				{0, float64(0)},
				{1, float64(1)},
			},
			IsNull: [][]bool{{false, true}, {false, false}},
		},
			args{
				&testSchema4{},
			},
			&testSchema4{Foo: []int{0, 1}},
			true,
		},
		{"fail - wrong number of columns", fields{
			Rows: [][]interface{}{
				{0},
				{1},
			},
			IsNull: [][]bool{{false, true}, {false, false}},
		},
			args{
				&testSchema{},
			},
			&testSchema{Foo: []int{0, 1}},
			true,
		},
		{"fail - wrong data type", fields{
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
		{"fail - misshapen IsNull", fields{
			Rows: [][]interface{}{
				{0, float64(0)},
				{1, float64(1)},
			},
			IsNull: [][]bool{{false, true}, {false}},
		},
			args{
				&testSchema3{},
			},
			&testSchema3{Foo: []int{0, 1}, Bar: []float64{0, 1}},
			true,
		},
		{"fail - unevenly shaped IsNull", fields{
			Rows: [][]interface{}{
				{0, float64(0)},
				{1, float64(1)},
			},
			IsNull: [][]bool{{false, true}, {false}},
		},
			args{
				&testSchema3{},
			},
			&testSchema3{Foo: []int{0, 1}, Bar: []float64{0, 1}},
			true,
		},
		{"fail - IsNull has wrong number of columns", fields{
			Rows: [][]interface{}{
				{0, float64(0)},
				{1, float64(1)},
			},
			IsNull: [][]bool{{false}, {false}},
		},
			args{
				&testSchema3{},
			},
			&testSchema3{Foo: []int{0, 1}, Bar: []float64{0, 1}},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := StructTransposer{
				Rows:   tt.fields.Rows,
				IsNull: tt.fields.IsNull,
			}
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
	randSeed = 1
	type fields struct {
		Rows   [][]interface{}
		IsNull [][]bool
	}
	tests := []struct {
		name   string
		fields fields
		want   *StructTransposer
	}{
		{"pass", fields{
			Rows: [][]interface{}{
				{0},
				{1},
				{2},
				{3},
				{4},
			},
			IsNull: [][]bool{{true}, {false}, {false}, {false}, {false}},
		},
			&StructTransposer{
				Rows: [][]interface{}{
					{2},
					{0},
					{1},
					{4},
					{3},
				},
				IsNull: [][]bool{{false}, {true}, {false}, {false}, {false}},
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			st := &StructTransposer{
				Rows:   tt.fields.Rows,
				IsNull: tt.fields.IsNull,
			}
			st.Shuffle()
			if !reflect.DeepEqual(st, tt.want) {
				t.Errorf("StructTransposer.Shuffle() -> got %v, want %v", st, tt.want)
			}
		})
	}
}
