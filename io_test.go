package tada

import (
	"bytes"
	"fmt"
	"io"
	"strings"
	"testing"
)

// func TestReadCSV(t *testing.T) {
// 	type args struct {
// 		r      io.Reader
// 		config []ReadOption
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		want    *DataFrame
// 		wantErr bool
// 	}{
// 		{"1 header, 0 labels - nil config",
// 			args{strings.NewReader("Name, Age\n , 1\n bar, 2"), nil},
// 			&DataFrame{
// 				values: []*valueContainer{
// 					{slice: []string{"", "bar"}, isNull: []bool{false, false}, id: mockID, name: "Name"},
// 					{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				name:          "",
// 				colLevelNames: []string{"*0"}}, false},
// 		{"1 header, 0 labels - empty string as null",
// 			args{strings.NewReader("Name, Age\n , 1\n bar, 2"), []ReadOption{EmptyStringAsNull()}},
// 			&DataFrame{
// 				values: []*valueContainer{
// 					{slice: []string{"", "bar"}, isNull: []bool{true, false}, id: mockID, name: "Name"},
// 					{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				name:          "",
// 				colLevelNames: []string{"*0"}}, false},
// 		{"fail - bad reader",
// 			args{badReader{}, nil},
// 			nil, true},
// 		{"fail - bad delimiter",
// 			args{strings.NewReader("Name, Age\n foo, 1\n bar, 2"), []ReadOption{WithDelimiter(0)}},
// 			nil, true},
// 		{"fail - empty",
// 			args{strings.NewReader(""), nil},
// 			nil, true},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := ReadCSV(tt.args.r, tt.args.config...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ReadCSV() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !EqualDataFrames(got, tt.want) {
// 				t.Errorf("ReadCSV() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func TestReadCSV_File(t *testing.T) {
// 	type args struct {
// 		path   string
// 		config []ReadOption
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		want    *DataFrame
// 		wantErr bool
// 	}{
// 		{"1 header, 0 labels - nil config",
// 			args{"test_csv/1_header_0_labels.csv", nil},
// 			&DataFrame{
// 				values: []*valueContainer{
// 					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "Name"},
// 					{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				name:          "",
// 				colLevelNames: []string{"*0"}}, false},
// 		{"fail - bad delimiter",
// 			args{"test_csv/bad_delimiter.csv", nil},
// 			nil, true},
// 		{"fail - empty",
// 			args{"test_csv/empty.csv", nil},
// 			nil, true},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			f, err := os.Open(tt.args.path)
// 			if err != nil {
// 				t.Fatal(err)
// 			}
// 			got, err := ReadCSV(f, tt.args.config...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ReadCSV() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !EqualDataFrames(got, tt.want) {
// 				t.Errorf("ReadCSV() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func TestReadCSVFromRecords(t *testing.T) {
// 	type args struct {
// 		csv    [][]string
// 		config []ReadOption
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		want    *DataFrame
// 		wantErr bool
// 	}{
// 		{"1 header row, 2 columns, no index",
// 			args{
// 				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
// 				config: nil},
// 			&DataFrame{values: []*valueContainer{
// 				{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
// 				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			false},
// 		{"1 header row, 2 columns, no index, nil config",
// 			args{
// 				csv:    [][]string{{"foo", "bar"}, {"1", "5"}, {"2", "6"}},
// 				config: nil},
// 			&DataFrame{values: []*valueContainer{
// 				{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
// 				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			false},
// 		{"column as major dimension",
// 			args{
// 				csv:    [][]string{{"foo", "1", "2"}, {"bar", "5", "6"}},
// 				config: []ReadOption{ByColumn()}},
// 			&DataFrame{values: []*valueContainer{
// 				{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
// 				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			false},
// 		{"fail - no rows",
// 			args{csv: nil,
// 				config: nil},
// 			nil,
// 			true},
// 		{"fail - no columns",
// 			args{csv: [][]string{{}},
// 				config: nil},
// 			nil,
// 			true},
// 		{"fail - misaligned",
// 			args{csv: [][]string{{"foo"}, {"bar", "baz"}},
// 				config: nil},
// 			nil,
// 			true},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			got, err := ReadCSVFromRecords(tt.args.csv, tt.args.config...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ReadCSVFromRecords() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !EqualDataFrames(got, tt.want) {
// 				t.Errorf("ReadCSVFromRecords() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

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
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "0"},
					{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
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

func TestWriteMockCSV(t *testing.T) {
	want1 := `corge,qux
.5,baz
.5,baz
.5,baz
`

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
				config:     []ReadOption{ByColumn()}},
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

type badReader struct{}

func (r badReader) Read([]byte) (int, error) {
	return 0, fmt.Errorf("foo")
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

// func TestReadStruct(t *testing.T) {
// 	type fields struct {
// 		s       interface{}
// 		isSlice bool
// 	}
// 	type args struct {
// 		options []ReadOption
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    *DataFrame
// 		wantErr bool
// 	}{
// 		{"pass - default labels",
// 			fields{
// 				testSchema{
// 					Foo: []int{1, 2},
// 					Bar: []float64{3, 4},
// 				}, false,
// 			},
// 			args{nil},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
// 					{slice: []float64{3, 4}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 				},
// 				colLevelNames: []string{"*0"},
// 				name:          ""},
// 			false},
// 		{"pass - pointer with labels",
// 			fields{
// 				&testSchema{
// 					Foo: []int{1, 2},
// 					Bar: []float64{3, 4},
// 				}, false,
// 			},
// 			args{nil},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"},
// 					{slice: []float64{3, 4}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 				},
// 				colLevelNames: []string{"*0"},
// 				name:          ""},
// 			false},
// 		{"pass - default labels - no tags",
// 			fields{
// 				testSchema2{
// 					Foo: []int{1, 2},
// 					Bar: []float64{3, 4},
// 				}, false,
// 			},
// 			args{nil},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "Foo"},
// 					{slice: []float64{3, 4}, isNull: []bool{false, false}, id: mockID, name: "Bar"},
// 				},
// 				colLevelNames: []string{"*0"},
// 				name:          ""},
// 			false},
// 		{"pass - supplied labels",
// 			fields{
// 				testSchema{
// 					Foo: []int{1, 2},
// 					Bar: []float64{3, 4},
// 				}, false,
// 			},
// 			args{[]ReadOption{WithLabels(1)}},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 				values: []*valueContainer{
// 					{slice: []float64{3, 4}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 				},
// 				colLevelNames: []string{"*0"},
// 				name:          ""},
// 			false},
// 		{"pass - is slice",
// 			fields{
// 				[]testStruct{{"foo", 1}, {"(null)", 2}},
// 				true,
// 			},
// 			args{nil},
// 			&DataFrame{
// 				values: []*valueContainer{
// 					{slice: []string{"foo", "(null)"}, isNull: []bool{false, true}, id: mockID, name: "Name"},
// 					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				name:          "",
// 				colLevelNames: []string{"*0"}},
// 			false,
// 		},
// 		{"fail - is slice - bad input",
// 			fields{
// 				"foo", true,
// 			},
// 			args{nil},
// 			nil,
// 			true,
// 		},
// 		// {"pass - null table",
// 		// 	args{
// 		// 		testSchema3{
// 		// 			Foo:     []int{0, 2},
// 		// 			Bar:     []float64{3, 4},
// 		// 			NullMap: [][]bool{{true, false}, {false, false}},
// 		// 		}, nil},
// 		// 	&DataFrame{
// 		// 		labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 		// 		values: []*valueContainer{
// 		// 			{slice: []int{0, 2}, isNull: []bool{true, false}, id: mockID, name: "foo"},
// 		// 			{slice: []float64{3, 4}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 		// 		},
// 		// 		colLevelNames: []string{"*0"},
// 		// 		name:          ""},
// 		// 	false},
// 		// {"pass - null table - with index",
// 		// 	args{
// 		// 		testSchema3{
// 		// 			Foo:     []int{0, 2},
// 		// 			Bar:     []float64{3, 4},
// 		// 			NullMap: [][]bool{{true, false}, {false, false}},
// 		// 		}, []ReadOption{WithLabels(1)}},
// 		// 	&DataFrame{
// 		// 		labels: []*valueContainer{{slice: []int{0, 2}, isNull: []bool{true, false}, id: mockID, name: "foo"}},
// 		// 		values: []*valueContainer{
// 		// 			{slice: []float64{3, 4}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 		// 		},
// 		// 		colLevelNames: []string{"*0"},
// 		// 		name:          ""},
// 		// 	false},
// 		// {"fail - null table of wrong type",
// 		// 	args{
// 		// 		testSchema4{
// 		// 			Foo:     []int{0, 2},
// 		// 			NullMap: [][]int{{0, 1}, {1, 2}},
// 		// 		}, nil},
// 		// 	nil,
// 		// 	true},
// 		// {"fail - null table with wrong length",
// 		// 	args{
// 		// 		testSchema3{
// 		// 			Foo:     []int{0, 2},
// 		// 			Bar:     []float64{3, 4},
// 		// 			NullMap: [][]bool{{true, false}, {false}},
// 		// 		}, nil},
// 		// 	nil,
// 		// 	true},
// 		// {"fail - nil values",
// 		// 	args{
// 		// 		testSchema{
// 		// 			Foo: []int{1, 2},
// 		// 		}, nil},
// 		// 	nil,
// 		// 	true},
// 		{"fail - not struct",
// 			fields{
// 				[]int{}, false,
// 			},
// 			args{nil},
// 			nil,
// 			true},
// 		{"fail - uneven lengths",
// 			fields{
// 				testSchema{
// 					Foo: []int{1, 2},
// 					Bar: []float64{3, 4, 5},
// 				}, false,
// 			},
// 			args{nil},
// 			nil,
// 			true},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			reader := NewStructReader(tt.fields.s, tt.fields.isSlice)
// 			got, err := reader.Read(tt.args.options...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ReadStruct() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !EqualDataFrames(got, tt.want) {
// 				t.Errorf("ReadStruct() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func TestDataFrame_Struct(t *testing.T) {
// 	type fields struct {
// 		labels        []*valueContainer
// 		values        []*valueContainer
// 		name          string
// 		err           error
// 		colLevelNames []string
// 	}
// 	type args struct {
// 		structPointer interface{}
// 		options       []WriteOption
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    interface{}
// 		wantErr bool
// 	}{
// 		{"pass - match tag names", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema{}, nil},
// 			&testSchema{
// 				Foo: []int{0, 1},
// 				Bar: []float64{0, 1},
// 			},
// 			false,
// 		},
// 		{"pass - match exported names", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Foo"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "Bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema2{}, nil},
// 			&testSchema2{
// 				Foo: []int{0, 1},
// 				Bar: []float64{0, 1},
// 			},
// 			false,
// 		},
// 		{"pass - ignore label names", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 			values: []*valueContainer{
// 				{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"},
// 				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema{}, []WriteOption{ExcludeLabels()}},
// 			&testSchema{
// 				Foo: []int{0, 1},
// 				Bar: []float64{0, 1},
// 			},
// 			false,
// 		},
// 		{"pass - null table", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema3{}, nil},
// 			&testSchema3{
// 				Foo:     []int{0, 1},
// 				NullMap: [][]bool{{false, false}, {true, false}},
// 				Bar:     []float64{0, 1},
// 			},
// 			false,
// 		},
// 		{"fail - not pointer", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Bar"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Foo"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{testSchema{}, nil},
// 			testSchema{},
// 			true,
// 		},

// 		{"fail - not pointer to struct", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Bar"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "Foo"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&[]float64{}, nil},
// 			&[]float64{},
// 			true,
// 		},
// 		{"fail - not enough containers", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Foo"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "Bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema5{}, nil},
// 			&testSchema5{
// 				Foo: []int{0, 1},
// 				Bar: []float64{0, 1},
// 			},
// 			true,
// 		},
// 		{"fail - wrong order", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Bar"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "Foo"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema{}, nil},
// 			&testSchema{},
// 			true,
// 		},
// 		{"fail - does not match exported name or tag name", fields{
// 			labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "corge"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema{}, nil},
// 			&testSchema{},
// 			true,
// 		},
// 		{"fail - does not match field type", fields{
// 			labels: []*valueContainer{{slice: []float64{0, 1}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 			values: []*valueContainer{
// 				{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "bar"},
// 			},
// 			colLevelNames: []string{"*0"}},
// 			args{&testSchema{}, nil},
// 			&testSchema{},
// 			true,
// 		},
// 		{"fail - null table of wrong type",
// 			fields{
// 				labels: []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "Foo"}},
// 				values: []*valueContainer{
// 					{slice: []float64{0, 1}, isNull: []bool{true, false}, id: mockID, name: "Bar"},
// 				},
// 				colLevelNames: []string{"*0"}},
// 			args{&testSchema4{}, nil},
// 			&testSchema4{Foo: []int{0, 1}},
// 			true},
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
// 			err := df.Struct(tt.args.structPointer, tt.args.options...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("DataFrame.Struct() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !reflect.DeepEqual(tt.args.structPointer, tt.want) {
// 				t.Errorf("DataFrame.Struct() -> %v, want %v", tt.args.structPointer, tt.want)
// 			}
// 		})
// 	}
// }

// func TestReadInterfaceFromRecords(t *testing.T) {
// 	type args struct {
// 		records [][]interface{}
// 		options []ReadOption
// 	}
// 	tests := []struct {
// 		name    string
// 		args    args
// 		wantRet *DataFrame
// 		wantErr bool
// 	}{
// 		{"pass - major dim rows - 1 header",
// 			args{
// 				[][]interface{}{
// 					{"foo", "bar"},
// 					{float64(1), float64(2)},
// 				}, nil},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []interface{}{float64(1)}, isNull: []bool{false}, id: mockID, name: "foo"},
// 					{slice: []interface{}{float64(2)}, isNull: []bool{false}, id: mockID, name: "bar"},
// 				},
// 				colLevelNames: []string{"*0"}},
// 			false,
// 		},
// 		{"pass - major dim cols - 1 header",
// 			args{
// 				[][]interface{}{
// 					{"foo", float64(1)},
// 					{"bar", float64(2)},
// 				}, []ReadOption{ByColumn()}},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []interface{}{float64(1)}, isNull: []bool{false}, id: mockID, name: "foo"},
// 					{slice: []interface{}{float64(2)}, isNull: []bool{false}, id: mockID, name: "bar"},
// 				},
// 				colLevelNames: []string{"*0"}},
// 			false,
// 		},
// 		{"pass - major dim cols - 1 header - not string",
// 			args{
// 				[][]interface{}{
// 					{0, float64(1)},
// 					{1, float64(2)},
// 				}, []ReadOption{ByColumn()}},
// 			&DataFrame{
// 				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []interface{}{float64(1)}, isNull: []bool{false}, id: mockID, name: "0"},
// 					{slice: []interface{}{float64(2)}, isNull: []bool{false}, id: mockID, name: "1"},
// 				},
// 				colLevelNames: []string{"*0"}},
// 			false,
// 		},
// 		{"pass - major dim cols - 1 label",
// 			args{
// 				[][]interface{}{
// 					{"foo", float64(1)},
// 					{"bar", float64(2)},
// 				}, []ReadOption{ByColumn(), WithLabels(1)}},
// 			&DataFrame{
// 				labels: []*valueContainer{
// 					{slice: []interface{}{float64(1)}, isNull: []bool{false}, id: mockID, name: "foo"}},
// 				values: []*valueContainer{
// 					{slice: []interface{}{float64(2)}, isNull: []bool{false}, id: mockID, name: "bar"},
// 				},
// 				colLevelNames: []string{"*0"}},
// 			false,
// 		},
// 		{"pass - major dim cols - 0 headers",
// 			args{
// 				[][]interface{}{
// 					{float64(1)},
// 					{float64(2)},
// 				}, []ReadOption{ByColumn(), WithHeaders(0)}},
// 			&DataFrame{
// 				labels: []*valueContainer{
// 					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
// 				values: []*valueContainer{
// 					{slice: []interface{}{float64(1)}, isNull: []bool{false}, id: mockID, name: "0"},
// 					{slice: []interface{}{float64(2)}, isNull: []bool{false}, id: mockID, name: "1"},
// 				},
// 				colLevelNames: []string{"*0"}},
// 			false,
// 		},
// 		{"fail - no records",
// 			args{
// 				[][]interface{}{}, nil},
// 			nil, true,
// 		},
// 		{"fail - first record empty",
// 			args{
// 				[][]interface{}{{}, {0}}, nil},
// 			nil, true,
// 		},
// 		{"fail - unevenly shaped",
// 			args{
// 				[][]interface{}{{"foo"}, {1, 2}}, nil},
// 			nil, true,
// 		},
// 	}
// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			gotRet, err := ReadInterfaceFromRecords(tt.args.records, tt.args.options...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("ReadInterfaceFromRecords() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if !EqualDataFrames(gotRet, tt.wantRet) {
// 				t.Errorf("ReadInterfaceFromRecords() = %v, want %v", gotRet, tt.wantRet)
// 			}
// 		})
// 	}
// }

// func TestDataFrame_CSVRecords(t *testing.T) {
// 	type fields struct {
// 		labels        []*valueContainer
// 		values        []*valueContainer
// 		name          string
// 		err           error
// 		colLevelNames []string
// 	}
// 	type args struct {
// 		options []WriteOption
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    [][]string
// 		wantErr bool
// 	}{
// 		{"pass",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{},
// 			[][]string{{"*0", "foo"}, {"0", "a"}, {"1", "b"}}, false},
// 		{"pass - ignore labels",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{[]WriteOption{ExcludeLabels()}},
// 			[][]string{{"foo"}, {"a"}, {"b"}}, false},
// 		{"fail",
// 			fields{values: nil,
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{},
// 			nil, true},
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
// 			got := df.CSVRecords(tt.args.options...)
// 			if !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("DataFrame.CSVRecords() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// func TestDataFrame_InterfaceRecords(t *testing.T) {
// 	type fields struct {
// 		labels        []*valueContainer
// 		values        []*valueContainer
// 		name          string
// 		err           error
// 		colLevelNames []string
// 	}
// 	type args struct {
// 		options []WriteOption
// 	}
// 	tests := []struct {
// 		name   string
// 		fields fields
// 		args   args
// 		want   [][]interface{}
// 	}{
// 		{"pass",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", ""}, isNull: []bool{false, true}, id: mockID, name: "foo"},
// 				{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 			},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{nil},
// 			[][]interface{}{{"*0", 0, 1}, {"foo", "a", "(null)"}, {"bar", 1, "qux"}}},
// 		{"pass - exclude labels",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", ""}, isNull: []bool{false, true}, id: mockID, name: "foo"},
// 				{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
// 			},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{[]WriteOption{ExcludeLabels()}},
// 			[][]interface{}{{"foo", "a", "(null)"}, {"bar", 1, "qux"}}},
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
// 			if got := df.InterfaceRecords(tt.args.options...); !reflect.DeepEqual(got, tt.want) {
// 				t.Errorf("DataFrame.InterfaceRecords() = %v, want %v", got, tt.want)
// 			}
// 		})
// 	}
// }

// type badWriter struct{}

// func (w badWriter) Write([]byte) (int, error) {
// 	return 0, fmt.Errorf("foo")
// }

// func TestDataFrame_WriteCSV(t *testing.T) {
// 	type fields struct {
// 		labels        []*valueContainer
// 		values        []*valueContainer
// 		name          string
// 		err           error
// 		colLevelNames []string
// 	}
// 	type args struct {
// 		w       io.Writer
// 		options []WriteOption
// 	}
// 	tests := []struct {
// 		name    string
// 		fields  fields
// 		args    args
// 		want    string
// 		wantErr bool
// 	}{
// 		{"pass",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{new(bytes.Buffer), nil},
// 			"*0,foo\n0,a\n1,b\n",
// 			false},
// 		{"pass - delimiter",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{new(bytes.Buffer), []WriteOption{Delimiter('|')}},
// 			"*0|foo\n0|a\n1|b\n",
// 			false},
// 		{"pass - exclude labels",
// 			fields{values: []*valueContainer{
// 				{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
// 				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 				colLevelNames: []string{"*0"}},
// 			args{new(bytes.Buffer), []WriteOption{ExcludeLabels()}},
// 			"foo\na\nb\n",
// 			false},
// 		{"fail - bad writer", fields{values: nil,
// 			labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
// 			colLevelNames: []string{"*0"}},
// 			args{badWriter{}, nil}, "", true},
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
// 			w := tt.args.w
// 			err := df.WriteCSV(w, tt.args.options...)
// 			if (err != nil) != tt.wantErr {
// 				t.Errorf("DataFrame.WriteCSV() error = %v, wantErr %v", err, tt.wantErr)
// 				return
// 			}
// 			if tt.wantErr == false {
// 				if w.(*bytes.Buffer).String() != tt.want {
// 					t.Errorf("DataFrame.WriteCSV() -> w = %v, want %v", w.(*bytes.Buffer).String(), tt.want)
// 				}
// 			}

// 		})
// 	}
// }
