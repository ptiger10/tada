package tada

import (
	"bytes"
	"encoding/csv"
	"reflect"
	"strings"
	"testing"

	"cloud.google.com/go/civil"
	"github.com/ptiger10/tablediff"
)

func TestSliceReader_Read(t *testing.T) {
	type fields struct {
		ColumnSlices []interface{}
		LabelSlices  []interface{}
		ColumnNames  []string
		LabelNames   []string
		Name         string
		NumColLevels int
	}
	tests := []struct {
		name    string
		fields  fields
		want    *DataFrame
		wantErr bool
	}{
		{"pass - supplied values and labels - default names",
			fields{
				ColumnSlices: []interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
				LabelSlices:  []interface{}{[]string{"a", "b"}},
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "0"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "1"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"pass - supplied values and labels - multiple column levels",
			fields{
				ColumnSlices: []interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
				LabelSlices:  []interface{}{[]string{"a", "b"}},
				NumColLevels: 2,
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "0"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "1"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0", "*1"}},
			false,
		},
		{"pass - supplied values and labels - supplied names",
			fields{
				ColumnSlices: []interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
				LabelSlices:  []interface{}{[]string{"a", "b"}},
				ColumnNames:  []string{"A", "B"},
				LabelNames:   []string{"qux"},
				Name:         "foobar",
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "A"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "B"}},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "qux"}},
				colLevelNames: []string{"*0"},
				name:          "foobar"},
			false,
		},
		{"pass - default labels", fields{
			ColumnSlices: []interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
		},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1, 2}, isNull: []bool{false, false}, id: mockID, name: "0"},
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"pass - default values", fields{
			LabelSlices: []interface{}{[]string{"a", "b"}},
		},
			&DataFrame{
				values:        []*valueContainer{},
				labels:        []*valueContainer{{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - slices and labels nil", fields{},
			nil,
			true,
		},
		{"fail - empty slice", fields{
			ColumnSlices: []interface{}{[]string{}},
		},
			nil,
			true,
		},
		{"fail - unsupported kind", fields{
			ColumnSlices: []interface{}{"foo"}},
			nil,
			true,
		},
		{"fail - unsupported label kind", fields{
			ColumnSlices: []interface{}{[]float64{1}},
			LabelSlices:  []interface{}{"foo"}},
			nil,
			true,
		},
		{"fail - wrong length of column names",
			fields{
				ColumnSlices: []interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
				LabelSlices:  []interface{}{[]string{"a", "b"}},
				ColumnNames:  []string{"A"},
				LabelNames:   []string{"qux"},
			},
			nil,
			true,
		},
		{"fail - wrong length of label names",
			fields{
				ColumnSlices: []interface{}{[]float64{1, 2}, []string{"foo", "bar"}},
				LabelSlices:  []interface{}{[]string{"a", "b"}},
				ColumnNames:  []string{"A", "B"},
				LabelNames:   []string{"qux", "quz"},
			},
			nil,
			true,
		},
		{"fail - wrong length labels", fields{
			ColumnSlices: []interface{}{[]int{0}},
			LabelSlices:  []interface{}{[]string{"a", "b"}}},
			nil,
			true,
		},
		{"fail - wrong length columns", fields{
			ColumnSlices: []interface{}{[]int{0}, []string{"a", "b"}}},
			nil,
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := SliceReader{
				ColSlices:    tt.fields.ColumnSlices,
				LabelSlices:  tt.fields.LabelSlices,
				ColNames:     tt.fields.ColumnNames,
				LabelNames:   tt.fields.LabelNames,
				Name:         tt.fields.Name,
				NumColLevels: tt.fields.NumColLevels,
			}
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("SliceReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("SliceReader.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordReader_Read(t *testing.T) {
	type fields struct {
		HeaderRows        int
		LabelLevels       int
		ByColumn          bool
		BlankStringAsNull bool
		InferTypes        bool
		records           [][]string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *DataFrame
		wantErr bool
	}{
		{"empty space as null",
			fields{
				HeaderRows:        1,
				LabelLevels:       0,
				ByColumn:          false,
				InferTypes:        false,
				BlankStringAsNull: true,
				records:           [][]string{{"foo", "bar"}, {"", "5"}, {"2", "6"}},
			},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"", "2"}, isNull: []bool{true, false}, id: mockID, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"infer types",
			fields{
				HeaderRows:  1,
				LabelLevels: 0,
				ByColumn:    false,
				InferTypes:  true,
				records:     [][]string{{"foo", "bar", "baz"}, {"qux", "2", "1/1/2020"}},
			},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"qux"}, isNull: []bool{false}, id: mockID, name: "foo", cache: []string{"qux"}},
				{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "bar", cache: []string{"2"}},
				{slice: []civil.Date{{Year: 2020, Month: 1, Day: 1}}, isNull: []bool{false}, id: mockID, name: "baz", cache: []string{"1/1/2020"}}},
				labels: []*valueContainer{
					{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"by row",
			fields{
				HeaderRows:        1,
				LabelLevels:       0,
				ByColumn:          false,
				BlankStringAsNull: false,
				records:           [][]string{{"foo", "bar"}, {"", "5"}, {"2", "6"}},
			},
			&DataFrame{values: []*valueContainer{
				{slice: []string{"", "2"}, isNull: []bool{false, false}, id: mockID, name: "foo"},
				{slice: []string{"5", "6"}, isNull: []bool{false, false}, id: mockID, name: "bar"}},
				labels: []*valueContainer{
					{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			false},
		{"fail - no rows",
			fields{records: nil},
			nil,
			true},
		{"fail - no columns",
			fields{records: [][]string{{}}},
			nil,
			true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := RecordReader{
				HeaderRows:        tt.fields.HeaderRows,
				LabelLevels:       tt.fields.LabelLevels,
				ByColumn:          tt.fields.ByColumn,
				BlankStringAsNull: tt.fields.BlankStringAsNull,
				InferTypes:        tt.fields.InferTypes,
				records:           tt.fields.records,
			}
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("RecordReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("RecordReader.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestRecordWriter_Write(t *testing.T) {
	type fields struct {
		IncludeLabels bool
		ByColumn      bool
		records       [][]string
	}
	type args struct {
		df *DataFrame
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    [][]string
		wantErr bool
	}{
		{"pass - include labels - by column",
			fields{
				IncludeLabels: true,
				ByColumn:      true,
			},
			args{&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", ""}, isNull: []bool{false, true}, id: mockID, name: "foo"},
					{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			},
			[][]string{{"*0", "0", "1"}, {"foo", "a", "(null)"}, {"bar", "1", "qux"}},
			false},
		{"pass - exclude labels - by row",
			fields{
				IncludeLabels: false,
				ByColumn:      false,
			},
			args{&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", ""}, isNull: []bool{false, true}, id: mockID, name: "foo"},
					{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			},
			[][]string{{"foo", "bar"}, {"a", "1"}, {"(null)", "qux"}},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &RecordWriter{
				IncludeLabels: tt.fields.IncludeLabels,
				ByColumn:      tt.fields.ByColumn,
				records:       tt.fields.records,
			}
			if err := w.Write(tt.args.df); (err != nil) != tt.wantErr {
				t.Errorf("RecordWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(w.Records(), tt.want) {
				t.Errorf("RecordWriter.Write().Records() = %v, want %v", w.Records(), tt.want)
			}
		})
	}
}

func TestInterfaceRecordReader_Read(t *testing.T) {
	type fields struct {
		HeaderRows  int
		LabelLevels int
		ByColumn    bool
		records     [][]interface{}
	}
	tests := []struct {
		name    string
		fields  fields
		want    *DataFrame
		wantErr bool
	}{
		{"pass - by rows - 1 header",
			fields{
				HeaderRows:  1,
				LabelLevels: 0,
				ByColumn:    false,
				records: [][]interface{}{
					{"foo", "bar"},
					{float64(1), float64(2)},
				},
			},
			&DataFrame{
				labels: []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
				values: []*valueContainer{
					{slice: []interface{}{float64(1)}, isNull: []bool{false}, id: mockID, name: "foo"},
					{slice: []interface{}{float64(2)}, isNull: []bool{false}, id: mockID, name: "bar"},
				},
				colLevelNames: []string{"*0"}},
			false,
		},
		{"fail - no records",
			fields{records: [][]interface{}{}},
			nil, true,
		},
		{"fail - first record empty",
			fields{records: [][]interface{}{{}, {0}}},
			nil, true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := InterfaceRecordReader{
				HeaderRows:  tt.fields.HeaderRows,
				LabelLevels: tt.fields.LabelLevels,
				ByColumn:    tt.fields.ByColumn,
				records:     tt.fields.records,
			}
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("InterfaceRecordReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("InterfaceRecordReader.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestInterfaceRecordWriter_Write(t *testing.T) {
	type fields struct {
		IncludeLabels bool
		ByColumn      bool
		records       [][]interface{}
	}
	type args struct {
		df *DataFrame
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    [][]interface{}
		wantErr bool
	}{
		{"pass - include labels - by column",
			fields{
				IncludeLabels: true,
				ByColumn:      true,
			},
			args{&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", ""}, isNull: []bool{false, true}, id: mockID, name: "foo"},
					{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			},
			[][]interface{}{{"*0", 0, 1}, {"foo", "a", nil}, {"bar", 1, "qux"}},
			false},
		{"pass - exclude labels - by row",
			fields{
				IncludeLabels: false,
				ByColumn:      false,
			},
			args{&DataFrame{
				values: []*valueContainer{
					{slice: []string{"a", ""}, isNull: []bool{false, true}, id: mockID, name: "foo"},
					{slice: []interface{}{1, "qux"}, isNull: []bool{false, false}, id: mockID, name: "bar"},
				},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"}},
			},
			[][]interface{}{{"foo", "bar"}, {"a", 1}, {nil, "qux"}},
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &InterfaceRecordWriter{
				IncludeLabels: tt.fields.IncludeLabels,
				ByColumn:      tt.fields.ByColumn,
				records:       tt.fields.records,
			}
			if err := w.Write(tt.args.df); (err != nil) != tt.wantErr {
				t.Errorf("InterfaceRecordWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(w.Records(), tt.want) {
				t.Errorf("InterfaceRecordWriter.Write().Records() = %v, want %v", w.Records(), tt.want)
			}
		})
	}
}

func TestCSVReader_Read(t *testing.T) {
	type fields struct {
		RecordReader RecordReader
		Reader       *csv.Reader
	}
	tests := []struct {
		name        string
		fields      fields
		want        *DataFrame
		wantRecords [][]string
		wantErr     bool
	}{
		{"pass", fields{
			RecordReader: RecordReader{
				HeaderRows: 1,
			},
			Reader: csv.NewReader(strings.NewReader("Name, Age\n foo, 1\n bar, 2")),
		},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "Name"},
					{slice: []string{"1", "2"}, isNull: []bool{false, false}, id: mockID, name: "Age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"},
			},
			[][]string{
				{"Name", "Age"},
				{"foo", "1"},
				{"bar", "2"},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := CSVReader{
				RecordReader: tt.fields.RecordReader,
				Reader:       tt.fields.Reader,
			}
			r.TrimLeadingSpace = true
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("CSVReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("CSVReader.Read() = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(r.Records(), tt.wantRecords) {
				t.Errorf("CSVReader.Read() [][]string = %v, want %v", r.Records(), tt.wantRecords)
			}
		})
	}
}

func TestCSVWriter_Write(t *testing.T) {
	b := new(bytes.Buffer)
	type fields struct {
		RecordWriter *RecordWriter
		Writer       *csv.Writer
	}
	type args struct {
		df *DataFrame
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    string
		wantErr bool
	}{
		{"pass",
			fields{
				RecordWriter: &RecordWriter{
					IncludeLabels: true,
					ByColumn:      false,
				},
				Writer: csv.NewWriter(b),
			},
			args{
				&DataFrame{
					values: []*valueContainer{
						{slice: []string{"a", "b"}, isNull: []bool{false, false}, id: mockID, name: "foo"}},
					labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
					colLevelNames: []string{"*0"}},
			},
			"*0,foo\n0,a\n1,b\n",
			false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := CSVWriter{
				RecordWriter: tt.fields.RecordWriter,
				Writer:       tt.fields.Writer,
			}
			if err := w.Write(tt.args.df); (err != nil) != tt.wantErr {
				t.Errorf("CSVWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(b.String(), tt.want) {
				t.Errorf("InterfaceRecordWriter.Write().String() = %v, want %v", b.String(), tt.want)
			}
		})
	}
}

// type badReader struct{}

// func (r badReader) Read([]byte) (int, error) {
// 	return 0, fmt.Errorf("foo")
// }

// type badWriter struct{}

// func (w badWriter) Write([]byte) (int, error) {
// 	return 0, fmt.Errorf("foo")
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

func (mat testMatrix) T() Matrix {
	return mat
}

func Test_valueContainer_UnmarshalJSON(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *valueContainer
		wantErr bool
	}{
		{"nil",
			args{[]byte(`null`)},
			&valueContainer{},
			false,
		},
		{"err",
			args{[]byte(`foo`)},
			&valueContainer{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := new(valueContainer)
			if err := vc.UnmarshalJSON(tt.args.b); (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(vc, tt.want) {
				t.Errorf("DataFrame.UnmarshalJSON() -> = %v, want %v", vc, tt.want)
			}
		})
	}
}

func TestDataFrame_UnmarshalJSON(t *testing.T) {
	type args struct {
		b []byte
	}
	tests := []struct {
		name    string
		args    args
		want    *DataFrame
		wantErr bool
	}{
		{"pass",
			args{[]byte(`
			{"name": "foobar",
			"values": [{"slice": ["foo"], "isNull": [false], "id": "1", "name": "bar"}],
			"colLevelNames": ["*0"]}`)},
			&DataFrame{
				name:          "foobar",
				values:        []*valueContainer{{slice: []interface{}{"foo"}, isNull: []bool{false}, name: "bar", id: "1"}},
				colLevelNames: []string{"*0"},
			},
			false,
		},
		{"nil",
			args{[]byte(`null`)},
			&DataFrame{},
			false,
		},
		{"err",
			args{[]byte(`foo`)},
			&DataFrame{},
			true,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			df := new(DataFrame)
			if err := df.UnmarshalJSON(tt.args.b); (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.UnmarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !EqualDataFrames(df, tt.want) {
				t.Errorf("DataFrame.UnmarshalJSON() -> = %v, want %v", df, tt.want)
			}
		})
	}
}

func TestStructReader_Read(t *testing.T) {
	type fields struct {
		sliceOfStructs interface{}
		IsNull         [][]bool
		LabelLevels    int
		Name           string
	}
	tests := []struct {
		name    string
		fields  fields
		want    *DataFrame
		wantErr bool
	}{
		{"pass",
			fields{
				sliceOfStructs: []testStruct{{"foo", 1}, {"", 2}},
				IsNull:         [][]bool{{false, false}, {true, false}},
			},
			&DataFrame{
				values: []*valueContainer{
					{slice: []string{"foo", ""}, isNull: []bool{false, true}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := StructReader{
				sliceOfStructs: tt.fields.sliceOfStructs,
				IsNull:         tt.fields.IsNull,
				LabelLevels:    tt.fields.LabelLevels,
				Name:           tt.fields.Name,
			}
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("StructReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("StructReader.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestStructWriter_Write(t *testing.T) {
	type fields struct {
		sliceOfStructs interface{}
		isNull         [][]bool
		IncludeLabels  bool
	}
	type args struct {
		df *DataFrame
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    interface{}
		wantErr bool
	}{
		{"pass",
			fields{
				sliceOfStructs: &[]testStruct{},
			},
			args{
				&DataFrame{
					values: []*valueContainer{
						{slice: []string{"foo", ""}, isNull: []bool{false, false}, id: mockID, name: "name"},
						{slice: []int{1, 2}, isNull: []bool{false, false}, id: mockID, name: "age"}},
					labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
					colLevelNames: []string{"*0"},
				},
			},
			&[]testStruct{
				{"foo", 1},
				{"", 2},
			},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			w := &StructWriter{
				sliceOfStructs: tt.fields.sliceOfStructs,
				isNull:         tt.fields.isNull,
				IncludeLabels:  tt.fields.IncludeLabels,
			}
			if err := w.Write(tt.args.df); (err != nil) != tt.wantErr {
				t.Errorf("StructWriter.Write() error = %v, wantErr %v", err, tt.wantErr)
			}
			if !reflect.DeepEqual(w.sliceOfStructs, tt.want) {
				t.Errorf("StructWriter.Write() -> = %v, want %v", w.sliceOfStructs, tt.want)
			}
		})
	}
}

func TestWriteMockCSV(t *testing.T) {
	got := `foo,bar
10,fred
100,corge`
	want := `foo,bar
0.5,baz
0.5,baz
`
	b := new(bytes.Buffer)
	type args struct {
		r *CSVReader
		w *CSVWriter
		n int
	}
	tests := []struct {
		name    string
		args    args
		want    string
		wantErr bool
	}{
		{"pass",
			args{NewCSVReader(strings.NewReader(got)), NewCSVWriter(b), 2},
			want, false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			err := WriteMockCSV(tt.args.r, tt.args.w, tt.args.n)
			if (err != nil) != tt.wantErr {
				t.Errorf("WriteMockCSV() error = %v, wantErr %v", err, tt.wantErr)
			}
			if b.String() != tt.want {
				t.Errorf("WriteMockCSV() -> = %v, want %v", b.String(), tt.want)

			}
		})
	}
}

func TestMatrixReader_Read(t *testing.T) {
	type fields struct {
		mat Matrix
	}
	tests := []struct {
		name    string
		fields  fields
		want    *DataFrame
		wantErr bool
	}{
		{"matrix with same signature as gonum mat/matrix",
			fields{mat: testMatrix{values: [][]float64{{1, 2}}}},
			&DataFrame{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "0"},
					{slice: []float64{2}, isNull: []bool{false}, id: mockID, name: "1"}},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
				name:          "",
				colLevelNames: []string{"*0"}},
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			r := MatrixReader{
				mat: tt.fields.mat,
			}
			got, err := r.Read()
			if (err != nil) != tt.wantErr {
				t.Errorf("MatrixReader.Read() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !EqualDataFrames(got, tt.want) {
				t.Errorf("MatrixReader.Read() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestDataFrame_EqualRecords(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		got  *RecordWriter
		want *CSVReader
	}
	tests := []struct {
		name    string
		fields  fields
		args    args
		want    bool
		want1   *tablediff.Differences
		wantErr bool
	}{
		{"pass",
			fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args{
				got:  NewRecordWriter(),
				want: NewCSVReader(strings.NewReader("foo\n1")),
			},
			true,
			nil,
			false,
		},
		{"fail - bad read",
			fields{
				values: []*valueContainer{
					{slice: []float64{1}, isNull: []bool{false}, id: mockID, name: "foo"},
				},
				labels:        []*valueContainer{{slice: []int{0}, isNull: []bool{false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
				name:          "baz"},
			args{
				got:  NewRecordWriter(),
				want: NewCSVReader(strings.NewReader("foo\n1,2,3")),
			},
			false,
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
				err:           tt.fields.err,
				colLevelNames: tt.fields.colLevelNames,
			}
			got, got1, err := df.EqualRecords(tt.args.got, tt.args.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.EqualRecords() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if got != tt.want {
				t.Errorf("DataFrame.EqualRecords() got = %v, want %v", got, tt.want)
			}
			if !reflect.DeepEqual(got1, tt.want1) {
				t.Errorf("DataFrame.EqualRecords() got1 = %v, want %v", got1, tt.want1)
			}
		})
	}
}

func Test_valueContainer_alias(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
		id     string
	}
	tests := []struct {
		name   string
		fields fields
		want   valueContainerAlias
	}{
		{"pass",
			fields{
				[]string{"foo"},
				[]bool{false},
				nil,
				"foo",
				"123",
			},
			valueContainerAlias{
				[]string{"foo"},
				[]bool{false},
				"foo",
				"123",
			},
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			if got := vc.alias(); !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.alias() = %v, want %v", got, tt.want)
			}
		})
	}
}

func Test_valueContainer_MarshalJSON(t *testing.T) {
	type fields struct {
		slice  interface{}
		isNull []bool
		cache  []string
		name   string
		id     string
	}
	tests := []struct {
		name    string
		fields  fields
		want    []byte
		wantErr bool
	}{
		{"pass",
			fields{
				[]string{"foo"},
				[]bool{false},
				nil,
				"foo",
				"123",
			},
			[]byte(`{"slice":["foo"],"isNull":[false],"name":"foo","id":"123"}`),
			false,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vc := valueContainer{
				slice:  tt.fields.slice,
				isNull: tt.fields.isNull,
				cache:  tt.fields.cache,
				name:   tt.fields.name,
				id:     tt.fields.id,
			}
			got, err := vc.MarshalJSON()
			if (err != nil) != tt.wantErr {
				t.Errorf("valueContainer.MarshalJSON() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Errorf("valueContainer.MarshalJSON() = %v, want %v", string(got), tt.want)
			}
		})
	}
}

func TestDataFrame_EqualStructs(t *testing.T) {
	type fields struct {
		labels        []*valueContainer
		values        []*valueContainer
		name          string
		err           error
		colLevelNames []string
	}
	type args struct {
		got  *StructWriter
		want StructReader
	}
	tests := []struct {
		name     string
		fields   fields
		args     args
		wantEq   bool
		wantDiff string
		wantErr  bool
	}{
		{"pass - eq - no null check",
			fields{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{true, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{
				NewStructWriter(&[]testStruct{}),
				NewStructReader([]testStruct{
					{"foo", 1},
					{"bar", 2},
				}),
			},
			true,
			"",
			false,
		},
		{"pass - eq - null check",
			fields{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{true, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{
				NewStructWriter(&[]testStruct{}),
				StructReader{
					sliceOfStructs: []testStruct{
						{"foo", 1},
						{"bar", 2},
					},
					IsNull: [][]bool{
						{false, true},
						{false, false},
					},
				},
			},
			true,
			"",
			false,
		},
		{"pass - neq - no null check",
			fields{
				values: []*valueContainer{
					{slice: []string{"foo", "baz"}, isNull: []bool{false, false}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{true, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{
				NewStructWriter(&[]testStruct{}),
				StructReader{
					sliceOfStructs: []testStruct{
						{"foo", 1},
						{"bar", 2},
					},
				},
			},
			false,
			"modified: [1].Name = \"baz\"\n",
			false,
		},
		{"pass - neq - null check",
			fields{
				values: []*valueContainer{
					{slice: []string{"foo", "baz"}, isNull: []bool{false, false}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{true, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{
				NewStructWriter(&[]testStruct{}),
				StructReader{
					sliceOfStructs: []testStruct{
						{"foo", 1},
						{"bar", 2},
					},
					IsNull: [][]bool{
						{false, false},
						{false, false},
					},
				},
			},
			false,
			"IsNull: modified: [0][1] = true\n",
			false,
		},
		{"fail - bad writer",
			fields{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{true, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{
				NewStructWriter("foo"),
				StructReader{
					sliceOfStructs: []testStruct{
						{"foo", 1},
						{"bar", 2},
					},
					IsNull: [][]bool{
						{false, true},
						{false, false},
					},
				},
			},
			false,
			"",
			true,
		},
		{"fail - bad reader nulls",
			fields{
				values: []*valueContainer{
					{slice: []string{"foo", "bar"}, isNull: []bool{false, false}, id: mockID, name: "name"},
					{slice: []int{1, 2}, isNull: []bool{true, false}, id: mockID, name: "age"}},
				labels:        []*valueContainer{{slice: []int{0, 1}, isNull: []bool{false, false}, id: mockID, name: "*0"}},
				colLevelNames: []string{"*0"},
			},
			args{
				NewStructWriter(&[]testStruct{}),
				StructReader{
					sliceOfStructs: []testStruct{
						{"foo", 1},
						{"bar", 2},
					},
					IsNull: [][]bool{
						{false},
					},
				},
			},
			false,
			"",
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
			gotEq, gotDiff, err := df.EqualStructs(tt.args.got, tt.args.want)
			if (err != nil) != tt.wantErr {
				t.Errorf("DataFrame.EqualStructs() error = %v, wantErr %v", err, tt.wantErr)
				return
			}
			if gotEq != tt.wantEq {
				t.Errorf("DataFrame.EqualStructs() gotEq = %v, want %v", gotEq, tt.wantEq)
			}
			if gotDiff != tt.wantDiff {
				t.Errorf("DataFrame.EqualStructs() gotDiff = %v, want %v", gotDiff, tt.wantDiff)
			}
		})
	}
}
