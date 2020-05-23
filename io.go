package tada

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"
	"unicode"
)

// ByColumn configures a read function to expect columns to be the major dimension of csv data
// (default: expects rows to be the major dimension).
// For example, when reading this data:
//
// [["foo", "bar"], ["baz", "qux"]]
//
// default				   		ByColumn()
// (major dimension: rows)			(major dimension: columns)
//	foo bar							foo baz
//  baz qux							bar qux

// -- custom Encoder/Decoder

func (vc valueContainerAlias) vc() valueContainer {
	return valueContainer{
		slice:  vc.Slice,
		isNull: vc.IsNull,
		name:   vc.Name,
		id:     vc.ID,
	}
}

func (vc valueContainer) alias() valueContainerAlias {
	return valueContainerAlias{
		Slice:  vc.slice,
		IsNull: vc.isNull,
		Name:   vc.name,
		ID:     vc.id,
	}
}

func (vc valueContainer) MarshalJSON() ([]byte, error) {
	return json.Marshal(vc.alias())
}

func (vc *valueContainer) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var alias valueContainerAlias
	json.Unmarshal(b, &alias)
	*vc = alias.vc()
	return nil
}

func (df dataFrameAlias) df() DataFrame {
	return DataFrame{
		labels:        df.Labels,
		values:        df.Values,
		name:          df.Name,
		colLevelNames: df.ColLevelNames,
	}
}

func (df *DataFrame) alias() dataFrameAlias {
	return dataFrameAlias{
		Labels:        df.labels,
		Values:        df.values,
		Name:          df.name,
		ColLevelNames: df.colLevelNames,
	}
}

// MarshalJSON satisifies the json.Marshaler interface for writing a DataFrame to JSON.
func (df *DataFrame) MarshalJSON() ([]byte, error) {
	return json.Marshal(df.alias())
}

// UnmarshalJSON satisifies the json.Unmarshaler interface for reading a DataFrame from JSON.
func (df *DataFrame) UnmarshalJSON(b []byte) error {
	if string(b) == "null" {
		return nil
	}
	var alias dataFrameAlias
	json.Unmarshal(b, &alias)
	*df = alias.df()
	return nil
}

// A Reader can read in a DataFrame from various data sources.
type Reader interface {
	Read() (*DataFrame, error)
}

// A Writer can write a DataFrame into various receivers.
type Writer interface {
	Write(*DataFrame) error
}

// WriteTo writes from a Series to a Writer.
func (s *Series) WriteTo(w Writer) error {
	df := s.DataFrame()
	err := w.Write(df)
	if err != nil {
		return fmt.Errorf("writing from Series: %v", err)
	}
	return nil
}

// WriteTo writes a DataFrame to a Writer.
func (df *DataFrame) WriteTo(w Writer) error {
	err := w.Write(df)
	if err != nil {
		return fmt.Errorf("writing to Writer: %v", err)
	}
	return nil
}

// -- READ/WRITERS

// -- [][]string records

// RecordReader reads [][]string records into a DataFrame.
type RecordReader struct {
	HeaderRows  int
	LabelLevels int
	ByColumn    bool
	records     [][]string
}

// NewRecordReader returns a default RecordReader.
func NewRecordReader(records [][]string) RecordReader {
	return RecordReader{
		HeaderRows:  1,
		LabelLevels: 0,
		ByColumn:    false,
		records:     records,
	}
}

// Read reads [][]string records to a DataFrame.
// All columns will be read as []string.
// Records are read with row as the major dimension, unless r.ByColumn = true.
//
// If no label levels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func (r RecordReader) Read() (*DataFrame, error) {
	if len(r.records) == 0 {
		return nil, fmt.Errorf("reading csv from records: must have at least one record")
	}
	if len(r.records[0]) == 0 {
		return nil, fmt.Errorf("reading csv from records: first record cannot be empty")
	}
	vc, err := readRecords(r.records, r.ByColumn, r.HeaderRows)
	if err != nil {
		return nil, fmt.Errorf("reading csv from records: %v", err)
	}
	df := containersToDF(vc, r.HeaderRows, r.LabelLevels)
	return df, nil
}

// RecordWriter writes [][]string records from a DataFrame.
type RecordWriter struct {
	IncludeLabels bool
	ByColumn      bool
	records       [][]string
}

// NewRecordWriter returns a *RecordWriter with default settings
func NewRecordWriter() *RecordWriter {
	return &RecordWriter{
		ByColumn:      false,
		IncludeLabels: false,
	}
}

// Records returns the [][]string records written to w.
func (w RecordWriter) Records() [][]string {
	return w.records
}

// Write reduces a DataFrame to [][]string and writes the result to w.
// Null values are replaced with (null).
func (w *RecordWriter) Write(df *DataFrame) error {
	containers := df.values
	if w.IncludeLabels {
		containers = append(df.labels, df.values...)
	}
	w.records = writeRecords(containers, w.ByColumn, df.numColLevels())
	return nil
}

// -- encoding/csv

// CSVReader reads encoding/csv.Reader into a DataFrame.
type CSVReader struct {
	RecordReader
	*csv.Reader
}

// NewCSVReader creates a new CSVReader with embedded encoding/csv reader and default settings.
func NewCSVReader(r io.Reader) CSVReader {
	return CSVReader{
		RecordReader: RecordReader{
			HeaderRows:  1,
			LabelLevels: 0,
		},
		Reader: csv.NewReader(r),
	}
}

func (r CSVReader) Read() (*DataFrame, error) {
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("CSVReader: %v", err)
	}
	r.records = records
	df, err := r.RecordReader.Read()
	if err != nil {
		return nil, fmt.Errorf("CSVReader: %v", err)
	}
	return df, nil
}

// CSVWriter writes DataFrame values into an encoding/csv.Writer.
type CSVWriter struct {
	RecordWriter
	*csv.Writer
}

// NewCSVWriter creates a new *CSVWriter with embedded encoding/csv.Writer and default settings.
func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{
		RecordWriter: RecordWriter{
			IncludeLabels: false,
			ByColumn:      false,
		},
		Writer: csv.NewWriter(w),
	}
}

func (w CSVWriter) Write(df *DataFrame) error {
	w.RecordWriter.Write(df)
	return w.Writer.WriteAll(w.Records())
}

// -- [][]interface{} records

// InterfaceRecordReader reads [][]interface{} records into a DataFrame.
type InterfaceRecordReader struct {
	HeaderRows  int
	LabelLevels int
	ByColumn    bool
	records     [][]interface{}
}

// NewInterfaceRecordReader returns a default InterfaceRecordReader.
func NewInterfaceRecordReader(records [][]interface{}) InterfaceRecordReader {
	return InterfaceRecordReader{
		HeaderRows:  1,
		LabelLevels: 0,
		ByColumn:    false,
		records:     records,
	}
}

// Read reads [][]interface{} records into a DataFrame.
// All columns will be read as []interface{}.
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func (r InterfaceRecordReader) Read() (*DataFrame, error) {
	if len(r.records) == 0 {
		return nil, fmt.Errorf("reading csv from records: must have at least one record")
	}
	if len(r.records[0]) == 0 {
		return nil, fmt.Errorf("reading csv from records: first record cannot be empty")
	}
	vc, err := readInterfaceRecords(r.records, r.ByColumn, r.HeaderRows)
	if err != nil {
		return nil, fmt.Errorf("reading csv from records: %v", err)
	}
	df := containersToDF(vc, r.HeaderRows, r.LabelLevels)
	return df, nil
}

// InterfaceRecordWriter writes DataFrame values into [][]interface{} records.
type InterfaceRecordWriter struct {
	IncludeLabels bool
	ByColumn      bool
	records       [][]interface{}
}

// NewInterfaceRecordWriter returns an *InterfaceRecordWriter with default settings.
func NewInterfaceRecordWriter() *InterfaceRecordWriter {
	return &InterfaceRecordWriter{
		ByColumn:      false,
		IncludeLabels: false,
	}
}

// Records returns the [][]interface{} written to w.
func (w InterfaceRecordWriter) Records() [][]interface{} {
	return w.records
}

// Write reduces a DataFrame to [][]interface{} and writes the result to w.
// Null values are replaced with nil.
func (w *InterfaceRecordWriter) Write(df *DataFrame) error {
	containers := df.values
	if w.IncludeLabels {
		containers = append(df.labels, df.values...)
	}
	w.records = writeInterfaceRecords(containers, w.ByColumn, df.numColLevels())
	return nil
}

// -- structs

// StructReader reads a single struct or slice of structs into a DataFrame.
type StructReader struct {
	s           interface{}
	IsNull      [][]bool
	IsSlice     bool
	LabelLevels int
}

// NewStructReader returns a new reader for a struct or slice of structs.
func NewStructReader(s interface{}) StructReader {
	return StructReader{
		s:           s,
		IsSlice:     false,
		LabelLevels: 0,
	}
}

// ReadStruct reads the exported fields in the StructReader into a DataFrame.
// strct must be a struct or pointer to a struct.
// If any exported field in strct is nil, returns an error.
//
// If a "tada" tag is present with the value "isNull", this field must be [][]bool with one equal-lengthed slice for each exported field.
// These values will set the null status for each of the resulting value containers in the DataFrame, from left-to-right.
// If a "tada" tag has any other value, the resulting value container will have the same name as the tag value.
// Otherwise, the value container will have the same name as the exported field.
func (r StructReader) Read() (*DataFrame, error) {
	if r.IsSlice {
		values, err := readStructSlice(r.s)
		if err != nil {
			return nil, fmt.Errorf("reading struct slice: %v", err)
		}
		defaultLabels := makeDefaultLabels(0, reflect.ValueOf(r.s).Len(), true)
		return &DataFrame{
			values:        values,
			labels:        []*valueContainer{defaultLabels},
			colLevelNames: []string{"*0"},
		}, nil
	}
	if reflect.TypeOf(r.s).Kind() == reflect.Ptr {
		r.s = reflect.ValueOf(r.s).Elem().Interface()
	}
	if reflect.TypeOf(r.s).Kind() != reflect.Struct {
		return nil, fmt.Errorf("reading struct: strct must be reflect.Kind struct, not %s",
			reflect.TypeOf(r.s).Kind())
	}
	labels := make([]interface{}, 0)
	values := make([]interface{}, 0)
	labelNames := make([]string, 0)
	colNames := make([]string, 0)
	v := reflect.ValueOf(r.s)
	var hasNullTag bool
	var nullField string
	nullTag := "isNull"
	var offset int
	for k := 0; k < v.NumField(); k++ {
		field := reflect.TypeOf(r.s).Field(k)
		// is unexported field?
		if unicode.IsLower([]rune(field.Name)[0]) {
			offset--
			continue
		}
		// has null tag?
		if field.Tag.Get("tada") == nullTag {
			offset--
			if field.Type.String() != "[][]bool" {
				return nil, fmt.Errorf("reading struct: field with tag %v must be type [][]bool, not %s",
					nullTag, field.Type.String())
			}
			hasNullTag = true
			nullField = field.Name
			continue
		}
		// is nil?
		if v.Field(k).IsZero() {
			return nil, fmt.Errorf("reading struct: field %s: strct cannot contain a nil exported field",
				field.Name)
		}
		container := k + offset
		var name string
		// check tada tag first, then default to exported name
		if name = field.Tag.Get("tada"); name == "" {
			name = field.Name
		}
		// write to label
		if container < r.LabelLevels {
			labelNames = append(labelNames, name)
			labels = append(labels, v.Field(k).Interface())
			// write to column
		} else {
			colNames = append(colNames, name)
			values = append(values, v.Field(k).Interface())
		}
	}
	df := NewDataFrame(values, labels...)
	if df.err != nil {
		return nil, fmt.Errorf("reading struct as schema: %v", df.err)
	}
	// not default labels? apply label names
	if r.LabelLevels > 0 {
		df = df.SetLabelNames(labelNames)
	}
	df = df.SetColNames(colNames)

	if hasNullTag {
		var min int
		// default labels? do not change nulls
		if r.LabelLevels == 0 {
			min = 1
		}
		containers := makeIntRange(min, df.NumLevels()+df.NumColumns())
		nullTable := v.FieldByName(nullField).Interface().([][]bool)
		if len(nullTable) > 0 {
			for incrementor, k := range containers {
				err := df.SetNulls(k, nullTable[incrementor])
				if err != nil {
					return nil, fmt.Errorf("reading struct: writing nulls [%d]: %v", incrementor, err)
				}
			}
		}
	}
	return df, nil
}

// StructWriter writes a single struct or slice of structs from a DataFrame.
type StructWriter struct {
	s             interface{}
	isNull        [][]bool
	IsSlice       bool
	IncludeLabels bool
}

// NewStructWriter creates a new *StructWriter.
// s should be a pointer to either a slice of structs or single struct with only slice fields.
// After writing, s can be accessed directly outside of this writer.
func NewStructWriter(s interface{}) *StructWriter {
	return &StructWriter{
		s:             s,
		IsSlice:       true,
		IncludeLabels: false,
	}
}

func (w StructWriter) IsNull() [][]bool {
	return w.isNull
}

// Write writes the values of the df containers into structPointer.
// Returns an error if df does not contain, from left-to-right, the same container names and types
// as the exported fields that appear, from top-to-bottom, in structPointer.
// Exported struct fields must be types that are supported by NewDataFrame().
// If a "tada" tag is present with the value "isNull", this field must be [][]bool.
// The null status of each value container in the DataFrame, from left-to-right, will be written into this field in equal-lengthed slices.
// If df contains additional containers beyond those in structPointer, those are ignored.
func (w *StructWriter) Write(df *DataFrame) error {
	if w.IsSlice {
		return nil
	}
	if reflect.TypeOf(w.s).Kind() != reflect.Ptr {
		return fmt.Errorf("writing to struct: structPointer must be pointer to struct, not %s",
			reflect.TypeOf(w.s).Kind())
	}
	if reflect.TypeOf(w.s).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("writing to struct: structPointer must be pointer to struct, not to %s",
			reflect.TypeOf(w.s).Elem().Kind())
	}
	v := reflect.ValueOf(w.s).Elem()
	var mergedLabelsAndCols []*valueContainer
	if w.IncludeLabels {
		mergedLabelsAndCols = append(df.labels, df.values...)
	} else {
		mergedLabelsAndCols = df.values
	}
	var offset int
	var hasNullTag bool
	var nullField string
	nullTag := "isNull"
	for k := 0; k < v.NumField(); k++ {
		field := reflect.TypeOf(w.s).Elem().Field(k)
		// is unexported field?
		if unicode.IsLower([]rune(field.Name)[0]) {
			offset--
			continue
		}
		tag := field.Tag.Get("tada")
		// has null tag?
		if tag == nullTag {
			offset--
			if field.Type.String() != "[][]bool" {
				return fmt.Errorf("writing to struct: field with tag %v must be type [][]bool, not %s", nullTag, field.Type.String())
			}
			hasNullTag = true
			nullField = field.Name
			continue
		}
		container := k + offset
		// df does not have enough containers?
		if container >= len(mergedLabelsAndCols) {
			return fmt.Errorf("writing to struct: writing to exported field %s [%d]: insufficient number of containers [%d]",
				field.Name, container, len(mergedLabelsAndCols))
		}
		// use tag as name if it exists, else default to exported name
		name := tag
		if tag == "" {
			name = field.Name
		}
		if mergedLabelsAndCols[container].name != name {
			return fmt.Errorf("writing to struct: writing to exported field %s [%d]: container name does not match (%s != %s)",
				field.Name, container,
				mergedLabelsAndCols[container].name, name)
		}
		if mergedLabelsAndCols[container].dtype() != field.Type {
			return fmt.Errorf("writing to struct: writing to exported field %s [%d]: container %s has wrong type (%s != %s)",
				field.Name, container, mergedLabelsAndCols[container].name,
				mergedLabelsAndCols[container].dtype(), field.Type)
		}
		src := reflect.ValueOf(mergedLabelsAndCols[container].slice)
		dst := v.FieldByName(field.Name)
		dst.Set(src)
	}
	if hasNullTag {
		copiedFields := v.NumField() + offset
		nullTable := make([][]bool, copiedFields)
		for k := 0; k < copiedFields; k++ {
			nullTable[k] = mergedLabelsAndCols[k].isNull
		}
		src := reflect.ValueOf(nullTable).Interface()
		dst := v.FieldByName(nullField)
		dst.Set(reflect.ValueOf(src))
	}

	return nil
}

// ReadMatrix reads data satisfying the gonum Matrix interface into a DataFrame.
// Panics if any slices in the matrix are shorter than the first slice.
func ReadMatrix(mat Matrix) *DataFrame {
	numRows, numCols := mat.Dims()
	// major dimension: columns
	data := make([]interface{}, numCols)
	for k := range data {
		floats := make([]float64, numRows)
		for i := 0; i < numRows; i++ {
			floats[i] = mat.At(i, k)
		}
		data[k] = floats
	}
	ret := NewDataFrame(data)
	return ret
}

// -- WRITERS

// // WriteMockCSV reads r (configured by options) and writes n mock rows to w,
// // with column names and types inferred based on the data in src.
// // Regardless of the major dimension of src, the major dimension of the output is rows.
// // Available options: WithHeaders, WithLabels, ByColumn.
// //
// // Default if no options are supplied:
// // 1 header row, no labels, rows as major dimension
// func WriteMockCSV(w io.Writer, n int, r Reader) error {
// 	config := &readConfig{}
// 	numSampleRows := 10
// 	inferredTypes := make([]map[string]int, 0)
// 	dtypes := []string{"float", "int", "string", "datetime", "time", "bool"}
// 	var headers [][]string
// 	var rowCount int
// 	data, err := r.Read()
// 	if err != nil {
// 		return fmt.Errorf("writing mock csv: reading r: %v", err)
// 	}
// 	// data has default labels? exclude them
// 	receiver := new(CSVWriter)
// 	err = data.Write(receiver, writeOptionIncludeLabels(config.numLabelLevels > 0))
// 	if err != nil {
// 		return fmt.Errorf("writing mock csv: reading r: %v", err)
// 	}
// 	src := receiver.Records()

// 	if !config.majorDimIsCols {
// 		rowCount = len(src)
// 	} else {
// 		rowCount = len(src[0])
// 	}
// 	// numSampleRows must not exceed total number of non-header rows in src
// 	maxRows := rowCount - config.numHeaderRows
// 	if maxRows < numSampleRows {
// 		numSampleRows = maxRows
// 	}

// 	// major dimension is rows?
// 	if !config.majorDimIsCols {
// 		// copy headers
// 		for i := 0; i < config.numHeaderRows; i++ {
// 			headers = append(headers, src[i])
// 		}
// 		// prepare one inferredTypes map per column
// 		for range src[0] {
// 			emptyMap := map[string]int{}
// 			for _, dtype := range dtypes {
// 				emptyMap[dtype] = 0
// 			}
// 			inferredTypes = append(inferredTypes, emptyMap)
// 		}

// 		// for each row, infer type column-by-column
// 		// offset data sample by header rows
// 		dataSample := src[config.numHeaderRows : numSampleRows+config.numHeaderRows]
// 		for i := range dataSample {
// 			for k := range dataSample[i] {
// 				value := dataSample[i][k]
// 				dtype := inferType(value)
// 				inferredTypes[k][dtype]++
// 			}
// 		}

// 		// major dimension is columns?
// 	} else {

// 		// prepare one inferredTypes map per column
// 		for range src {
// 			emptyMap := map[string]int{}
// 			for _, dtype := range dtypes {
// 				emptyMap[dtype] = 0
// 			}
// 			inferredTypes = append(inferredTypes, emptyMap)
// 		}

// 		// copy headers
// 		headers = make([][]string, 0)
// 		for l := 0; l < config.numHeaderRows; l++ {
// 			headers = append(headers, make([]string, len(src)))
// 			for k := range src {
// 				// NB: major dimension of output is rows
// 				headers[l][k] = src[k][l]
// 			}
// 		}

// 		// for each column, infer type row-by-row
// 		for k := range src {
// 			// offset by header rows
// 			// infer type of only the sample rows
// 			dataSample := src[k][config.numHeaderRows : numSampleRows+config.numHeaderRows]
// 			for i := range dataSample {
// 				dtype := inferType(dataSample[i])
// 				inferredTypes[k][dtype]++
// 			}
// 		}
// 	}
// 	// major dimension of output is rows, for compatibility with csv.NewWriter
// 	mockCSV := mockCSVFromDTypes(inferredTypes, n)
// 	mockCSV = append(headers, mockCSV...)
// 	writer := csv.NewWriter(w)
// 	return writer.WriteAll(mockCSV)
// }
