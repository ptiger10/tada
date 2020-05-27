package tada

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"io"
	"reflect"

	"github.com/d4l3k/messagediff"
	"github.com/ptiger10/tablediff"
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
	err := json.Unmarshal(b, &alias)
	if err != nil {
		return fmt.Errorf("unmarshaling value container alias: %v", err)
	}
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
	err := json.Unmarshal(b, &alias)
	if err != nil {
		return fmt.Errorf("unmarshaling DataFrame: %v", err)
	}
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

// NewDataFrame constructs a new DataFrame from Reader.
func NewDataFrame(r Reader) (*DataFrame, error) {
	df, err := r.Read()
	if err != nil {
		return nil, fmt.Errorf("constructing new DataFrame: %v", err)
	}
	return df, nil
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

// -- slice of slices

// SliceReader reads slices into a DataFrame.
type SliceReader struct {
	ColSlices    []interface{}
	LabelSlices  []interface{}
	ColNames     []string
	LabelNames   []string
	Name         string
	NumColLevels int // advanced: allows for multiple levels of column header names
}

// NewSliceReader returns a default SliceReader.
func NewSliceReader(columnSlices []interface{}) SliceReader {
	return SliceReader{
		ColSlices: columnSlices,
	}
}

// MustRead reads from SliceReader and returns a DataFrame. Any errors are written to the DataFrame directly.
func (r SliceReader) MustRead() *DataFrame {
	df, err := r.Read()
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Read creates a new DataFrame from a SliceReader.
// Each element in columnSlices and labelSlices must itself be a slice.
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// Columns are named sequentially (e.g., 0, 1, etc) by default. Default column names are displayed on printing.
// Label levels are named sequentially with a prefix (e.g., *0, *1, etc) by default. Default label names are hidden on printing.
func (r SliceReader) Read() (*DataFrame, error) {
	if r.ColSlices == nil && r.LabelSlices == nil {
		return nil, fmt.Errorf("slice reader: values and labels cannot both be nil")
	}
	labels, err := makeValueContainersFromInterfaces(r.LabelSlices, true)
	if err != nil {
		return nil, fmt.Errorf("slice reader: labels: %v", err)
	}
	columns, err := makeValueContainersFromInterfaces(r.ColSlices, false)
	if err != nil {
		return nil, fmt.Errorf("slice reader: columns: %v", err)
	}
	containers := append(labels, columns...)
	df := containersToDF(containers, r.NumColLevels, len(r.LabelSlices), r.Name)

	// set container names
	if len(r.LabelNames) > 0 {
		err = df.SetLabelNames(r.LabelNames)
		if err != nil {
			return nil, fmt.Errorf("slice reader: %v", err)
		}
	}
	if len(r.ColNames) > 0 {
		err = df.SetColNames(r.ColNames)
		if err != nil {
			return nil, fmt.Errorf("slice reader: %v", err)
		}
	}
	requiredLength := containers[0].len()
	err = ensureEqualLengths(containers, requiredLength)
	if err != nil {
		return nil, fmt.Errorf("slice reader: %v", err)
	}

	return df, nil
}

// -- [][]string records

// RecordReader reads [][]string records into a DataFrame.
type RecordReader struct {
	HeaderRows        int
	LabelLevels       int
	ByColumn          bool
	Name              string
	InferTypes        bool
	BlankStringAsNull bool
	records           [][]string
}

// NewRecordReader returns a default RecordReader.
func NewRecordReader(records [][]string) RecordReader {
	return RecordReader{
		HeaderRows:        1,
		LabelLevels:       0,
		ByColumn:          false,
		BlankStringAsNull: false,
		InferTypes:        false,
		records:           records,
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
	if r.BlankStringAsNull {
		_, ok := optionNullStrings.Read()[""]
		if !ok {
			defer func() {
				SetOptionEmptyStringAsNull(false)
			}()
		}
		SetOptionEmptyStringAsNull(true)
	}

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
	if r.InferTypes {
		castToInferredTypes(vc)
	}
	df := containersToDF(vc, r.HeaderRows, r.LabelLevels, r.Name)
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

// Records returns the [][]string records after they have been read.
func (r *CSVReader) Records() [][]string {
	return r.records
}

// NewCSVReader creates a new CSVReader with embedded encoding/csv.Reader and default settings.
func NewCSVReader(r io.Reader) *CSVReader {
	return &CSVReader{
		RecordReader: RecordReader{
			HeaderRows:  1,
			LabelLevels: 0,
		},
		Reader: csv.NewReader(r),
	}
}

// Read reads a DataFrame from a encoding/csv.Reader
func (r *CSVReader) Read() (*DataFrame, error) {
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
	*RecordWriter
	*csv.Writer
}

// NewCSVWriter creates a new *CSVWriter with embedded encoding/csv.Writer and default settings.
func NewCSVWriter(w io.Writer) *CSVWriter {
	return &CSVWriter{
		RecordWriter: &RecordWriter{
			IncludeLabels: false,
			ByColumn:      false,
		},
		Writer: csv.NewWriter(w),
	}
}

func (w *CSVWriter) Write(df *DataFrame) error {
	w.RecordWriter.Write(df)
	return w.Writer.WriteAll(w.Records())
}

// -- [][]interface{} records

// InterfaceRecordReader reads [][]interface{} records into a DataFrame.
type InterfaceRecordReader struct {
	HeaderRows  int
	LabelLevels int
	ByColumn    bool
	Name        string
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
	df := containersToDF(vc, r.HeaderRows, r.LabelLevels, r.Name)
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
	sliceOfStructs interface{}
	IsNull         [][]bool
	LabelLevels    int
	Name           string
}

// NewStructReader returns a new reader for a struct or slice of structs.
func NewStructReader(sliceOfStructs interface{}) StructReader {
	return StructReader{
		sliceOfStructs: sliceOfStructs,
		LabelLevels:    0,
	}
}

// ReadStruct reads the exported fields in the StructReader into a DataFrame.
//
// If a "json" tag is present, the column will have the same name as the tag value.
// Otherwise, the column will have the same name as the exported field.
func (r StructReader) Read() (*DataFrame, error) {
	values, err := readStructSlice(r.sliceOfStructs, r.IsNull)
	if err != nil {
		return nil, fmt.Errorf("reading from StructReader: %v", err)
	}
	df := containersToDF(values, 1, r.LabelLevels, r.Name)
	return df, nil
}

// StructWriter writes a single struct or slice of structs from a DataFrame.
type StructWriter struct {
	sliceOfStructs interface{}
	isNull         [][]bool
	IncludeLabels  bool
	Strict         bool // returns error if DataFrame has columns that do not match exported fields
}

// NewStructWriter creates a new *StructWriter.
// ptrSliceOfStructs should be a pointer to an empty slice of structs.
// After writing, ptrSliceOfStructs can be accessed directly outside of this writer.
func NewStructWriter(ptrSliceOfStructs interface{}) *StructWriter {
	return &StructWriter{
		sliceOfStructs: ptrSliceOfStructs,
		IncludeLabels:  false,
	}
}

// IsNull returns a boolean matrix indicating whether each value is null.
func (w StructWriter) IsNull() [][]bool {
	return w.isNull
}

// Write writes the values of the df into a slice of structs.
func (w *StructWriter) Write(df *DataFrame) error {
	containers := df.values
	if w.IncludeLabels {
		containers = append(df.labels, df.values...)
	}
	isNull, err := writeStructSlice(containers, w.sliceOfStructs, w.Strict)
	if err != nil {
		return fmt.Errorf("writing to StructWriter: %v", err)
	}
	w.isNull = isNull
	return nil
}

// gonum.Matrix

// MatrixReader reads from a data structure that implements the gonum.Matrix interface.
type MatrixReader struct {
	mat Matrix
}

// NewMatrixReader creates a MatrixReader with default settings.
func NewMatrixReader(mat Matrix) MatrixReader {
	return MatrixReader{
		mat: mat,
	}
}

// ReadMatrix reads data satisfying the gonum Matrix interface into a DataFrame.
// Panics if any slices in the matrix are shorter than the first slice.
func (r MatrixReader) Read() (*DataFrame, error) {
	numRows, numCols := r.mat.Dims()
	// major dimension: columns
	data := make([]interface{}, numCols)
	for k := range data {
		floats := make([]float64, numRows)
		for i := 0; i < numRows; i++ {
			floats[i] = r.mat.At(i, k)
		}
		data[k] = floats
	}
	ret := NewSliceReader(data)
	df := ret.MustRead()
	return df, nil
}

// -- WRITERS

// WriteMockCSV reads r, infers the types, and writes n mock rows to w.
func WriteMockCSV(r *CSVReader, w *CSVWriter, n int) error {
	r.InferTypes = false
	df, err := r.Read()
	if err != nil {
		return fmt.Errorf("writing mock csv: %v", err)
	}
	containers := df.values
	if w.IncludeLabels {
		containers = append(df.labels, df.values...)
	}
	dtypes := make([]DType, len(containers))
	for k := range containers {
		dtypes[k] = containers[k].inferType()
	}
	containers = mockContainersFromDTypes(listNames(containers), dtypes, n)
	df = containersToDF(containers, r.HeaderRows, r.LabelLevels, r.Name)
	err = w.Write(df)
	if err != nil {
		return fmt.Errorf("writing mock csv: %v", err)
	}
	return nil
}

// EqualRecords reduces df to [][]string records, reads [][]string records from want,
// and evaluates whether the stringified values match.
// If they do not match, returns a tablediff.Differences object that can be printed to isolate their differences.
func (df *DataFrame) EqualRecords(got *RecordWriter, want *CSVReader) (bool, *tablediff.Differences, error) {
	got.Write(df) // RecordWriter.Write() cannot return error
	_, err := want.Read()
	if err != nil {
		return false, nil, fmt.Errorf("comparing records: reading want: %v", err)
	}
	diffs, eq := tablediff.Diff(got.Records(), want.records)
	return eq, diffs, nil
}

// EqualStructs writes df to a slice of structs, reads in a comparison slice of structs,
// and returns whether they are equal. If not, returns the differences between the two.
// If the want argument includes an IsNull field, null values are compared.
func (df *DataFrame) EqualStructs(got *StructWriter, want StructReader) (eq bool, diff string, err error) {
	err = df.WriteTo(got)
	if err != nil {
		return false, "", fmt.Errorf("comparing two slices of structs: writing to got: %v", err)
	}

	if want.IsNull != nil {
		_, err := want.Read()
		if err != nil {
			return false, "", fmt.Errorf("comparing two slices of structs: reading want: %v", err)
		}
		diff, eq := messagediff.PrettyDiff(want.IsNull, got.IsNull())
		if !eq {
			return false, "IsNull: " + diff, nil
		}
	}
	gotSlice := reflect.ValueOf(got.sliceOfStructs).Elem().Interface()
	diff, eq = messagediff.PrettyDiff(want.sliceOfStructs, gotSlice)
	if !eq {
		return false, diff, nil
	}
	return true, "", nil
}
