package tada

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"reflect"
	"strings"
	"unicode"
)

// Write writes from a Series to a Writer, configured by options.
func (s *Series) Write(w Writer, options ...WriteOption) error {
	df := s.DataFrame()
	return w.writeDF(df, options...)
}

// Write writes from a DataFrame to a Writer, configured by options.
func (df *DataFrame) Write(w Writer, options ...WriteOption) error {
	return w.writeDF(df, options...)
}

// -- READ OPTIONS

// WithHeaders configures a read function to expect n rows to be column headers (default: 1).
func WithHeaders(n int) ReadOption {
	return func(r *readConfig) {
		r.numHeaderRows = n
	}
}

// WithLabels configures a read function to expect the first n columns to be label levels (default: 0).
func WithLabels(n int) ReadOption {
	return func(r *readConfig) {
		r.numLabelLevels = n
	}
}

// WithDelimiter configures a read function to use sep as a field delimiter for use in ReadCSV (default: ",").
func WithDelimiter(sep rune) ReadOption {
	return func(r *readConfig) {
		r.delimiter = sep
	}
}

// EmptyStringAsNull configures a read function to read "" as a null value
func EmptyStringAsNull() ReadOption {
	return func(r *readConfig) {
		r.emptyStringAsNull = true
	}
}

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
func ByColumn() ReadOption {
	return func(r *readConfig) {
		r.majorDimIsCols = true
	}
}

// -- WRITE OPTIONS

// ExcludeLabels excludes the label levels from the output.
func ExcludeLabels() WriteOption {
	return func(w *writeConfig) {
		w.includeLabels = false
	}
}

// for internal use
func writeOptionIncludeLabels(set bool) func(w *writeConfig) {
	return func(w *writeConfig) {
		w.includeLabels = set
	}
}

// Delimiter configures a write function to use sep as a field delimiter for use in write functions (default: ",").
func Delimiter(sep rune) WriteOption {
	return func(w *writeConfig) {
		w.delimiter = sep
	}
}

// -- READ/WRITERS

// io.Reader/Writer

// ioReader reads from an io.Reader
type ioReader struct {
	r io.Reader
}

// NewReader creates a Reader from an io.Reader.
func NewReader(r io.Reader) Reader {
	return ioReader{r}
}

// IOWriter writes to an io.Writer.
type IOWriter struct {
	W io.Writer
}

// ReadDF reads records from an io.Reader into a Dataframe (configured by options).
// Rows must be the major dimension of the records.
// For advanced cases, use the standard csv library NewReader().ReadAll() + tada.csvReader.
// Available options: WithHeaders, WithLabels, WithDelimiter, EmptyStringAsNull.
//
// Default if no options are supplied:
// 1 header row; no labels; field delimiter is ","
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func (r ioReader) ReadDF(options ...ReadOption) (*DataFrame, error) {
	config := setReadConfig(options)
	if config.emptyStringAsNull {
		archive := copySet(optionNullStrings)
		optionNullStrings[""] = true
		defer func() {
			optionNullStrings = archive
		}()
	}
	b := new(bytes.Buffer)
	io.Copy(b, r.r)
	numRows, numCols, err := extractCSVDimensions(b.Bytes(), config.delimiter)
	if err != nil {
		return nil, fmt.Errorf("reading csv: %v", err)
	}
	retVals := makeStringMatrix(numCols, numRows)
	retNulls := makeBoolMatrix(numCols, numRows)
	err = readCSVBytes(bytes.NewReader(b.Bytes()), retVals, retNulls, config.delimiter)
	if err != nil {
		return nil, fmt.Errorf("reading csv: %s", err)
	}
	return makeDataFrameFromMatrices(retVals, retNulls, config), nil
}

func (w *IOWriter) writeDF(df *DataFrame, options ...WriteOption) error {
	config := setWriteConfig(options)
	cw := new(CSVWriter)
	err := df.Write(cw, writeOptionIncludeLabels(config.includeLabels))
	if err != nil {
		return err
	}
	var b bytes.Buffer
	csvw := csv.NewWriter(&b)
	csvw.Comma = config.delimiter
	csvw.WriteAll(cw.Records)
	_, err = w.W.Write(b.Bytes())
	return err
}

// -- [][]string records

// csvReader reads [][]string records to a DataFrame.
// Can be used with encoding/csv.NewReader().ReadAll() for max configuration.
type csvReader struct {
	records [][]string
}

// NewCSVReader returns a new [][]string reader.
func NewCSVReader(records [][]string) Reader {
	return csvReader{records}
}

// CSVWriter writes [][]string records to a DataFrame.
// Can be used with encoding/csv.NewReader().ReadAll() for max configuration.
type CSVWriter struct {
	Records [][]string
}

// ReadDF reads [][]string records to a DataFrame.
// All columns will be read as []string.
// Available options: WithHeaders, WithLabels, EmptyStringAsNull, ByColumn.
//
// Default if no options are supplied:
// 1 header row; no labels; rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func (r csvReader) ReadDF(options ...ReadOption) (ret *DataFrame, err error) {
	if len(r.records) == 0 {
		return nil, fmt.Errorf("reading csv from records: must have at least one record")
	}
	if len(r.records[0]) == 0 {
		return nil, fmt.Errorf("reading csv from records: first record cannot be empty")
	}
	config := setReadConfig(options)

	if config.emptyStringAsNull {
		archive := copySet(optionNullStrings)
		optionNullStrings[""] = true
		defer func() {
			optionNullStrings = archive
		}()
	}
	if config.majorDimIsCols {
		ret, err = readCSVByCols(r.records, config)
	} else {
		ret, err = readCSVByRows(r.records, config)
	}
	if err != nil {
		return nil, fmt.Errorf("reading csv from records: %v", err)
	}
	return ret, nil
}

// writeDF writes a DataFrame to a [][]string with rows as the major dimension.
// Null values are replaced with "(null)".
func (w *CSVWriter) writeDF(df *DataFrame, options ...WriteOption) error {
	config := setWriteConfig(options)
	transposedStringValues, err := df.toCSVByRows(config.includeLabels)
	if err != nil {
		return err
	}
	mergedLabelsAndCols := df.values
	if config.includeLabels {
		mergedLabelsAndCols = append(df.labels, df.values...)
	}
	// overwrite null values, skipping headers
	for i := range transposedStringValues[df.numColLevels():] {
		for k := range transposedStringValues[i] {
			if mergedLabelsAndCols[k].isNull[i] {
				transposedStringValues[i+df.numColLevels()][k] = optionsNullPrinter
			}
		}
	}
	// b := new(http.ResponseWriter)
	// reflect.ValueOf(&w.Records).Elem().Set(reflect.ValueOf(transposedStringValues))
	w.Records = transposedStringValues
	return nil
}

// -- [][]interface{} records

// interfaceReader reads [][]interface records to a DataFrame.
type interfaceReader struct {
	records [][]interface{}
}

// NewInterfaceReader returns a new [][]interface{} reader.
func NewInterfaceReader(records [][]interface{}) Reader {
	return interfaceReader{records}
}

// InterfaceWriter writes [][]interface records from a DataFrame.
type InterfaceWriter struct {
	Records [][]interface{}
}

// ReadDF reads [][]interface{} records into a DataFrame (configured by options).
// All columns will be read as []interface{}.
// Available options: WithHeaders, WithLabels, EmptyStringAsNull, ByColumn.
//
// Default if no options are supplied:
// 1 header row; no labels; rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func (r interfaceReader) ReadDF(options ...ReadOption) (ret *DataFrame, err error) {
	if len(r.records) == 0 {
		return nil, fmt.Errorf("reading records from [][]interface{}: must have at least one record")
	}
	if len(r.records[0]) == 0 {
		return nil, fmt.Errorf("reading records from [][]interface{}: first record cannot be empty")
	}
	config := setReadConfig(options)
	if config.emptyStringAsNull {
		archive := copySet(optionNullStrings)
		optionNullStrings[""] = true
		defer func() {
			optionNullStrings = archive
		}()
	}

	var slices []interface{}
	if !config.majorDimIsCols {
		slices, err = readNestedInterfaceByRows(r.records)
	} else {
		slices, err = readNestedInterfaceByCols(r.records)
	}
	if err != nil {
		return nil, fmt.Errorf("reading records from [][]interface{}: %v", err)
	}

	numCols := len(slices) - config.numLabelLevels
	labelNames := make([]string, config.numLabelLevels)
	colNames := make([]string, numCols)

	// iterate over all containers to get header names
	for j := 0; j < config.numLabelLevels; j++ {
		// write label headers, no offset
		fields := make([]string, config.numHeaderRows)
		for i := range fields {
			fields[i] = fmt.Sprint(slices[j].([]interface{})[i])
		}
		labelNames[j] = strings.Join(fields, optionLevelSeparator)
		// remove label headers from input
		slices[j] = slices[j].([]interface{})[config.numHeaderRows:]
	}
	for k := 0; k < numCols; k++ {
		// write col headers, offset for label cols
		offsetFromLabelCols := k + config.numLabelLevels
		fields := make([]string, config.numHeaderRows)
		for i := range fields {
			fields[i] = fmt.Sprint(slices[offsetFromLabelCols].([]interface{})[i])
		}
		colNames[k] = strings.Join(fields, optionLevelSeparator)
		// remove column headers from input
		slices[offsetFromLabelCols] = slices[offsetFromLabelCols].([]interface{})[config.numHeaderRows:]
	}
	labels := slices[:config.numLabelLevels]
	slices = slices[config.numLabelLevels:]

	if len(labels) > 0 {
		ret = NewDataFrame(slices, labels...)
		ret = ret.SetLabelNames(labelNames).SetColNames(colNames)
	} else {
		ret = NewDataFrame(slices)
		if config.numHeaderRows > 0 {
			ret = ret.SetColNames(colNames)
		}
	}
	return ret, nil
}

// writeDF writes a DataFrame to a [][]interface{} with columns as the major dimension.
// Null values are replaced with "(null)".
func (w *InterfaceWriter) writeDF(df *DataFrame, options ...WriteOption) error {
	config := setWriteConfig(options)
	containers := append(df.labels, df.values...)
	if !config.includeLabels {
		containers = df.values
	}
	ret := make([][]interface{}, len(containers))
	for k := range ret {
		ret[k] = containers[k].interfaceSlice(true)
	}
	w.Records = ret
	return nil
}

// StructReader reads a single struct or slice of structs into a DataFrame.
type structReader struct {
	s       interface{}
	isSlice bool
}

// NewStructReader returns a new reader for a struct or slice of structs.
func NewStructReader(s interface{}, isSlice bool) Reader {
	return structReader{s, isSlice}
}

// StructWriter writes a single struct or slice of structs from a DataFrame.
type StructWriter struct {
	Struct  interface{}
	IsSlice bool
}

// ReadStruct reads the exported fields in the StructReader into a DataFrame.
// strct must be a struct or pointer to a struct.
// If any exported field in strct is nil, returns an error.
//
// If a "tada" tag is present with the value "isNull", this field must be [][]bool with one equal-lengthed slice for each exported field.
// These values will set the null status for each of the resulting value containers in the DataFrame, from left-to-right.
// If a "tada" tag has any other value, the resulting value container will have the same name as the tag value.
// Otherwise, the value container will have the same name as the exported field.
func (r structReader) ReadDF(options ...ReadOption) (*DataFrame, error) {
	config := setReadConfig(options)
	if r.isSlice {
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
		if container < config.numLabelLevels {
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
	if config.numLabelLevels > 0 {
		df = df.SetLabelNames(labelNames)
	}
	df = df.SetColNames(colNames)

	if hasNullTag {
		var min int
		// default labels? do not change nulls
		if config.numLabelLevels == 0 {
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

// writeDF writes the values of the df containers into structPointer.
// Returns an error if df does not contain, from left-to-right, the same container names and types
// as the exported fields that appear, from top-to-bottom, in structPointer.
// Exported struct fields must be types that are supported by NewDataFrame().
// If a "tada" tag is present with the value "isNull", this field must be [][]bool.
// The null status of each value container in the DataFrame, from left-to-right, will be written into this field in equal-lengthed slices.
// If df contains additional containers beyond those in structPointer, those are ignored.
func (w *StructWriter) writeDF(df *DataFrame, options ...WriteOption) error {
	if w.IsSlice {
		return nil
	}
	config := setWriteConfig(options)
	if reflect.TypeOf(w.Struct).Kind() != reflect.Ptr {
		return fmt.Errorf("writing to struct: structPointer must be pointer to struct, not %s", reflect.TypeOf(w.Struct).Kind())
	}
	if reflect.TypeOf(w.Struct).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("writing to struct: structPointer must be pointer to struct, not to %s", reflect.TypeOf(w.Struct).Elem().Kind())
	}
	v := reflect.ValueOf(w.Struct).Elem()
	var mergedLabelsAndCols []*valueContainer
	if config.includeLabels {
		mergedLabelsAndCols = append(df.labels, df.values...)
	} else {
		mergedLabelsAndCols = df.values
	}
	var offset int
	var hasNullTag bool
	var nullField string
	nullTag := "isNull"
	for k := 0; k < v.NumField(); k++ {
		field := reflect.TypeOf(w.Struct).Elem().Field(k)
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

// WriteMockCSV reads r (configured by options) and writes n mock rows to w,
// with column names and types inferred based on the data in src.
// Regardless of the major dimension of src, the major dimension of the output is rows.
// Available options: WithHeaders, WithLabels, ByColumn.
//
// Default if no options are supplied:
// 1 header row, no labels, rows as major dimension
func WriteMockCSV(w io.Writer, n int, r io.Reader, options ...ReadOption) error {
	config := setReadConfig(options)
	numSampleRows := 10
	inferredTypes := make([]map[string]int, 0)
	dtypes := []string{"float", "int", "string", "datetime", "time", "bool"}
	var headers [][]string
	var rowCount int
	data, err := NewReader(r).ReadDF(options...)
	if err != nil {
		return fmt.Errorf("writing mock csv: reading r: %v", err)
	}
	// data has default labels? exclude them
	receiver := new(CSVWriter)
	err = data.Write(receiver, writeOptionIncludeLabels(config.numLabelLevels > 0))
	if err != nil {
		return fmt.Errorf("writing mock csv: reading r: %v", err)
	}
	src := receiver.Records

	if !config.majorDimIsCols {
		rowCount = len(src)
	} else {
		rowCount = len(src[0])
	}
	// numSampleRows must not exceed total number of non-header rows in src
	maxRows := rowCount - config.numHeaderRows
	if maxRows < numSampleRows {
		numSampleRows = maxRows
	}

	// major dimension is rows?
	if !config.majorDimIsCols {
		// copy headers
		for i := 0; i < config.numHeaderRows; i++ {
			headers = append(headers, src[i])
		}
		// prepare one inferredTypes map per column
		for range src[0] {
			emptyMap := map[string]int{}
			for _, dtype := range dtypes {
				emptyMap[dtype] = 0
			}
			inferredTypes = append(inferredTypes, emptyMap)
		}

		// for each row, infer type column-by-column
		// offset data sample by header rows
		dataSample := src[config.numHeaderRows : numSampleRows+config.numHeaderRows]
		for i := range dataSample {
			for k := range dataSample[i] {
				value := dataSample[i][k]
				dtype := inferType(value)
				inferredTypes[k][dtype]++
			}
		}

		// major dimension is columns?
	} else {

		// prepare one inferredTypes map per column
		for range src {
			emptyMap := map[string]int{}
			for _, dtype := range dtypes {
				emptyMap[dtype] = 0
			}
			inferredTypes = append(inferredTypes, emptyMap)
		}

		// copy headers
		headers = make([][]string, 0)
		for l := 0; l < config.numHeaderRows; l++ {
			headers = append(headers, make([]string, len(src)))
			for k := range src {
				// NB: major dimension of output is rows
				headers[l][k] = src[k][l]
			}
		}

		// for each column, infer type row-by-row
		for k := range src {
			// offset by header rows
			// infer type of only the sample rows
			dataSample := src[k][config.numHeaderRows : numSampleRows+config.numHeaderRows]
			for i := range dataSample {
				dtype := inferType(dataSample[i])
				inferredTypes[k][dtype]++
			}
		}
	}
	// major dimension of output is rows, for compatibility with csv.NewWriter
	mockCSV := mockCSVFromDTypes(inferredTypes, n)
	mockCSV = append(headers, mockCSV...)
	writer := csv.NewWriter(w)
	return writer.WriteAll(mockCSV)
}
