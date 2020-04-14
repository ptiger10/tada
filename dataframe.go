package tada

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"reflect"
	"strings"
	"unicode"

	"github.com/ptiger10/tablediff"
	"github.com/ptiger10/tablewriter"
)

// -- CONSTRUCTORS

// NewDataFrame creates a new DataFrame with slices (akin to column values) and optional labels.
// Slices must be comprised of supported slices, and each label must be a supported slice.
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// Columns are named sequentially (e.g., 0, 1, etc) by default. Default column names are displayed on printing.
// Label levels are named *n (e.g., *0, *1, etc) by default. Default label names are hidden on printing.
//
// Supported slice types: all variants of []float, []int, & []uint,
// []string, []bool, []time.Time, []interface{},
// and 2-dimensional variants of each (e.g., [][]string, [][]float64).
func NewDataFrame(slices []interface{}, labels ...interface{}) *DataFrame {
	if slices == nil && labels == nil {
		return dataFrameWithError(fmt.Errorf("constructing new DataFrame: slices and labels cannot both be nil"))
	}
	values := make([]*valueContainer, 0)
	var err error
	if slices != nil {
		// handle values
		values, err = makeValueContainersFromInterfaces(slices, false)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("constructing new DataFrame: slices: %v", err))
		}
	}
	// handle labels
	retLabels, err := makeValueContainersFromInterfaces(labels, true)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("constructing new DataFrame: labels: %v", err))
	}
	if len(retLabels) == 0 {
		// handle default labels case
		numRows := reflect.ValueOf(slices[0]).Len()
		defaultLabels := makeDefaultLabels(0, numRows, true)
		retLabels = append(retLabels, defaultLabels)
	}

	// ensure equal-lengthed slices
	var requiredLength int
	if len(values) > 0 {
		requiredLength = values[0].len()
	} else {
		// handle null values case
		requiredLength = retLabels[0].len()
	}
	err = ensureEqualLengths(retLabels, requiredLength)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("constructing new DataFrame: labels: %v", err))
	}
	if len(values) > 0 {
		err = ensureEqualLengths(values, requiredLength)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("constructing new DataFrame: columns: %v", err))
		}
	}
	return &DataFrame{values: values, labels: retLabels, colLevelNames: []string{"*0"}}
}

// MakeMultiLevelLabels expects labels to be a slice of slices.
// It returns a product of these slices by repeating each label value n times,
// where n is the number of unique label values in the other slices.
//
// For example, [["foo", "bar"], [1, 2, 3]]
// returns [["foo", "foo", "foo", "bar", "bar", "bar"], [1, 2, 3, 1, 2, 3]]
func MakeMultiLevelLabels(labels []interface{}) ([]interface{}, error) {
	for k := range labels {
		if !isSlice(labels[k]) {
			return nil, fmt.Errorf("constructing multi level labels: position %d: must be slice", k)
		}
	}
	var numNewRows int
	for k := range labels {
		v := reflect.ValueOf(labels[k])
		if k == 0 {
			numNewRows = v.Len()
		} else {
			numNewRows *= v.Len()
		}
	}
	ret := make([]interface{}, len(labels))
	for k := range labels {
		v := reflect.ValueOf(labels[k])
		newValues := reflect.MakeSlice(v.Type(), numNewRows, numNewRows)
		numRepeats := numNewRows / v.Len()
		// for first slice, repeat each value individually
		if k == 0 {
			for i := 0; i < v.Len(); i++ {
				for j := 0; j < numRepeats; j++ {
					offset := j + i*numRepeats
					src := v.Index(i)
					dst := newValues.Index(offset)
					dst.Set(src)
				}
			}
		} else {
			// otherwise, repeat values in blocks as-is
			for j := 0; j < numRepeats; j++ {
				for i := 0; i < v.Len(); i++ {
					offset := i + j*v.Len()
					src := v.Index(i)
					dst := newValues.Index(offset)
					dst.Set(src)
				}
			}
		}
		ret[k] = newValues.Interface()
	}

	return ret, nil
}

// Copy returns a new DataFrame with identical values as the original but no shared objects
// (i.e., all internals are newly allocated).
func (df *DataFrame) Copy() *DataFrame {
	colLevelNames := make([]string, len(df.colLevelNames))
	copy(colLevelNames, df.colLevelNames)

	ret := &DataFrame{
		values:        copyContainers(df.values),
		labels:        copyContainers(df.labels),
		err:           df.err,
		colLevelNames: colLevelNames,
		name:          df.name,
	}

	return ret
}

// ConcatSeries merges multiple Series from left-to-right, one after the other, via left joins on shared keys.
// For advanced cases, use df.LookupAdvanced() + df.WithCol().
func ConcatSeries(series ...*Series) (*DataFrame, error) {
	var ret *DataFrame
	for k, s := range series {
		if k == 0 {
			ret = s.DataFrame()
		} else {
			var err error
			ret, err = ret.Merge(s.DataFrame())
			if err != nil {
				return nil, fmt.Errorf("concatenating Series: %v", err)
			}
		}
	}
	return ret, nil
}

// Cast coerces the underlying container values (column or label level) to
// []float64, []string, []time.Time (aka timezone-aware DateTime), []civil.Date, or []civil.Time
// and caches the []byte values of the container (if inexpensive).
// Use cast to improve performance when calling multiple operations on values.
func (df *DataFrame) Cast(containerAsType map[string]DType) {
	mergedLabelsAndCols := append(df.labels, df.values...)
	for name, dtype := range containerAsType {
		index, err := indexOfContainer(name, mergedLabelsAndCols)
		if err != nil {
			df.resetWithError(fmt.Errorf("type casting: %v", err))
			return
		}
		mergedLabelsAndCols[index].cast(dtype)
	}
	return
}

// -- READERS

// ReadOptionHeaders configures a read function to expect n rows to be column headers (default: 1).
func ReadOptionHeaders(n int) func(*readConfig) {
	return func(r *readConfig) {
		r.numHeaderRows = n
	}
}

// ReadOptionLabels configures a read function to expect the first n columns to be label levels (default: 0).
func ReadOptionLabels(n int) func(*readConfig) {
	return func(r *readConfig) {
		r.numLabelLevels = n
	}
}

// ReadOptionDelimiter configures a read function to use sep as a field delimiter for use in ReadCSV (default: ",").
func ReadOptionDelimiter(sep rune) func(*readConfig) {
	return func(r *readConfig) {
		r.delimiter = sep
	}
}

// ReadOptionSwitchDims configures a read function to expect columns to be the major dimension of csv data
// (default: expects rows to be the major dimension).
// For example, when reading this data:
//
// [["foo", "bar"], ["baz", "qux"]]
//
// default				   		ReadOptionSwitchDims()
// (major dimension: rows)			(major dimension: columns)
//	foo bar							foo baz
//  baz qux							bar qux
func ReadOptionSwitchDims() func(*readConfig) {
	return func(r *readConfig) {
		r.majorDimIsCols = true
	}
}

// ReadCSV reads csv records in r into a Dataframe (configured by options).
// Rows must be the major dimension of r.
// For advanced cases, use the standard csv library NewReader().ReadAll() + tada.ReadCSVFromRecords().
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionDelimiter.
//
// Default if no options are supplied:
// 1 header row; no labels; field delimiter is ","
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ReadCSV(r io.Reader, options ...ReadOption) (*DataFrame, error) {
	config := setReadConfig(options)
	b, err := ioutil.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading csv: %s", err)
	}
	numRows, numCols, err := extractCSVDimensions(b, config.delimiter)
	if err != nil {
		return nil, fmt.Errorf("reading csv: %v", err)
	}
	retVals := makeStringMatrix(numCols, numRows)
	retNulls := makeBoolMatrix(numCols, numRows)
	data := bytes.NewReader(b)
	err = readCSVBytes(data, retVals, retNulls, config.delimiter)
	if err != nil {
		return nil, fmt.Errorf("reading csv: %s", err)
	}
	return makeDataFrameFromMatrices(retVals, retNulls, config), nil
}

// ReadCSVFromRecords reads records into a DataFrame (configured by options).
// Often used with encoding/csv.NewReader().ReadAll()
// All columns will be read as []string.
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionSwitchDims.
//
// Default if no options are supplied:
// 1 header row; no labels; rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ReadCSVFromRecords(records [][]string, options ...ReadOption) (ret *DataFrame, err error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("reading csv from records: must have at least one record")
	}
	if len(records[0]) == 0 {
		return nil, fmt.Errorf("reading csv from records: first record cannot be empty")
	}
	config := setReadConfig(options)

	if config.majorDimIsCols {
		ret, err = readCSVByCols(records, config)
	} else {
		ret, err = readCSVByRows(records, config)
	}
	if err != nil {
		return nil, fmt.Errorf("reading csv from records: %v", err)
	}
	return ret, nil
}

// ReadInterfaceRecords reads records into a DataFrame (configured by options).
// All columns will be read as []interface{}.
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionSwitchDims.
//
// Default if no options are supplied:
// 1 header row; no labels; rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ReadInterfaceRecords(records [][]interface{}, options ...ReadOption) (ret *DataFrame, err error) {
	if len(records) == 0 {
		return nil, fmt.Errorf("reading records from [][]interface{}: must have at least one record")
	}
	if len(records[0]) == 0 {
		return nil, fmt.Errorf("reading records from [][]interface{}: first record cannot be empty")
	}
	config := setReadConfig(options)
	var slices []interface{}
	if !config.majorDimIsCols {
		slices, err = readNestedInterfaceByRows(records)
	} else {
		slices, err = readNestedInterfaceByCols(records)
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
		if ret.err != nil {
			return nil, fmt.Errorf("reading records from [][]interface{}: %v", ret.err)
		}
		ret = ret.SetLabelNames(labelNames).SetColNames(colNames)
	} else {
		ret = NewDataFrame(slices)
		if ret.err != nil {
			return nil, fmt.Errorf("reading records from [][]interface{}: %v", ret.err)
		}
		if config.numHeaderRows > 0 {
			ret = ret.SetColNames(colNames)
		}
	}
	return ret, nil
}

// ReadStruct reads the exported fields in strct into a DataFrame.
// strct must be a struct or pointer to a struct.
// If any exported field in strct is nil, returns an error.
//
// If a "tada" tag is present with the value "isNull", this field must be [][]bool with one equal-lengthed slice for each exported field.
// These values will set the null status for each of the resulting value containers in the DataFrame, from left-to-right.
// If a "tada" tag has any other value, the resulting value container will have the same name as the tag value.
// Otherwise, the value container will have the same name as the exported field.
func ReadStruct(strct interface{}, options ...ReadOption) (*DataFrame, error) {
	config := setReadConfig(options)
	if reflect.TypeOf(strct).Kind() == reflect.Ptr {
		strct = reflect.ValueOf(strct).Elem().Interface()
	}
	if reflect.TypeOf(strct).Kind() != reflect.Struct {
		return nil, fmt.Errorf("reading struct: strct must be reflect.Kind struct, not %s",
			reflect.TypeOf(strct).Kind())
	}
	labels := make([]interface{}, 0)
	values := make([]interface{}, 0)
	labelNames := make([]string, 0)
	colNames := make([]string, 0)
	v := reflect.ValueOf(strct)
	var hasNullTag bool
	var nullField string
	nullTag := "isNull"
	var offset int
	for k := 0; k < v.NumField(); k++ {
		field := reflect.TypeOf(strct).Field(k)
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
					return nil, fmt.Errorf("reading struct: writing nulls: position %d: %v", incrementor, err)
				}
			}
		}
	}
	return df, nil
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

// ReadStructSlice reads a slice of structs into a DataFrame with field names converted to column names,
// field values converted to column values, and default labels. The structs must all be of the same type.
//
// A default label level named *0 is inserted ([]int incrementing from 0). Default label names are hidden on printing.
func ReadStructSlice(slice interface{}) (*DataFrame, error) {
	values, err := readStruct(slice)
	if err != nil {
		return nil, fmt.Errorf("reading struct slice: %v", err)
	}
	defaultLabels := makeDefaultLabels(0, reflect.ValueOf(slice).Len(), true)
	return &DataFrame{
		values:        values,
		labels:        []*valueContainer{defaultLabels},
		colLevelNames: []string{"*0"},
	}, nil
}

// Series converts a single-columned DataFrame to a Series that shares the same underlying values and labels.
func (df *DataFrame) Series() *Series {
	if len(df.values) != 1 {
		return seriesWithError(fmt.Errorf("converting to Series: DataFrame must have a single column"))
	}
	return &Series{
		values:     df.values[0],
		labels:     df.labels,
		sharedData: true,
	}
}

// EqualsCSV reads want (configured by wantOptions) into a dataframe,
// converts both df and want into [][]string records,
// and evaluates whether the stringified values match.
// If they do not match, returns a tablediff.Differences object that can be printed to isolate their differences.
//
// If includeLabels is true, then df's labels are included as columns.
func (df *DataFrame) EqualsCSV(includeLabels bool, want io.Reader, wantOptions ...ReadOption) (bool, *tablediff.Differences, error) {
	config := setReadConfig(wantOptions)
	df2, err := ReadCSV(want, wantOptions...)
	if err != nil {
		return false, nil, fmt.Errorf("comparing csv: reading want: %v", err)
	}

	got := df.CSVRecords(writeOptionIncludeLabels(includeLabels))
	// df2 has default labels? exclude them
	wantDF := df2.CSVRecords(writeOptionIncludeLabels(config.numLabelLevels > 0))
	diffs, eq := tablediff.Diff(got, wantDF)
	return eq, diffs, nil
}

// -- WRITERS

// WriteOptionExcludeLabels excludes the label levels from the output.
func WriteOptionExcludeLabels() func(*writeConfig) {
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

// WriteOptionDelimiter configures a write function to use sep as a field delimiter for use in write functions (default: ",").
func WriteOptionDelimiter(sep rune) func(*writeConfig) {
	return func(w *writeConfig) {
		w.delimiter = sep
	}
}

// InterfaceRecords writes a DataFrame to a [][]interface{} with columns as the major dimension.
// Null values are replaced with "(null)".
func (df *DataFrame) InterfaceRecords(options ...WriteOption) [][]interface{} {
	config := setWriteConfig(options)
	containers := append(df.labels, df.values...)
	if !config.includeLabels {
		containers = df.values
	}
	ret := make([][]interface{}, len(containers))
	for k := range ret {
		ret[k] = containers[k].interfaceSlice(true)
	}
	return ret
}

// CSVRecords writes a DataFrame to a [][]string with rows as the major dimension.
// Null values are replaced with "(null)".
func (df *DataFrame) CSVRecords(options ...WriteOption) [][]string {
	config := setWriteConfig(options)
	transposedStringValues, err := df.toCSVByRows(config.includeLabels)
	if err != nil {
		return nil
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
	return transposedStringValues
}

// Struct writes the values of the df containers into structPointer.
// Returns an error if df does not contain, from left-to-right, the same container names and types
// as the exported fields that appear, from top-to-bottom, in structPointer.
// Exported struct fields must be types that are supported by NewDataFrame().
// If a "tada" tag is present with the value "isNull", this field must be [][]bool.
// The null status of each value container in the DataFrame, from left-to-right, will be written into this field in equal-lengthed slices.
// If df contains additional containers beyond those in structPointer, those are ignored.
func (df *DataFrame) Struct(structPointer interface{}, options ...WriteOption) error {
	config := setWriteConfig(options)
	if reflect.TypeOf(structPointer).Kind() != reflect.Ptr {
		return fmt.Errorf("writing to struct: structPointer must be pointer to struct, not %s", reflect.TypeOf(structPointer).Kind())
	}
	if reflect.TypeOf(structPointer).Elem().Kind() != reflect.Struct {
		return fmt.Errorf("writing to struct: structPointer must be pointer to struct, not to %s", reflect.TypeOf(structPointer).Elem().Kind())
	}
	v := reflect.ValueOf(structPointer).Elem()
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
		field := reflect.TypeOf(structPointer).Elem().Field(k)
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

// WriteCSV converts a DataFrame to a csv with rows as the major dimension,
// and writes the output to w.
// Null values are replaced with "(null)".
func (df *DataFrame) WriteCSV(w io.Writer, options ...WriteOption) error {
	config := setWriteConfig(options)
	ret := df.CSVRecords(writeOptionIncludeLabels(config.includeLabels))
	var b bytes.Buffer
	cw := csv.NewWriter(&b)
	cw.Comma = config.delimiter
	cw.WriteAll(ret)
	_, err := w.Write(b.Bytes())
	return err
}

// WriteMockCSV reads r (configured by options) and writes n mock rows to w,
// with column names and types inferred based on the data in src.
// Regardless of the major dimension of src, the major dimension of the output is rows.
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionSwitchDims.
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
	data, err := ReadCSV(r, options...)
	if err != nil {
		return fmt.Errorf("writing mock csv: reading r: %v", err)
	}
	// data has default labels? exclude them
	src := data.CSVRecords(writeOptionIncludeLabels(config.numLabelLevels > 0))

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

// -- GETTERS

// String prints the DataFrame in table form, with the number of rows constrained by optionMaxRows,
// and the number of columns constrained by optionMaxColumns,
// which may be configured with PrintOptionMaxRows(n) and PrintOptionMaxColumns(n), respectively.
// By default, repeated values are merged together, but this behavior may be disabled with PrintOptionAutoMerge(false).
// By default, overly-wide non-header cells are truncated, but this behavior may be changed to wrapping with PrintOptionWrapLines(true).
func (df *DataFrame) String() string {
	if df.err != nil {
		return fmt.Sprintf("Error: %v", df.err)
	}
	var data [][]string
	if df.Len() <= optionMaxRows {
		data = df.CSVRecords()
	} else {
		// truncate rows
		n := optionMaxRows / 2
		topHalf := df.Head(n).CSVRecords()
		bottomHalf := df.Tail(n).CSVRecords()[df.numColLevels():]
		filler := make([]string, df.NumLevels()+df.NumColumns())
		for k := range filler {
			filler[k] = "..."
		}
		data = append(
			append(topHalf, filler),
			bottomHalf...)
	}
	// do not print *0-type label names
	for j := 0; j < df.NumLevels(); j++ {
		row := df.numColLevels() - 1
		data[row][j] = suppressDefaultName(data[row][j])
	}

	// truncate columns
	if df.NumColumns() >= optionMaxColumns {
		n := (optionMaxColumns / 2)

		for i := range data {
			labels := data[i][:df.NumLevels()]
			leftHalf := data[i][df.NumLevels() : n+df.NumLevels()]
			filler := "..."
			rightHalf := data[i][df.NumLevels()+df.NumColumns()-n:]
			data[i] = append(
				append(
					labels,
					append(leftHalf, filler)...),
				rightHalf...)
		}
	}
	// truncate cells
	if defaultMaxCellWidth() != optionMaxCellWidth {
		for i := range data {
			for k := range data[i] {
				if r := []rune(data[i][k]); len(r) > optionMaxCellWidth {
					data[i][k] = string(r[:optionMaxCellWidth-3]) + "..."
				}
			}
		}
	}
	// create table
	var buf bytes.Buffer
	table := tablewriter.NewTable(&buf)
	// configure table
	if optionMergeRepeats {
		table.MergeRepeats()
	}
	if !optionWrapLines {
		table.TruncateWideCells()
	}
	table.SetAlignment(tablewriter.AlignRight)
	table.SetLabelLevelCount(df.NumLevels())

	// write headers and rows
	for l := 0; l < df.numColLevels(); l++ {
		table.AppendHeaderRow(data[l])
	}
	table.AppendRows(data[df.numColLevels():])
	table.Render()
	ret := string(buf.Bytes())
	// append optional caption
	if df.name != "" {
		ret += fmt.Sprintf("name: %v\n", df.name)
	}
	return ret
}

// At returns the Element at the row and column index positions.
// If row or column is out of range, returns nil.
func (df *DataFrame) At(row, column int) *Element {
	if row >= df.Len() {
		return nil
	}
	if column >= df.NumColumns() {
		return nil
	}
	v := reflect.ValueOf(df.values[column].slice)
	return &Element{
		Val:    v.Index(row).Interface(),
		IsNull: df.values[column].isNull[row],
	}
}

// Len returns the number of rows in each column of the DataFrame.
func (df *DataFrame) Len() int {
	return reflect.ValueOf(df.values[0].slice).Len()
}

// Err returns the most recent error attached to the DataFrame, if any.
func (df *DataFrame) Err() error {
	return df.err
}

// HasType returns the index positions of all label and column containers
// containing a slice of values where reflect.Type.String() == sliceType.
// Container index positions may then be supplied to df.SubsetLabels() or df.SubsetCols().
//
// For example, to search for datetime labels: labels, _ := df.HasType("[]time.Time")
//
// To search for float64 columns: _, cols := df.HasType("[]float64")
//
func (df *DataFrame) HasType(sliceType string) (labelIndex, columnIndex []int) {
	for j := range df.labels {
		if df.labels[j].dtype().String() == sliceType {
			labelIndex = append(labelIndex, j)
		}
	}
	for k := range df.values {
		if df.values[k].dtype().String() == sliceType {
			columnIndex = append(columnIndex, k)
		}
	}
	return
}

func (df *DataFrame) numColLevels() int {
	return len(df.colLevelNames)
}

// NumColumns returns the number of colums in the DataFrame.
func (df *DataFrame) NumColumns() int {
	return len(df.values)
}

// NumLevels returns the number of label levels in the DataFrame.
func (df *DataFrame) NumLevels() int {
	return len(df.labels)
}

func listNames(columns []*valueContainer) []string {
	ret := make([]string, len(columns))
	for k := range columns {
		ret[k] = columns[k].name
	}
	return ret
}

func listNamesAtLevel(columns []*valueContainer, level int, numLevels int) ([]string, error) {
	ret := make([]string, len(columns))
	if level >= numLevels {
		return nil, fmt.Errorf("level out of range: %d >= %d", level, numLevels)
	}
	for k := range columns {
		levels := splitNameIntoLevels(columns[k].name)
		ret[k] = levels[level]
	}
	return ret, nil
}

// ListColNames returns the name of all the columns in the DataFrame, in order.
// If df has multiple column levels, each column name is a single string with level values separated by "|" (may be changed with SetOptionDefaultSeparator).
// To return the names at a specific level, use ListColNamesAtLevel().
func (df *DataFrame) ListColNames() []string {
	return listNames(df.values)
}

// ListColNamesAtLevel returns the name of all the columns in the DataFrame, in order, at the supplied column level.
// If level is out of range, returns a nil slice.
func (df *DataFrame) ListColNamesAtLevel(level int) []string {
	ret, err := listNamesAtLevel(df.values, level, df.numColLevels())
	if err != nil {
		return nil
	}
	return ret
}

// ListLabelNames returns the name of all the label levels in the DataFrame, in order.
func (df *DataFrame) ListLabelNames() []string {
	return listNames(df.labels)
}

// HasLabels returns an error if the DataFrame does not contain all of the labelNames supplied.
func (df *DataFrame) HasLabels(labelNames ...string) error {
	for _, name := range labelNames {
		_, err := indexOfContainer(name, df.labels)
		if err != nil {
			return fmt.Errorf("verifying labels: %v", err)
		}
	}
	return nil
}

// HasCols returns an error if the DataFrame does not contain all of the colNames supplied.
func (df *DataFrame) HasCols(colNames ...string) error {
	for _, name := range colNames {
		_, err := indexOfContainer(name, df.values)
		if err != nil {
			return fmt.Errorf("verifying columns: %v", err)
		}
	}
	return nil
}

// InPlace returns a DataFrameMutator, which contains most of the same methods as DataFrame
// but never returns a new DataFrame.
// If you want to save memory and improve performance and do not need to preserve the original DataFrame,
// consider using InPlace().
func (df *DataFrame) InPlace() *DataFrameMutator {
	return &DataFrameMutator{dataframe: df}
}

// Subset returns only the rows specified at the index positions, in the order specified.
//Returns a new DataFrame.
func (df *DataFrame) Subset(index []int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Subset(index)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Subset(index []int) error {
	for k := range df.dataframe.values {
		err := df.dataframe.values[k].subsetRows(index)
		if err != nil {
			return fmt.Errorf("subsetting rows: %v", err)
		}
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j].subsetRows(index)
	}
	return nil
}

// SwapLabels swaps the label levels with names i and j.
// Returns a new DataFrame.
func (df *DataFrame) SwapLabels(i, j string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().SwapLabels(i, j)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// SwapLabels swaps the label levels with names i and j.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SwapLabels(i, j string) error {
	index1, err := indexOfContainer(i, df.dataframe.labels)
	if err != nil {
		return fmt.Errorf("swapping labels:i: %v", err)
	}
	index2, err := indexOfContainer(j, df.dataframe.labels)
	if err != nil {
		return fmt.Errorf("swapping labels:j: %v", err)
	}
	df.dataframe.labels[index1], df.dataframe.labels[index2] = df.dataframe.labels[index2], df.dataframe.labels[index1]
	return nil
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Returns a new DataFrame.
func (df *DataFrame) SubsetLabels(index []int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().SubsetLabels(index)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SubsetLabels(index []int) error {
	labels, err := subsetContainers(df.dataframe.labels, index)
	if err != nil {
		return fmt.Errorf("subsetting labels: %v", err)
	}
	df.dataframe.labels = labels
	return nil
}

// SubsetCols returns only the labels specified at the index positions, in the order specified.
// Returns a new DataFrame.
func (df *DataFrame) SubsetCols(index []int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().SubsetCols(index)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// SubsetCols returns only the labels specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SubsetCols(index []int) error {
	cols, err := subsetContainers(df.dataframe.values, index)
	if err != nil {
		return fmt.Errorf("subsetting columns: %v", err)
	}
	df.dataframe.values = cols
	return nil
}

// DeduplicateNames deduplicates the names of containers (label levels and columns) from left-to-right
// by appending _n to duplicate names, where n is equal to the number of times that name has already appeared.
// Returns a new DataFrame.
func (df *DataFrame) DeduplicateNames() *DataFrame {
	df = df.Copy()
	df.InPlace().DeduplicateNames()
	return df
}

// DeduplicateNames deduplicates the names of containers (label levels and columns) from left-to-right
// by appending _n to duplicate names, where n is equal to the number of times that name has already appeared.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DeduplicateNames() {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	deduplicateContainerNames(mergedLabelsAndCols)
}

// NameOfLabel returns the name of the label level at index position n.
// If n is out of range, returns "-out of range-"
func (df *DataFrame) NameOfLabel(n int) string {
	return nameOfContainer(df.labels, n)
}

// NameOfCol returns the name of the column at index position n.
// If n is out of range, returns "-out of range-"
func (df *DataFrame) NameOfCol(n int) string {
	return nameOfContainer(df.values, n)
}

// IndexOfContainer returns the index position of the first container with a name matching name (case-sensitive).
// If name does not match any container, -1 is returned.
// If columns is true, only column names will be searched.
// If columns is false, only label level names will be searched.
func (df *DataFrame) IndexOfContainer(name string, columns bool) int {
	var i int
	var err error
	if !columns {
		i, err = indexOfContainer(name, df.labels)
	}
	if columns {
		i, err = indexOfContainer(name, df.values)
	}
	if err != nil {
		return -1
	}
	return i
}

// GetLabels returns label levels as interface{} slices within an []interface
// that may be supplied as optional labels argument to NewSeries() or NewDataFrame().
// NB: If supplying this output to either of these constructors,
// be sure to use the spread operator (...), or else the labels will not be read as separate levels.
func (df *DataFrame) GetLabels() []interface{} {
	var ret []interface{}
	labels := copyContainers(df.labels)
	for j := range labels {
		ret = append(ret, labels[j].slice)
	}
	return ret
}

// LabelsAsSeries finds the first label level with matching name
// and returns the values as a Series.
// Similar to Col(), but selects label values instead of column values.
// The labels in the Series are shared with the labels in the DataFrame.
// If label level name is default (prefixed with *), the prefix is removed.
func (df *DataFrame) LabelsAsSeries(name string) *Series {
	index, err := indexOfContainer(name, df.labels)
	if err != nil {
		return seriesWithError(fmt.Errorf("converting labels to Series: %v", err))
	}
	values := df.labels[index]
	retValues := &valueContainer{
		slice:  values.slice,
		isNull: values.isNull,
		name:   removeDefaultNameIndicator(values.name),
	}
	return &Series{
		values:     retValues,
		labels:     df.labels,
		sharedData: true,
	}
}

// Col finds the first column with matching name and returns as a Series.
// Similar to SelectLabels(), but selects column values instead of label values.
func (df *DataFrame) Col(name string) *Series {
	index, err := indexOfContainer(name, df.values)
	if err != nil {
		return seriesWithError(fmt.Errorf("getting column: %v", err))
	}
	return &Series{
		values:     df.values[index],
		labels:     df.labels,
		sharedData: true,
	}
}

// Cols returns all columns with matching names.
func (df *DataFrame) Cols(names ...string) *DataFrame {
	vals := make([]*valueContainer, len(names))
	for i, name := range names {
		index, err := indexOfContainer(name, df.values)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("getting columns: %v", err))
		}
		vals[i] = df.values[index]
	}
	return &DataFrame{
		values: vals,
		labels: df.labels,
		name:   df.name,
	}
}

// Head returns the first n rows of the DataFrame.
// If n is greater than the length of the DataFrame, returns the entire DataFrame.
// In either case, returns a new DataFrame.
func (df *DataFrame) Head(n int) *DataFrame {
	if df.Len() < n {
		n = df.Len()
	}
	retVals := make([]*valueContainer, len(df.values))
	for k := range df.values {
		retVals[k] = df.values[k].head(n)
	}
	retLabels := make([]*valueContainer, df.NumLevels())
	for j := range df.labels {
		retLabels[j] = df.labels[j].head(n)
	}
	return &DataFrame{values: retVals, labels: retLabels, name: df.name, colLevelNames: df.colLevelNames}
}

// Tail returns the last n rows of the DataFrame.
// If n is greater than the length of the DataFrame, returns the entire DataFrame.
// In either case, returns a new DataFrame.
func (df *DataFrame) Tail(n int) *DataFrame {
	if df.Len() < n {
		n = df.Len()
	}
	retVals := make([]*valueContainer, len(df.values))
	for k := range df.values {
		retVals[k] = df.values[k].tail(n)
	}
	retLabels := make([]*valueContainer, df.NumLevels())
	for j := range df.labels {
		retLabels[j] = df.labels[j].tail(n)
	}
	return &DataFrame{values: retVals, labels: retLabels, name: df.name, colLevelNames: df.colLevelNames}
}

// Range returns the rows of the DataFrame starting at first and ending immediately prior to last (left-inclusive, right-exclusive).
// If either first or last is greater than the length of the DataFrame, an error is returned.
// Returns a new DataFrame.
func (df *DataFrame) Range(first, last int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Range(first, last)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Range returns the rows of the DataFrame starting at first and ending immediately prior to last (left-inclusive, right-exclusive).
// If first or last is out of range, an error is returned.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Range(first, last int) error {
	if first > last {
		return fmt.Errorf("range: first is greater than last (%d > %d)", first, last)
	}
	if first >= df.dataframe.Len() {
		return fmt.Errorf("range: first index out of range [%d] with length %d", first, df.dataframe.Len())
	} else if last > df.dataframe.Len() {
		// permissible values for last includes the length of the dataframe
		return fmt.Errorf("range: last index out of range [%d] with max index %d (length + 1)", last, df.dataframe.Len()+1)
	}
	for k := range df.dataframe.values {
		df.dataframe.values[k] = df.dataframe.values[k].rangeSlice(first, last)
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j] = df.dataframe.labels[j].rangeSlice(first, last)
	}
	return nil
}

// FillNull fills null values and makes them non-null based on how,
// a map of container names (either column or label names) and tada.NullFiller structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its NullFiller struct is the strategy used to replace null values in that container.
// FillForward fills null values with the most recent non-null value in the container.
// FillBackward fills null values with the next non-null value in the container.
// FillZero fills null values with the zero value for that container type.
// FillFloat converts the container values to float64 and fills null values with the value supplied.
// If no field is selected, the container values are converted to float64 and all null values are filled with 0.
// Returns a new DataFrame.
func (df *DataFrame) FillNull(how map[string]NullFiller) *DataFrame {
	df = df.Copy()
	err := df.InPlace().FillNull(how)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// FillNull fills null values and makes them non-null based on how.
// How is a map of container names (either column or label names) and NullFillers.
// For each container name supplied, the first field selected (i.e., not left blank)
// in the NullFiller is the strategy used to replace null values.
// FillForward fills null values with the most recent non-null value in the container.
// FillBackward fills null values with the next non-null value in the container.
// FillZero fills null values with the zero value for that container type.
// FillFloat converts the container values to float64 and fills null values with the value supplied.
// If no field is selected, the container values are converted to float64 and all null values are filled with 0.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) FillNull(how map[string]NullFiller) error {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	for name, filler := range how {
		index, err := indexOfContainer(name, mergedLabelsAndCols)
		if err != nil {
			return fmt.Errorf("filling null rows: %v", err)
		}
		mergedLabelsAndCols[index].fillnull(filler)
	}
	return nil
}

// DropNull removes rows with a null value in any column.
// If subset is supplied, removes any rows with null values in any of the specified columns.
// Returns a new DataFrame.
func (df *DataFrame) DropNull(subset ...string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().DropNull(subset...)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// DropNull removes rows with a null value in any column.
// If subset is supplied, removes any rows with null values in any of the specified columns.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) DropNull(subset ...string) error {
	var index []int
	if len(subset) == 0 {
		index = makeIntRange(0, len(df.dataframe.values))
	} else {
		for _, name := range subset {
			i, err := indexOfContainer(name, df.dataframe.values)
			if err != nil {
				return fmt.Errorf("dropping null rows: %v", err)
			}
			index = append(index, i)
		}
	}

	subIndexes := make([][]int, len(index))
	for k := range index {
		subIndexes[k] = df.dataframe.values[k].valid()
	}
	allValid := intersection(subIndexes, df.dataframe.Len())
	df.Subset(allValid)
	return nil
}

// IsNull returns all the rows with any null values.
// If subset is supplied, returns all the rows with all non-null values in the specified columns.
// Returns a new DataFrame.
func (df *DataFrame) IsNull(subset ...string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().IsNull(subset...)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// IsNull returns all the rows with any null values.
// If subset is supplied, returns all the rows with all non-null values in the specified columns.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) IsNull(subset ...string) error {
	var index []int
	if len(subset) == 0 {
		index = makeIntRange(0, len(df.dataframe.values))
	} else {
		for _, name := range subset {
			i, err := indexOfContainer(name, df.dataframe.values)
			if err != nil {
				return fmt.Errorf("getting null rows: %v", err)
			}
			index = append(index, i)
		}
	}

	subIndexes := make([][]int, len(index))
	for k := range index {
		subIndexes[k] = df.dataframe.values[k].null()
	}
	anyNull := union(subIndexes)
	df.Subset(anyNull)
	return nil
}

// SetNulls overwrites the underlying boolean slice that records whether each value is null or not
// for the container at position n (either labels or columns).
func (df *DataFrame) SetNulls(n int, nulls []bool) error {
	mergedLabelsAndCols := append(df.labels, df.values...)
	if n >= len(mergedLabelsAndCols) {
		return fmt.Errorf("setting null slice: n cannot be greater than the number of labels and columns (%d)", len(mergedLabelsAndCols))
	}
	if len(nulls) != df.Len() {
		return fmt.Errorf("setting null slice: nulls must be same length as existing null slice (%d != %d)",
			len(nulls), df.Len())
	}
	mergedLabelsAndCols[n].isNull = nulls
	return nil
}

// FilterCols returns the columns with names that satisfy lambda at the supplied column level.
// level should be 0 unless df has multiple column levels.
func (df *DataFrame) FilterCols(lambda func(string) bool, level int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().FilterCols(lambda, level)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// FilterCols returns the columns with names that satisfy lambda at the supplied column level.
// level should be 0 unless df has multiple column levels.
func (df *DataFrameMutator) FilterCols(lambda func(string) bool, level int) error {
	if lambda == nil {
		return fmt.Errorf("filtering columns: must provide lambda function")
	}
	var subset []int
	names, err := listNamesAtLevel(df.dataframe.values, level, df.dataframe.numColLevels())
	if err != nil {
		return fmt.Errorf("filtering columns: %v", err)
	}
	for k := range names {
		if lambda(names[k]) {
			subset = append(subset, k)
		}
	}
	df.SubsetCols(subset)
	return nil
}

// -- SETTERS

// WithLabels resolves as follows:
//
// If a scalar string is supplied as input and a label level exists that matches name: rename the level to match input.
// In this case, name must already exist.
//
// If a slice is supplied as input and a label level exists that matches name: replace the values at this level to match input.
// If a slice is supplied as input and a label level does not exist that matches name: append a new level named name and values matching input.
// If input is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, returns a new DataFrame.
func (df *DataFrame) WithLabels(name string, input interface{}) *DataFrame {
	df = df.Copy()
	err := df.InPlace().WithLabels(name, input)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as input and a label level exists that matches name: rename the level to match input.
// In this case, name must already exist.
//
// If a slice is supplied as input and a label level exists that matches name: replace the values at this level to match input.
// If a slice is supplied as input and a label level does not exist that matches name: append a new level named name and values matching input.
// If input is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, modifies the underlying DataFrame in place.
func (df *DataFrameMutator) WithLabels(name string, input interface{}) error {
	labels, err := withColumn(df.dataframe.labels, name, input, df.dataframe.Len())
	if err != nil {
		return fmt.Errorf("setting labels: %v", err)
	}
	df.dataframe.labels = labels
	return nil
}

// WithCol resolves as follows:
//
// If a scalar string is supplied as input and a column exists that matches name: rename the column to match input.
// In this case, name must already exist.
//
// If a slice is supplied as input and a column exists that matches name: replace the values at this column to match input.
// If a slice is supplied as input and a column does not exist that matches name: append a new column named name and values matching input.
// If input is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, returns a new DataFrame.
func (df *DataFrame) WithCol(name string, input interface{}) *DataFrame {
	df = df.Copy()
	err := df.InPlace().WithCol(name, input)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// WithCol resolves as follows:
//
// If a scalar string is supplied as input and a column exists that matches name: rename the column to match input.
// In this case, name must already exist.
//
// If a slice is supplied as input and a column exists that matches name: replace the values at this column to match input.
// If a slice is supplied as input and a column does not exist that matches name: append a new column named name and values matching input.
// If input is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, modifies the underlying DataFrame in place.
func (df *DataFrameMutator) WithCol(name string, input interface{}) error {
	cols, err := withColumn(df.dataframe.values, name, input, df.dataframe.Len())
	if err != nil {
		return fmt.Errorf("setting column: %v", err)
	}
	df.dataframe.values = cols
	return nil
}

// Shuffle randomizes the row order of the DataFrame.
// Returns a new DataFrame.
func (df *DataFrame) Shuffle(seed int64) *DataFrame {
	df = df.Copy()
	df.InPlace().Shuffle(seed)
	return df
}

// Shuffle randomizes the row order of the DataFrame.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) Shuffle(seed int64) {
	index := makeIntRange(0, df.dataframe.Len())
	rand.Seed(seed)
	rand.Shuffle(len(index), func(i, j int) { index[i], index[j] = index[j], index[i] })
	df.Subset(index)
}

// DropLabels drops the first label level matching name.
// Returns a new DataFrame.
func (df *DataFrame) DropLabels(name string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().DropLabels(name)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// DropLabels drops the first label level matching name.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropLabels(name string) error {
	newCols, err := dropFromContainers(name, df.dataframe.labels)
	if err != nil {
		return fmt.Errorf("dropping labels: %v", err)
	}
	df.dataframe.labels = newCols
	return nil
}

// DropCol drops the first column matching name.
// Returns a new DataFrame.
func (df *DataFrame) DropCol(name string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().DropCol(name)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// DropCol drops the first column matching name.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropCol(name string) error {
	newCols, err := dropFromContainers(name, df.dataframe.values)
	if err != nil {
		return fmt.Errorf("dropping column: %v", err)
	}
	df.dataframe.values = newCols
	return nil
}

// DropRow removes the row at the specified index.
// Returns a new DataFrame.
func (df *DataFrame) DropRow(index int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().DropRow(index)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// DropRow removes the row at the specified index.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropRow(index int) error {
	for k := range df.dataframe.values {
		err := df.dataframe.values[k].dropRow(index)
		if err != nil {
			return fmt.Errorf("dropping row: %v", err)
		}
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j].dropRow(index)
	}
	return nil
}

// Append adds the other labels and values as new rows to the DataFrame.
// If the types of any container do not match, all the values in that container are coerced to string.
// Returns a new DataFrame.
func (df *DataFrame) Append(other *DataFrame) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Append(other)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Append adds the other labels and values as new rows to the DataFrame.
// If the types of any container do not match, all the values in that container are coerced to string.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Append(other *DataFrame) error {
	if len(other.labels) != len(df.dataframe.labels) {
		return fmt.Errorf("appending rows: other must have same number of label levels as original (%d != %d)",
			len(other.labels), len(df.dataframe.labels))
	}
	if len(other.values) != len(df.dataframe.values) {
		return fmt.Errorf("appending rows: other must have same number of columns as original (%d != %d)",
			len(other.values), len(df.dataframe.values))
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j] = df.dataframe.labels[j].append(other.labels[j])
	}
	for k := range df.dataframe.values {
		df.dataframe.values[k] = df.dataframe.values[k].append(other.values[k])
	}
	return nil
}

// Relabel resets the DataFrame labels to default labels (e.g., []int from 0 to df.Len()-1, with *0 as name).
// Returns a new DataFrame.
func (df *DataFrame) Relabel() *DataFrame {
	df = df.Copy()
	df.InPlace().Relabel()
	return df
}

// Relabel resets the DataFrame labels to default labels (e.g., []int from 0 to df.Len()-1, with *0 as name).
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Relabel() {
	df.dataframe.labels = []*valueContainer{makeDefaultLabels(0, df.dataframe.Len(), true)}
	return
}

// SetAsLabels appends the column(s) supplied as colNames as label levels and drops the column(s).
// The number of colNames supplied must be less than the number of columns in the Series.
// Returns a new DataFrame.
func (df *DataFrame) SetAsLabels(colNames ...string) *DataFrame {
	df = df.Copy()
	df.InPlace().SetAsLabels(colNames...)
	return df
}

// SetAsLabels appends the column(s) supplied as colNames as label levels and drops the column(s).
// The number of colNames supplied must be less than the number of columns in the Series.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SetAsLabels(colNames ...string) {
	if len(colNames) >= len(df.dataframe.values) {
		df.dataframe.resetWithError(fmt.Errorf("setting column as labels: number of colNames must be less than number of columns (%d >= %d)",
			len(colNames), len(df.dataframe.values)))
		return
	}
	for i := 0; i < len(colNames); i++ {
		index, err := indexOfContainer(colNames[i], df.dataframe.values)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("setting column as labels: %v", err))
			return
		}
		df.dataframe.labels = append(df.dataframe.labels, df.dataframe.values[index])
		df.DropCol(colNames[i])
	}
	return
}

// ResetLabels appends the label level(s) at the supplied index levels as columns and drops the level.
// If no index levels are supplied, all label levels are appended as columns and dropped as levels, and replaced by a default label column.
// Returns a new DataFrame.
func (df *DataFrame) ResetLabels(index ...string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().ResetLabels(index...)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// ResetLabels appends the label level(s) at the supplied index levels as columns and drops the level(s).
// If no index levels are supplied, all label levels are appended as columns and dropped as levels, and replaced by a default label column.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) ResetLabels(labelLevels ...string) error {
	index := make([]int, len(labelLevels))
	if len(labelLevels) == 0 {
		index = makeIntRange(0, df.dataframe.NumLevels())
	}
	for j, name := range labelLevels {
		idx, err := indexOfContainer(name, df.dataframe.labels)
		if err != nil {
			return fmt.Errorf("resetting labels to columns: %v", err)
		}
		index[j] = idx
	}
	for incrementor, i := range index {
		// iteratively subset all label levels except the one to be dropped
		adjustedIndex := i - incrementor
		newVal := df.dataframe.labels[adjustedIndex]
		// If label level name has default indicator, remove default indicator
		newVal.name = removeDefaultNameIndicator(newVal.name)
		df.dataframe.values = append(df.dataframe.values, newVal)
		exclude := excludeFromIndex(df.dataframe.NumLevels(), adjustedIndex)
		df.dataframe.labels, _ = subsetContainers(df.dataframe.labels, exclude)
	}
	if df.dataframe.NumLevels() == 0 {
		defaultLabels := makeDefaultLabels(0, df.dataframe.Len(), true)
		df.dataframe.labels = append(df.dataframe.labels, defaultLabels)
	}
	return nil
}

// SetName sets the name of a DataFrame and returns the entire DataFrame.
func (df *DataFrame) SetName(name string) *DataFrame {
	df.name = name
	return df
}

// Name returns the name of the DataFrame.
func (df *DataFrame) Name() string {
	return df.name
}

// SetLabelNames sets the names of all the label levels in the DataFrame and returns the entire DataFrame.
// If an error is returned, it is written to the DataFrame.
func (df *DataFrame) SetLabelNames(levelNames []string) *DataFrame {
	if len(levelNames) != len(df.labels) {
		return dataFrameWithError(
			fmt.Errorf("setting label names: number of levelNames must match number of levels in DataFrame (%d != %d)", len(levelNames), len(df.labels)))
	}
	for j := range levelNames {
		df.labels[j].name = levelNames[j]
	}
	return df
}

// SetColNames sets the names of all the columns in the DataFrame and returns the entire DataFrame.
// If an error is returned, it is written to the DataFrame.
func (df *DataFrame) SetColNames(colNames []string) *DataFrame {
	if len(colNames) != len(df.values) {
		return dataFrameWithError(
			fmt.Errorf("setting column names: number of colNames must match number of columns in DataFrame (%d != %d)",
				len(colNames), len(df.values)))
	}
	for k := range colNames {
		df.values[k].name = colNames[k]
	}
	return df
}

// ReorderCols reorders the columns to be in the same order as specified by colNames.
// If a column is not specified, it is excluded from the resulting DataFrame.
// Returns a new DataFrame.
func (df *DataFrame) ReorderCols(colNames []string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().ReorderCols(colNames)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// ReorderCols reorders the columns to be in the same order as specified by colNames.
// If a column is not specified, it is excluded from the resulting DataFrame.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) ReorderCols(colNames []string) error {
	newIndex := make([]int, len(colNames))
	for k, name := range colNames {
		index, err := indexOfContainer(name, df.dataframe.values)
		if err != nil {
			return fmt.Errorf("reordering columns: colNames (index %d): %v", k, err)
		}
		newIndex[k] = index
	}
	df.SubsetCols(newIndex)
	return nil
}

// ReorderLabels reorders the label levels to be in the same order as specified by levelNames.
// If a level is not specified, it is excluded from the resulting DataFrame.
// Returns a new DataFrame.
func (df *DataFrame) ReorderLabels(levelNames []string) *DataFrame {
	df = df.Copy()
	err := df.InPlace().ReorderLabels(levelNames)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// ReorderLabels reorders the label levels to be in the same order as specified by levelNames.
// If a level is not specified, it is excluded from the resulting DataFrame.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) ReorderLabels(levelNames []string) error {
	newIndex := make([]int, len(levelNames))
	for j, name := range levelNames {
		index, err := indexOfContainer(name, df.dataframe.labels)
		if err != nil {
			return fmt.Errorf("reordering labels: levelNames (index %d): %v", j, err)
		}
		newIndex[j] = index
	}
	df.SubsetLabels(newIndex)
	return nil
}

// -- RESHAPING

// Transpose transposes rows into columns.
// Row values become column values, column names become labels,
// labels become column names (and multi-level labels become multi-level columns)
// and label level names swap with column level names.
// For example a DataFrame with 2 rows and 1 column has 2 columns and 1 row after transposition.
// Because rows can contain heterogenous types, every column is coerced to []interface{}.
func (df *DataFrame) Transpose() *DataFrame {
	// row values become column values: 2 row x 1 col -> 2 col x 1 row
	vals := make([][]interface{}, df.Len())
	valsIsNull := make([][]bool, df.Len())
	// each new column has the same number of rows as prior columns
	for i := range vals {
		vals[i] = make([]interface{}, df.NumColumns())
		valsIsNull[i] = make([]bool, df.NumColumns())
	}
	// label names become column names: 2 row x 1 level -> 2 col x 1 level
	colNames := make([][]string, df.Len())
	// each new column name has the same number of levels as prior label levels
	for i := range colNames {
		colNames[i] = make([]string, df.NumLevels())
	}
	// column levels become label levels: 2 level x 1 col -> 2 level x 1 row
	labels := make([][]string, df.numColLevels())
	labelsIsNull := make([][]bool, df.numColLevels())

	// column level names become label level names
	labelNames := make([]string, df.numColLevels())
	// label level names become column level names
	colLevelNames := make([]string, df.NumLevels())

	// each new label level has same number of rows as prior columns
	for l := range labels {
		labels[l] = make([]string, df.NumColumns())
		labelsIsNull[l] = make([]bool, df.NumColumns())
	}

	// iterate over labels to write column names and column level names
	for j := range df.labels {
		v := df.labels[j].string().slice
		for i := range v {
			colNames[i][j] = v[i]
		}
		colLevelNames[j] = df.labels[j].name
	}
	// iterate over column levels to write label level names
	for l := range df.colLevelNames {
		labelNames[l] = df.colLevelNames[l]
	}
	// iterate over columns
	for k := range df.values {
		// write label values
		splitColName := splitNameIntoLevels(df.values[k].name)
		for l := range splitColName {
			labels[l][k] = splitColName[l]
			labelsIsNull[l][k] = false
		}
		// write values
		v := reflect.ValueOf(df.values[k].slice)
		for i := 0; i < v.Len(); i++ {
			src := v.Index(i).Interface()
			vals[i][k] = src
			valsIsNull[i][k] = df.values[k].isNull[i]
		}
	}

	retColNames := make([]string, len(vals))
	for k := range colNames {
		retColNames[k] = joinLevelsIntoName(colNames[k])
	}
	slice, _ := readNestedInterfaceByCols(vals)
	// transfer to valueContainers
	retLabels := copyStringsIntoValueContainers(labels, labelsIsNull, labelNames)
	retVals := copyInterfaceIntoValueContainers(slice, valsIsNull, retColNames)

	return &DataFrame{
		values:        retVals,
		labels:        retLabels,
		name:          df.name,
		colLevelNames: colLevelNames,
	}
}

// PromoteToColLevel pivots an existing container (either column or label names) into a new column level.
// If promoting would use either the last column or index level, it returns an error.
// Each unique value in the stacked column is stacked above each existing column.
// Promotion can add new columns and remove label rows with duplicate values.
func (df *DataFrame) PromoteToColLevel(name string) *DataFrame {

	// -- isolate container to promote

	mergedLabelsAndCols := append(df.labels, df.values...)
	index, err := indexOfContainer(name, mergedLabelsAndCols)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("promoting to column level: %v", err))
	}
	// by default, include all original label levels in new labels
	residualLabelIndex := makeIntRange(0, len(df.labels))
	// check whether container refers to label or column
	if index >= len(df.labels) {
		if len(df.values) <= 1 {
			return dataFrameWithError(fmt.Errorf("promoting to column level: cannot stack only column"))
		}
	} else {
		if len(df.labels) <= 1 {
			return dataFrameWithError(fmt.Errorf("promoting to column level: cannot stack only label level"))
		}
		// if a label level is being promoted, exclude it from new labels
		residualLabelIndex = excludeFromIndex(len(df.labels), index)
	}
	// adjust for label/value merging
	// colIndex >= 1 means a column has been selected
	colIndex := index - len(df.labels)
	valsToPromote := mergedLabelsAndCols[index]

	// -- set up helpers and new containers

	// this step isolates the unique values in the promoted column and the rows in the original slice containing those values
	_, rowIndices, uniqueValuesToPromote := reduceContainers([]*valueContainer{valsToPromote})
	// this step consolidates duplicate residual labels and maps each original row index to its new row index
	residualLabels, _ := subsetContainers(df.labels, residualLabelIndex)
	labels, oldToNewRowMapping := reduceContainersForPromote(residualLabels)
	// set new column level names
	retColLevelNames := append([]string{valsToPromote.name}, df.colLevelNames...)
	// new values will have as many columns as unique values in the column-to-be-stacked * existing columns
	// (minus the stacked column * existing columns, if a column is selected and not label level)
	numNewCols := len(uniqueValuesToPromote) * df.NumColumns()
	numNewRows := reflect.ValueOf(labels[0].slice).Len()
	colNames := make([]string, numNewCols)
	// each item in newVals should be a slice representing a new column
	newVals := make([]interface{}, numNewCols)
	newIsNull := makeBoolMatrix(numNewCols, numNewRows)
	for k := range newIsNull {
		for i := range newIsNull[k] {
			// by default, set all nulls to true; these must be explicitly overwritten in the next iterator
			newIsNull[k][i] = true
		}
	}

	// -- iterate over original data and write into new containers

	// iterate over original columns -> unique values of stacked column -> row index of each unique value
	// compare to original value at column and row position
	// write to new row in container corresponding to unique label combo
	for k := 0; k < df.NumColumns(); k++ {
		// skip column if it is derivative of the original - then drop those columns later
		if k == colIndex {
			continue
		}
		originalVals := reflect.ValueOf(df.values[k].slice)
		// m -> incrementor of unique values in the column to be promoted
		for m, uniqueValue := range uniqueValuesToPromote {
			newColumnIndex := k*len(uniqueValuesToPromote) + m
			newHeader := joinLevelsIntoName([]string{uniqueValue, df.values[k].name})
			colNames[newColumnIndex] = newHeader
			// each item in newVals is a slice of the same type as originalVals at that column position
			newVals[newColumnIndex] = reflect.MakeSlice(originalVals.Type(), numNewRows, numNewRows).Interface()

			// write to new column and new row index
			// length of rowIndices matches length of uniqueValues
			for _, i := range rowIndices[m] {
				// only original rows containing the current unique value will be written into new container
				newRowIndex := oldToNewRowMapping[i]
				// retain the original null value
				newIsNull[newColumnIndex][newRowIndex] = df.values[k].isNull[i]
				src := originalVals.Index(i)
				dst := reflect.ValueOf(newVals[newColumnIndex]).Index(newRowIndex)
				dst.Set(src)
			}
		}
	}

	// -- transfer values into final form

	// if a column was selected for promotion, drop all new columns that are a derivative of the original
	if colIndex >= 0 {
		toDropStart := colIndex * len(uniqueValuesToPromote)
		toDropEnd := toDropStart + len(uniqueValuesToPromote)
		newVals = append(newVals[:toDropStart], newVals[toDropEnd:]...)
		colNames = append(colNames[:toDropStart], colNames[toDropEnd:]...)
		newIsNull = append(newIsNull[:toDropStart], newIsNull[toDropEnd:]...)
	}

	retVals := copyInterfaceIntoValueContainers(newVals, newIsNull, colNames)

	return &DataFrame{
		values:        retVals,
		labels:        labels,
		colLevelNames: retColLevelNames,
		name:          df.name,
	}
}

// -- FILTERS

// Filter returns a new DataFrame with only rows that satisfy all of the filters,
// which is a map of container names (either column name or label name) and anonymous functions.
//
// Rows with null values never satsify a filter.
// If no filter is provided, function does nothing.
// For equality filtering on one or more containers, consider FilterByValue.
// Returns a new DataFrame.
func (df *DataFrame) Filter(filters map[string]FilterFn) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Filter(filters)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Filter returns a new DataFrame with only rows that satisfy all of the filters,
// which is a map of container names (either column name or label name) and anonymous functions.
//
// Rows with null values never satsify a filter.
// If no filter is provided, function does nothing.
// For equality filtering on one or more containers, consider FilterByValue.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Filter(filters map[string]FilterFn) error {
	if len(filters) == 0 {
		return nil
	}

	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	index, err := filter(mergedLabelsAndCols, filters)
	if err != nil {
		return fmt.Errorf("filtering rows: %v", err)
	}
	df.Subset(index)
	return nil
}

// FilterIndex returns the index positions of the rows in container that satsify filterFn.
// A filter that matches no rows returns empty []int. An out of range container returns nil.
func (df *DataFrame) FilterIndex(container string, filterFn FilterFn) []int {
	mergedLabelsAndCols := append(df.labels, df.values...)
	index, err := filter(mergedLabelsAndCols, map[string]FilterFn{container: filterFn})
	if err != nil {
		return nil
	}
	// no matches? convert from nil to empty slice
	if len(index) == 0 {
		return []int{}
	}
	return index
}

// Where iterates over the rows in df and evaluates whether each one satisfies filters,
// which is a map of container names (either column or label names) and tada.FilterFn structs.
// If yes, returns ifTrue at that row position.
// If not, returns ifFalse at that row position.
// Values are coerced from their original type to the selected field type for filtering, but after filtering retains their original type.
//
// Returns an unnamed Series with a copy of the labels from the original Series and null status based on the supplied values.
// If an unsupported value type is supplied as either ifTrue or ifFalse, returns an error.
func (df *DataFrame) Where(filters map[string]FilterFn, ifTrue, ifFalse interface{}) (*Series, error) {
	ret := make([]interface{}, df.Len())
	// []int of positions where all filters are true
	mergedLabelsAndCols := append(df.labels, df.values...)
	index, err := filter(mergedLabelsAndCols, filters)
	if err != nil {
		return nil, fmt.Errorf("where: %v", err)
	}
	for _, i := range index {
		ret[i] = ifTrue
	}
	// []int of positions where any filters is not true
	inverseIndex := difference(makeIntRange(0, df.Len()), index)
	for _, i := range inverseIndex {
		ret[i] = ifFalse
	}
	isNull, err := setNullsFromInterface(ret)
	if err != nil {
		err := isSupportedSlice([]interface{}{ifTrue})
		// ifTrue is unsupported?
		if err != nil {
			return nil, fmt.Errorf("where: ifTrue: %v", err)
		}
		// ifFalse is unsupported
		return nil, fmt.Errorf("where: ifFalse: %v", err)
	}
	return &Series{
		values: &valueContainer{
			slice:  ret,
			isNull: isNull,
		},
		labels: copyContainers(df.labels),
	}, nil
}

// FilterByValue returns the rows in the DataFrame satisfying all filters,
// which is a map of of container names (either column or label names) to interface{} values.
// A filter is satisfied for a given row value if the stringified value in that container at that row matches the stringified interface{} value.
// Returns a new DataFrame.
func (df *DataFrame) FilterByValue(filters map[string]interface{}) *DataFrame {
	df = df.Copy()
	err := df.InPlace().FilterByValue(filters)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// FilterByValue returns the rows in the DataFrame satisfying all filters,
// which is a map of of container names (either column or label names) to interface{} values.
// A filter is satisfied for a given row value if the stringified value in that container at that row matches the stringified interface{} value.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) FilterByValue(filters map[string]interface{}) error {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	index, err := filterByValue(mergedLabelsAndCols, filters)
	if err != nil {
		return fmt.Errorf("filter by value: %v", err)
	}
	df.Subset(index)
	return nil
}

// -- APPLY

// Apply applies an anonymous function to every row in a container based on lambdas,
// which is a map of container names (either column or label names) to anonymous functions.
// A row's null status can be set in-place within the anonymous function by accessing the []bool argument.
// Returns a new DataFrame.
func (df *DataFrame) Apply(lambdas map[string]ApplyFn) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Apply(lambdas)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Apply applies an anonymous function to every row in a container based on lambdas,
// which is a map of container names (either column or label names) to anonymous functions.
// A row's null status can be changed in-place within the anonymous function.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Apply(lambdas map[string]ApplyFn) error {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	for containerName, lambda := range lambdas {
		err := lambda.validate()
		if err != nil {
			return fmt.Errorf("applying lambda function: %v", err)
		}
		index, err := indexOfContainer(containerName, mergedLabelsAndCols)
		if err != nil {
			return fmt.Errorf("applying lambda function: %v", err)
		}
		err = mergedLabelsAndCols[index].apply(lambda, nil)
		if err != nil {
			return fmt.Errorf("applying lambda function: %v", err)
		}
	}
	return nil
}

// SetRows applies lambda within container (either label or column name)
// to set the values at the specified row positions.
// The new values must be the same type as the existing values.
// Returns a new DataFrame.
func (df *DataFrame) SetRows(lambda ApplyFn, container string, rows []int) *DataFrame {
	df = df.Copy()
	err := df.InPlace().SetRows(lambda, container, rows)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// SetRows applies lambda within container (either label or column name)
// to set the values at the specified row positions.
// The new values must be the same type as the existing values.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) SetRows(lambda ApplyFn, container string, rows []int) error {
	err := lambda.validate()
	if err != nil {
		return fmt.Errorf("applying lambda to rows: %v", err)
	}
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	index, err := indexOfContainer(container, mergedLabelsAndCols)
	if err != nil {
		return fmt.Errorf("applying lambda to rows: %v", err)
	}
	err = mergedLabelsAndCols[index].apply(lambda, rows)
	if err != nil {
		return fmt.Errorf("applying lambda to rows: %v", err)
	}
	return nil
}

// -- MERGERS

// JoinOptionHow specifies how to join two Series or DataFrames. Supported options:
// left (ie left join), right, inner (default: left).
func JoinOptionHow(how string) func(*joinConfig) {
	return func(l *joinConfig) {
		l.how = how
	}
}

// JoinOptionLeftOn specifies the key(s) to use to join the left Series/DataFrame.
// Keys must be existing container names (either label level or column names).
// Default: no keys are specified, so shared label names are used automatically as keys.
func JoinOptionLeftOn(keys []string) func(*joinConfig) {
	return func(l *joinConfig) {
		l.leftOn = keys
	}
}

// JoinOptionRightOn specifies the key(s) to use to join the right Series/DataFrame.
// Keys must be existing container names (either label level or column names).
// Default: no keys are specified, so shared label names are used automatically as keys.
func JoinOptionRightOn(keys []string) func(*joinConfig) {
	return func(l *joinConfig) {
		l.rightOn = keys
	}
}

// Merge joins other onto df.
// Performs a left join unless a different join type is specified as an option.
// If left and right keys are supplied as options, those are used as lookup keys.
// Otherwise, the join will automatically use shared label names or return an error if none exist.
//
// Merge identifies the row alignment between df and other and appends aligned values as new columns on df.
// Rows are aligned when
// 1) one or more containers (either column or label level) in other share the same name as one or more containers in df,
// and 2) the stringified values in the other containers match the values in the df containers.
// For the following dataframes:
//
// df    	other
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in df is "aligned" with row 0 in other, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// After merging, the result will be:
//
// df
// FOO BAR QUX
// bar 0   null
// baz 1   corge
//
// Finally, all container names (columns and label names) are deduplicated after the merge so that they are unique.
// Returns a new DataFrame.
func (df *DataFrame) Merge(other *DataFrame, options ...JoinOption) (*DataFrame, error) {
	config := setJoinConfig(options)
	if config.how == "inner" {
		// inner merge should be a left merge with null rows dropped
		options = append(options, JoinOptionHow("left"))
	}
	lookupDF, err := df.Lookup(other, options...)
	if err != nil {
		return nil, fmt.Errorf("merging data: %v", err)
	}
	var ret *DataFrame
	if config.how == "right" {
		ret = other.Copy()
	} else {
		ret = df.Copy()
	}
	for k := range lookupDF.values {
		ret.values = append(ret.values, lookupDF.values[k])
	}
	if config.how == "inner" {
		ret.InPlace().DropNull()
	}
	ret.InPlace().DeduplicateNames()
	return ret, nil
}

// Lookup performs the lookup portion of a join of other onto df.
// Performs a left join unless a different join type is specified as an option.
// If left and right keys are supplied as options, those are used as lookup keys.
// Otherwise, the join will automatically use shared label names or return an error if none exist.
//
// Lookup identifies the row alignment between df and other and returns the aligned values.
// Rows are aligned when:
// 1) one or more containers (either column or label level) in other share the same name as one or more containers in df,
// and 2) the stringified values in the other containers match the values in the df containers.
// For the following dataframes:
//
// df    	other
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in df is "aligned" with row 0 in other, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// The result of a lookup will be:
//
// FOO BAR
// bar (null)
// baz corge
//
// Returns a new DataFrame.
func (df *DataFrame) Lookup(other *DataFrame, options ...JoinOption) (*DataFrame, error) {
	config := setJoinConfig(options)
	mergedLabelsAndCols := append(df.labels, df.values...)
	otherMergedLabelsAndCols := append(other.labels, other.values...)
	var leftKeys, rightKeys []int
	var err error
	if len(config.leftOn) == 0 || len(config.rightOn) == 0 {
		if !(len(config.leftOn) == 0 && len(config.rightOn) == 0) {
			return nil, fmt.Errorf("lookup: if either leftOn or rightOn is empty, both must be empty")
		}
	}
	// no join keys specified? find matching labels
	if len(config.leftOn) == 0 {
		leftKeys, rightKeys, err = findMatchingKeysBetweenTwoContainers(df.labels, other.labels)
		if err != nil {
			return nil, fmt.Errorf("lookup: %v", err)
		}
	} else {
		leftKeys, err = indexOfContainers(config.leftOn, mergedLabelsAndCols)
		if err != nil {
			return nil, fmt.Errorf("lookup: leftOn: %v", err)
		}
		rightKeys, err = indexOfContainers(config.rightOn, otherMergedLabelsAndCols)
		if err != nil {
			return nil, fmt.Errorf("lookup: rightOn: %v", err)
		}
	}
	ret, err := lookupDataFrame(
		config.how, df.name, df.colLevelNames,
		df.values, df.labels, leftKeys,
		other.values, other.labels, rightKeys, config.leftOn, config.rightOn)
	if err != nil {
		return nil, fmt.Errorf("lookup: %v", err)
	}
	return ret, nil
}

// -- SORTERS

// Sort sorts the values by zero or more Sorter specifications.
// If no Sorter is supplied, does not sort.
// If no DType is supplied for a Sorter, sorts as float64.
// DType is only used for the process of sorting. Once it has been sorted, data retains its original type.
// Returns a new DataFrame.
func (df *DataFrame) Sort(by ...Sorter) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Sort(by...)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Sort sorts the values by zero or more Sorter specifications.
// If no Sorter is supplied, does not sort.
// If no DType is supplied for a Sorter, sorts as float64.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Sort(by ...Sorter) error {
	if len(by) == 0 {
		return fmt.Errorf("sorting rows: must supply at least one Sorter")
	}

	mergedLabelsAndValues := append(df.dataframe.labels, df.dataframe.values...)
	// sortContainers iteratively updates the index
	newIndex, err := sortContainers(mergedLabelsAndValues, by)
	if err != nil {
		return fmt.Errorf("sorting rows: %v", err)
	}
	// rearrange the data in place with the final index
	df.Subset(newIndex)
	return nil
}

// -- GROUPERS

// GroupBy groups the DataFrame rows that share the same stringified value
// in the container(s) (columns or labels) specified by names.
// If error occurs, writes error to GroupedDataFrame.
func (df *DataFrame) GroupBy(names ...string) *GroupedDataFrame {
	var index []int
	var err error
	mergedLabelsAndCols := append(df.labels, df.values...)
	// if no names supplied, group by all label levels and use all label level names
	if len(names) == 0 {
		index = makeIntRange(0, df.NumLevels())
	} else {
		index, err = indexOfContainers(names, mergedLabelsAndCols)
		if err != nil {
			return groupedDataFrameWithError(fmt.Errorf("group by: %v", err))
		}
	}
	containers, _ := subsetContainers(mergedLabelsAndCols, index)
	newLabels, rowIndices, orderedKeys := reduceContainers(containers)
	return &GroupedDataFrame{
		orderedKeys: orderedKeys,
		rowIndices:  rowIndices,
		labels:      newLabels,
		df:          df,
	}
}

// PivotTable creates a spreadsheet-style pivot table as a DataFrame by
// grouping rows using the unique values in labels,
// reducing the values in values using an aggFunc aggregation function, then
// promoting the unique values in columns to be new columns.
// labels, columns, and values should all refer to existing container names (either columns or labels).
// Supported aggFuncs: sum, mean, median, stdDev, count, min, max.
func (df *DataFrame) PivotTable(labels, columns, values, aggFunc string) (*DataFrame, error) {

	mergedLabelsAndCols := append(df.labels, df.values...)
	_, err := indexOfContainer(labels, mergedLabelsAndCols)
	if err != nil {
		return nil, fmt.Errorf("pivot table: labels: %v", err)
	}
	_, err = indexOfContainer(columns, mergedLabelsAndCols)
	if err != nil {
		return nil, fmt.Errorf("pivot table: columns: %v", err)
	}
	_, err = indexOfContainer(values, mergedLabelsAndCols)
	if err != nil {
		return nil, fmt.Errorf("pivot table: values: %v", err)
	}
	grouper := df.GroupBy(labels, columns)
	var ret *DataFrame
	switch aggFunc {
	case "sum":
		ret = grouper.Sum(values)
	case "mean":
		ret = grouper.Mean(values)
	case "median":
		ret = grouper.Median(values)
	case "stdDev":
		ret = grouper.StdDev(values)
	case "count":
		ret = grouper.Count(values)
	case "min":
		ret = grouper.Min(values)
	case "max":
		ret = grouper.Max(values)
	default:
		return nil, fmt.Errorf("pivot table: aggFunc: unsupported (%v)", aggFunc)
	}
	ret = ret.PromoteToColLevel(columns)
	ret.dropColLevel(1)
	return ret, nil
}

// dropColLevel drops a column level inplace by changing the name in every column container
func (df *DataFrame) dropColLevel(level int) *DataFrame {
	df.colLevelNames = append(df.colLevelNames[:level], df.colLevelNames[level+1:]...)
	for k := range df.values {
		priorNames := splitNameIntoLevels(df.values[k].name)
		newNames := append(priorNames[:level], priorNames[level+1:]...)
		df.values[k].name = joinLevelsIntoName(newNames)
	}
	return df
}

// Resample coerces values to time.Time and truncates them by the logic supplied in how,
// which is a map of of container names (either column or label names) to tada.Resampler structs.
// For each container name in the map, the first By field selected (i.e., not left blank)
// in its Resampler struct provides the resampling logic for that container.
// If slice type is civil.Date or civil.Time before resampling, it will be returned as civil.Date or civil.Time after resampling.
//
// Returns a new DataFrame.
func (df *DataFrame) Resample(how map[string]Resampler) *DataFrame {
	df = df.Copy()
	err := df.InPlace().Resample(how)
	if err != nil {
		return dataFrameWithError(err)
	}
	return df
}

// Resample coerces values to time.Time and truncates them by the logic supplied in how,
// which is a map of of container names (either column or label names) to tada.Resampler structs.
// For each container name in the map, the first By field selected (i.e., not left blank)
// in its Resampler struct provides the resampling logic for that container.
// If slice type is civil.Date or civil.Time before resampling, it will be returned as civil.Date or civil.Time after resampling.
//
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Resample(how map[string]Resampler) error {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	for name, resampler := range how {
		index, err := indexOfContainer(name, mergedLabelsAndCols)
		if err != nil {
			return fmt.Errorf("resample: %v", err)
		}
		mergedLabelsAndCols[index].resample(resampler)
	}
	return nil
}

// -- ITERATORS

// Iterator returns an iterator which may be used to access the values in each row as map[string]Element.
func (df *DataFrame) Iterator() *DataFrameIterator {
	return &DataFrameIterator{
		current: -1,
		df:      df,
	}
}

// Next advances to next row. Returns false at end of iteration.
func (iter *DataFrameIterator) Next() bool {
	iter.current++
	return iter.current < iter.df.Len()
}

// Row returns the current row in the DataFrame as a map.
// The map keys are the names of containers (including label levels).
// The value in each map is an Element containing an interface value and a boolean denoting if the value is null.
// If multiple columns have the same header, only the Element of the left-most column are returned.
func (iter *DataFrameIterator) Row() map[string]Element {
	ret := make(map[string]Element)
	for k := iter.df.NumColumns() - 1; k >= 0; k-- {
		ret[iter.df.values[k].name] = iter.df.values[k].iterRow(iter.current)
	}
	for j := iter.df.NumLevels() - 1; j >= 0; j-- {
		ret[iter.df.labels[j].name] = iter.df.labels[j].iterRow(iter.current)
	}
	return ret
}

// -- COUNT

func (df *DataFrame) count(name string, countFunction func(interface{}, []bool, []int) (int, bool)) *Series {
	retVals := make([]int, len(df.values))
	retNulls := make([]bool, len(df.values))
	labels := make([]string, len(df.values))
	labelNulls := make([]bool, len(df.values))

	for k := range df.values {
		retVals[k], retNulls[k] = countFunction(
			df.values[k].slice,
			df.values[k].isNull,
			makeIntRange(0, df.Len()))

		labels[k] = df.values[k].name
		labelNulls[k] = false
	}
	return &Series{
		values: &valueContainer{slice: retVals, isNull: retNulls, name: name},
		labels: []*valueContainer{{slice: labels, isNull: labelNulls, name: "*0"}},
	}
}

// -- MATH

func (df *DataFrame) math(name string, mathFunction func([]float64, []bool, []int) (float64, bool)) *Series {
	retVals := make([]float64, len(df.values))
	retNulls := make([]bool, len(df.values))
	labels := make([]string, len(df.values))
	labelNulls := make([]bool, len(df.values))

	for k := range df.values {
		retVals[k], retNulls[k] = mathFunction(
			df.values[k].float64().slice,
			df.values[k].isNull,
			makeIntRange(0, df.Len()))

		labels[k] = df.values[k].name
		labelNulls[k] = false
	}
	return &Series{
		values: &valueContainer{slice: retVals, isNull: retNulls, name: name},
		labels: []*valueContainer{{slice: labels, isNull: labelNulls, name: "*0"}},
	}
}

// SumCols finds each column matching a supplied colName, coerces its values to float64, and adds them row-wise.
// The resulting Series is named name.
// If any column has a null value for a given row, that row is considered null.
func (df *DataFrame) SumCols(name string, colNames ...string) (*Series, error) {
	if len(colNames) == 0 {
		return nil, fmt.Errorf("sum columns: colNames cannot be empty")
	}
	var ret *Series
	for i, name := range colNames {
		_, err := indexOfContainer(name, df.values)
		if err != nil {
			return nil, fmt.Errorf("sum columns: %v", err)
		}
		if i == 0 {
			ret = df.Col(name)
		} else {
			ret = ret.Add(df.Col(name), false)
		}
	}
	ret.SetName(name)
	return ret, nil
}

// Sum coerces the values in each column to float64 and sums each column.
func (df *DataFrame) Sum() *Series {
	return df.math("sum", sum)
}

// Mean coerces the values in each column to float64 and calculates the mean of each column.
func (df *DataFrame) Mean() *Series {
	return df.math("mean", mean)
}

// Median coerces the values in each column to float64 and calculates the median of each column.
func (df *DataFrame) Median() *Series {
	return df.math("median", median)
}

// StdDev coerces the values in each column to float64 and calculates the standard deviation of each column.
func (df *DataFrame) StdDev() *Series {
	return df.math("stdDev", std)
}

// Count counts the number of non-null values in each column.
func (df *DataFrame) Count() *Series {
	return df.count("count", count)
}

// NUnique counts the number of unique non-null values in each column.
func (df *DataFrame) NUnique() *Series {
	return df.count("nunique", nunique)
}

// Min coerces the values in each column to float64 and returns the minimum non-null value in each column.
func (df *DataFrame) Min() *Series {
	return df.math("min", min)
}

// Max coerces the values in each column to float64 and returns the maximum non-null value in each column.
func (df *DataFrame) Max() *Series {
	return df.math("max", max)
}

// Reduce uses lambda to reduce all columns to a Series named name
// with column names as labels and reduced values as row values.
// The type of the new Series is a slice with the same type as the first value outputted by the anonymous function.
func (df *DataFrame) Reduce(name string, lambda ReduceFn) (*Series, error) {
	err := lambda.validate()
	if err != nil {
		return nil, fmt.Errorf("reducing DataFrame: %v", err)
	}
	if df.NumColumns() == 0 {
		return nil, fmt.Errorf("reducing DataFrame: DataFrame must have at least one column")
	}
	// must deduce output type from first result
	firstResult, _ := lambda(df.values[0].slice, df.values[0].isNull)
	firstType := reflect.TypeOf(firstResult)
	sampleLabel := splitNameIntoLevels(df.values[0].name)

	retVals := reflect.MakeSlice(reflect.SliceOf(firstType), df.NumColumns(), df.NumColumns())
	retNulls := make([]bool, df.NumColumns())
	stringifiedLevels := make([][]string, len(sampleLabel))
	for j := range stringifiedLevels {
		stringifiedLevels[j] = make([]string, df.NumColumns())
	}
	for i := range df.values {
		val, null := lambda(df.values[i].slice, df.values[i].isNull)
		src := reflect.ValueOf(val)
		if src.Type() != firstType {
			return nil, fmt.Errorf("reducing DataFrame: column %s: type must match type of first reduced value (%s != %s)",
				df.values[i].name, src.Type().String(), firstType.String())
		}
		dst := retVals.Index(i)
		dst.Set(src)
		if null {
			retNulls[i] = null
		}
		levels := splitNameIntoLevels(df.values[i].name)
		for j := range levels {
			stringifiedLevels[j][i] = levels[j]
		}
	}
	retLabels := copyStringsIntoValueContainers(
		stringifiedLevels,
		makeBoolMatrix(len(stringifiedLevels), df.NumColumns()),
		df.colLevelNames,
	)
	return &Series{
		values: &valueContainer{
			slice:  retVals.Interface(),
			isNull: retNulls,
			name:   name,
		},
		labels: retLabels,
	}, nil
}
