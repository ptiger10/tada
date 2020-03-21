package tada

import (
	"bytes"
	"encoding/csv"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"strings"

	"github.com/ptiger10/tablediff"
	"github.com/ptiger10/tablewriter"
)

// -- CONSTRUCTORS

// NewDataFrame creates a new DataFrame with `slices` (akin to column values) and optional `labels`.
// `Slices` must be comprised of supported slices, and each `label` must be a supported slice.
//
// If no `labels` are supplied, a default label level is inserted ([]int incrementing from 0).
// Columns are named sequentially (e.g., 0, 1, etc) by default. Default column names are displayed on printing.
// Label levels are named *n (e.g., *0, *1, etc) by default. Default label names are hidden on printing.
//
// Supported slice types: all variants of []float, []int, & []uint,
// []string, []bool, []time.Time, []interface{},
// and 2-dimensional variants of each (e.g., [][]string, [][]float64).
func NewDataFrame(slices []interface{}, labels ...interface{}) *DataFrame {
	if slices == nil && labels == nil {
		return dataFrameWithError(fmt.Errorf("NewSeries(): `slices` and `labels` cannot both be nil"))
	}
	var values []*valueContainer
	var err error
	if slices != nil {
		// handle values
		values, err = makeValueContainersFromInterfaces(slices, false)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("NewDataFrame(): `slices`: %v", err))
		}
	}
	// handle labels
	retLabels, err := makeValueContainersFromInterfaces(labels, true)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("NewDataFrame(): `labels`: %v", err))
	}
	if len(retLabels) == 0 {
		// handle default labels
		numRows := reflect.ValueOf(slices[0]).Len()
		defaultLabels := makeDefaultLabels(0, numRows, true)
		retLabels = append(retLabels, defaultLabels)
	}
	if slices == nil {
		// default values
		defaultValues := makeDefaultLabels(0, reflect.ValueOf(labels[0]).Len(), false)
		values = append(values, defaultValues)
	}
	return &DataFrame{values: values, labels: retLabels, colLevelNames: []string{"*0"}}
}

// MakeMultiLevelLabels expects `labels` to be a slice of slices.
// It returns a product of these slices by repeating each label value n times,
// where n is the number of unique label values in the other slices.
//
// For example, [["foo", "bar"], [1, 2, 3]]
// returns [["foo", "foo", "foo", "bar", "bar", "bar"], [1, 2, 3, 1, 2, 3]]
func MakeMultiLevelLabels(labels []interface{}) ([]interface{}, error) {
	for k := range labels {
		if !isSlice(labels[k]) {
			return nil, fmt.Errorf("MakeSlicesFromCrossProduct(): position %d: must be slice", k)
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

// ConcatSeries concatenates multiple Series with identical labels into a single DataFrame.
// To join Series with different labels, use s.ToDataFrame() + df.Merge() (for simple cases)
// or df.LookupAdvanced() + df.WithCol() (for advanced cases)
func ConcatSeries(series ...*Series) (*DataFrame, error) {
	ret := &DataFrame{colLevelNames: []string{"*0"}}
	for k, s := range series {
		if k == 0 {
			ret.labels = s.labels
		} else {
			if s.Len() != ret.Len() {
				return nil, fmt.Errorf("ConcatSeries(): position %d: all series must have same number of rows (%v != %v)", k, s.Len(), ret.Len())
			}
			if !reflect.DeepEqual(s.labels, ret.labels) {
				return nil, fmt.Errorf("ConcatSeries(): position %d: all series must have same labels", k)
			}
		}
		ret.values = append(ret.values, s.values)
	}
	return ret, nil
}

// Cast casts the underlying container values (column or label level) to []float64, []string, or []time.Time
// and caches the []byte values of the container (if inexpensive).
// Use cast to improve performance when calling multiple operations on values.
func (df *DataFrame) Cast(containerAsType map[string]DType) {
	mergedLabelsAndCols := append(df.labels, df.values...)
	for name, dtype := range containerAsType {
		index, err := indexOfContainer(name, mergedLabelsAndCols)
		if err != nil {
			df.resetWithError(fmt.Errorf("Cast(): %v", err))
			return
		}
		mergedLabelsAndCols[index].cast(dtype)
	}
	return
}

// -- READERS

// ReadOptionHeaders configures a read function to expect `n` rows of column headers (default: 1).
func ReadOptionHeaders(n int) func(*readConfig) {
	return func(r *readConfig) {
		r.NumHeaderRows = n
	}
}

// ReadOptionLabels configures a read function to expect the first `n` columns to be label levels (default: 0).
func ReadOptionLabels(n int) func(*readConfig) {
	return func(r *readConfig) {
		r.NumLabelLevels = n
	}
}

// ReadOptionDelimiter configures a read function to use `sep` as a field delimiter for use in ImportCSV or ReadCSVFromString (default: ",").
func ReadOptionDelimiter(sep rune) func(*readConfig) {
	return func(r *readConfig) {
		r.Delimiter = sep
	}
}

// ReadOptionSwitchDims configures a read function to expect columns to be the major dimension of csv data
// (default: expects rows to be the major dimension).
// For example, when reading this data:
//
// [["foo", "bar"], ["baz", "qux"]]
//
// `default`				   		`ReadOptionSwitchDims()`
// (major dimension: rows)			(major dimension: columns)
//	foo bar							foo baz
//  baz qux							bar qux
func ReadOptionSwitchDims() func(*readConfig) {
	return func(r *readConfig) {
		r.MajorDimIsCols = true
	}
}

// ReadCSV reads `data` into a DataFrame (configured by `options`).
// Often used with (encoding/csv) csv.NewReader().ReadAll()
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionSwitchDims.
//
// Default if no options are supplied:
// 1 header row, no labels, rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing.
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ReadCSV(data [][]string, options ...func(*readConfig)) (ret *DataFrame, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("ReadCSV(): `data` must have at least one row")
	}
	if len(data[0]) == 0 {
		return nil, fmt.Errorf("ReadCSV(): `data` must have at least one column")
	}
	config := setReadConfig(options)

	if config.MajorDimIsCols {
		ret, err = readCSVByCols(data, config)
	} else {
		ret, err = readCSVByRows(data, config)
	}
	if err != nil {
		return nil, fmt.Errorf("ReadCSV(): %v", err)
	}
	return ret, nil
}

// ReadCSVFromString reads a stringified csv table into a DataFrame (configured by `options`).
// This function is most commonly used in conjuction with calling WriteMockCSV(), saving the result as a string, and then reading a DataFrame from that string within a test.
// The major dimension of the table should be rows.
// For advanced cases, use the standard csv library NewReader().ReadAll() + tada.ReadCSV().
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionDelimiter.
//
// Default if no options are supplied:
// 1 header row, no labels, field delimiter is ","
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ReadCSVFromString(data string, options ...func(*readConfig)) (*DataFrame, error) {
	if data == "" {
		return nil, fmt.Errorf("ReadCSVFromString(): `data` cannot be empty")
	}
	config := setReadConfig(options)

	reader := strings.NewReader(data)
	r := csv.NewReader(reader)
	r.Comma = config.Delimiter

	r.TrimLeadingSpace = true
	records, err := r.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("ReadCSVFromString(): %v", err)
	}
	// should never return error because already checked for misalignment and empty string
	return readCSVByRows(records, config)
}

// ImportCSV reads the file at `path` into a Dataframe (configured by `options`).
// For advanced cases, use the standard csv library NewReader().ReadAll() + tada.ReadCSV().
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionDelimiter, ReadOptionSwitchDims.
//
// Default if no options are supplied:
// 1 header row, no labels, field delimiter is ",", rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ImportCSV(path string, options ...func(*readConfig)) (*DataFrame, error) {
	config := setReadConfig(options)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ImportCSV(): %s", err)
	}
	numRows, numCols, err := extractCSVDimensions(data, config.Delimiter)
	if numRows == 0 {
		return nil, fmt.Errorf("ImportCSV(): must have at least one row")
	}
	retVals := makeStringMatrix(numCols, numRows)
	retNulls := makeBoolMatrix(numCols, numRows)
	r := bytes.NewReader(data)
	err = readCSVBytes(r, retVals, retNulls, config.Delimiter)
	if err != nil {
		return nil, fmt.Errorf("ImportCSV(): %s", err)
	}
	return makeDataFrameFromMatrices(retVals, retNulls, config), nil
}

// ReadInterface converts `data` to [][]string and reads into  a Dataframe (configured by `options`).
// Google Sheets, for example, exports data as [][]interface{} with either rows or columns as the major dimension.
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionSwitchDims.
//
// Default if no options are supplied:
// 1 header row, no labels, rows as major dimension
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// If no headers are supplied, a default level of sequential column names (e.g., 0, 1, etc) is used. Default column names are displayed on printing
// Label levels are named *i (e.g., *0, *1, etc) by default when first created. Default label names are hidden on printing.
func ReadInterface(data [][]interface{}, options ...func(*readConfig)) (ret *DataFrame, err error) {
	if len(data) == 0 {
		return nil, fmt.Errorf("ReadInterface(): `data` must have at least one row")
	}
	if len(data[0]) == 0 {
		return nil, fmt.Errorf("ReadInterface(): `data` must have at least one column")
	}
	numLines := len(data[0])
	for i := range data {
		if len(data[i]) != numLines {
			return nil, fmt.Errorf("ReadInterface(): `data`: slice %d: all slices must have same length as first slice (%d != %d)",
				i, len(data[i]), numLines)
		}
	}

	config := setReadConfig(options)
	// convert [][]interface to [][]string
	str := make([][]string, len(data))
	for j := range str {
		str[j] = make([]string, len(data[0]))
	}
	for i := range data {
		for j := range data[i] {
			str[i][j] = fmt.Sprint(data[i][j])
		}
	}

	// ducks error because already checked for misalignment and empty data
	if config.MajorDimIsCols {
		ret, _ = readCSVByCols(str, config)
	} else {
		ret, _ = readCSVByRows(str, config)
	}
	return ret, nil
}

// ReadMatrix reads data satisfying the gonum Matrix interface into a DataFrame.
// Panics if any slices in the matrix are shorter than the first slice.
func ReadMatrix(mat Matrix) *DataFrame {

	numRows, numCols := mat.Dims()
	// major dimension: columns
	data := make([][]string, numCols)
	for k := range data {
		data[k] = make([]string, numRows)
		for i := 0; i < numRows; i++ {
			data[k][i] = fmt.Sprint(mat.At(i, k))
		}
	}
	// ducks error because expects all slices to be the same length
	ret, _ := readCSVByCols(data, &readConfig{})
	return ret
}

// ReadStruct reads a `slice` of structs into a DataFrame with field names converted to column names,
// field values converted to column values, and default labels. The structs must all be of the same type.
//
// A default label level named *0 is inserted ([]int incrementing from 0). Default label names are hidden on printing.
func ReadStruct(slice interface{}) (*DataFrame, error) {
	values, err := readStruct(slice)
	if err != nil {
		return nil, fmt.Errorf("ReadStruct(): %v", err)
	}
	defaultLabels := makeDefaultLabels(0, reflect.ValueOf(slice).Len(), true)
	return &DataFrame{
		values:        values,
		labels:        []*valueContainer{defaultLabels},
		colLevelNames: []string{"*0"},
	}, nil
}

// ToSeries converts a single-columned DataFrame to a Series that shares the same underlying values and labels.
func (df *DataFrame) ToSeries() *Series {
	if len(df.values) != 1 {
		return seriesWithError(fmt.Errorf("ToSeries(): DataFrame must have a single column"))
	}
	return &Series{
		values:     df.values[0],
		labels:     df.labels,
		sharedData: true,
	}
}

// ToCSV writes a DataFrame to a [][]string with rows as the major dimension.
// Null values are replaced with "n/a".
// If `includeLabels` is true, then the DataFrame's labels are written.
func (df *DataFrame) ToCSV(includeLabels bool) [][]string {
	transposedStringValues, err := df.toCSVByRows(includeLabels)
	if err != nil {
		return nil
	}
	mergedLabelsAndCols := append(df.labels, df.values...)
	// overwrite null values, skipping headers
	for i := range transposedStringValues[df.numColLevels():] {
		for k := range transposedStringValues[i] {
			if mergedLabelsAndCols[k].isNull[i] {
				transposedStringValues[i+df.numColLevels()][k] = "n/a"
			}
		}
	}
	return transposedStringValues
}

// ExportCSV converts a DataFrame to a [][]string with rows as the major dimension,
// and writes the output to a csv file.
// Null values are replaced with "n/a".
// If `includeLabels` is true, then the DataFrame's labels are written.
func (df *DataFrame) ExportCSV(file string, includeLabels bool) error {
	ret := df.ToCSV(includeLabels)
	if len(ret) == 0 {
		return fmt.Errorf("ExportCSV(): `df` cannot be empty")
	}
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	// duck error because csv is controlled
	w.WriteAll(ret)
	ioutil.WriteFile(file, b.Bytes(), 0666)
	return nil
}

// ToInterface exports a DataFrame to a [][]interface with rows as the major dimension.
// Null values are not changed, and should be handled explicitly with DropNull() or FillNull().
// If `includeLabels` is true, then the DataFrame's labels are written.
func (df *DataFrame) ToInterface(includeLabels bool) [][]interface{} {
	transposedStringValues, err := df.toCSVByRows(includeLabels)
	if err != nil {
		return nil
	}
	ret := make([][]interface{}, len(transposedStringValues))
	for k := range ret {
		ret[k] = make([]interface{}, len(transposedStringValues[0]))
	}
	for i := range transposedStringValues {
		for k := range transposedStringValues[i] {
			ret[i][k] = transposedStringValues[i][k]
		}
	}
	return ret
}

// EqualsCSV converts `df` to csv, compares it to `data`,
// and evaluates whether the stringified values match.
// If `includeLabels` is true, then the DataFrame's labels are included as columns.
// If they do not match, returns a tablediff.Differences object that can be printed to isolate their differences.
func (df *DataFrame) EqualsCSV(data [][]string, includeLabels bool) (bool, *tablediff.Differences, error) {
	numLines := len(data[0])
	for i := range data {
		if len(data[i]) != numLines {
			return false, nil, fmt.Errorf("EqualsCSV(): `data`: slice %d: all slices must have same length as first slice (%d != %d)",
				i, len(data[i]), numLines)
		}
	}
	compare := df.ToCSV(includeLabels)
	diffs, eq := tablediff.Diff(compare, data)
	return eq, diffs, nil
}

// EqualsCSVFromString converts `df` to csv, compares it to the csv read from `data`,
// and evaluates whether the two match.
// If `includeLabels` is true, then the DataFrame's labels are included as columns.
// If they do not match, returns a tablediff.Differences object that can be printed to isolate their differences.
func (df *DataFrame) EqualsCSVFromString(data string, includeLabels bool) (bool, *tablediff.Differences, error) {
	compare := df.ToCSV(includeLabels)

	reader := strings.NewReader(data)
	r := csv.NewReader(reader)
	r.TrimLeadingSpace = true
	r.LazyQuotes = true
	records, err := r.ReadAll()
	if err != nil {
		return false, nil, fmt.Errorf("EqualsCSVFromString()(): %v", err)
	}
	diffs, eq := tablediff.Diff(compare, records)
	return eq, diffs, nil
}

// WriteMockCSV reads `src` (configured by `options`) and writes `n` mock rows to `w`,
// with column names and types inferred based on the data in `src`.
// Regardless of the major dimension of `src`, the major dimension of the output is rows.
// Available options: ReadOptionHeaders, ReadOptionLabels, ReadOptionSwitchDims.
//
// Default if no options are supplied:
// 1 header row, no labels, rows as major dimension
func WriteMockCSV(src [][]string, w io.Writer, n int, options ...func(*readConfig)) error {
	config := setReadConfig(options)
	numSampleRows := 10
	inferredTypes := make([]map[string]int, 0)
	dtypes := []string{"float", "int", "string", "datetime", "time", "bool"}
	var headers [][]string
	var rowCount, colCount int
	// validate input
	if len(src) == 0 {
		return fmt.Errorf("WriteMockCSV(): `src` cannot be empty")
	}
	if !config.MajorDimIsCols {
		rowCount = len(src)
		colCount = len(src[0])
	} else {
		colCount = len(src)
		rowCount = len(src[0])
	}
	if colCount == 0 {
		return fmt.Errorf("WriteMockCSV(): `src` must have at least one column")
	}
	// numSampleRows must not exceed total number of non-header rows in `src`
	maxRows := rowCount - config.NumHeaderRows
	if maxRows < numSampleRows {
		numSampleRows = maxRows
	}

	// major dimension is rows?
	if !config.MajorDimIsCols {
		// copy headers
		for i := 0; i < config.NumHeaderRows; i++ {
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
		dataSample := src[config.NumHeaderRows : numSampleRows+config.NumHeaderRows]
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
		for l := 0; l < config.NumHeaderRows; l++ {
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
			dataSample := src[k][config.NumHeaderRows : numSampleRows+config.NumHeaderRows]
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
		data = df.ToCSV(true)
	} else {
		// truncate rows
		n := optionMaxRows / 2
		topHalf := df.Head(n).ToCSV(true)
		bottomHalf := df.Tail(n).ToCSV(true)[df.numColLevels():]
		filler := make([]string, df.numLevels()+df.numColumns())
		for k := range filler {
			filler[k] = "..."
		}
		data = append(
			append(topHalf, filler),
			bottomHalf...)
	}
	// do not print *0-type label names
	for j := 0; j < df.numLevels(); j++ {
		data[0][j] = suppressDefaultName(data[0][j])
	}

	// truncate columns
	if df.numColumns() >= optionMaxColumns {
		n := (optionMaxColumns / 2)

		for i := range data {
			labels := data[i][:df.numLevels()]
			leftHalf := data[i][df.numLevels() : n+df.numLevels()]
			filler := "..."
			rightHalf := data[i][df.numLevels()+df.numColumns()-n:]
			data[i] = append(
				append(
					labels,
					append(leftHalf, filler)...),
				rightHalf...)
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
	table.SetLabelLevelCount(df.numLevels())

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

// At returns the Element at the `row` and `column` index positions.
// If `row` or `column` is out of range, returns an empty Element.
func (df *DataFrame) At(row, column int) Element {
	if row >= df.Len() {
		return Element{}
	}
	if column >= df.numColumns() {
		return Element{}
	}
	v := reflect.ValueOf(df.values[column].slice)
	return Element{
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

// numLevels returns the number of label columns in the DataFrame.
func (df *DataFrame) numLevels() int {
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
		return nil, fmt.Errorf("`level` out of range: %d >= %d", level, numLevels)
	}
	for k := range columns {
		levels := splitNameIntoLevels(columns[k].name)
		ret[k] = levels[level]
	}
	return ret, nil
}

// ListColNames returns the name of all the columns in the DataFrame, in order.
// If `df` has multiple column levels, each column name is a single string with level values separated by "|" (may be changed with SetOptionDefaultSeparator).
// To return the names at a specific level, use ListColNamesAtLevel().
func (df *DataFrame) ListColNames() []string {
	return listNames(df.values)
}

// ListColNamesAtLevel returns the name of all the columns in the DataFrame, in order, at the supplied column `level`.
// If `level` is out of range, returns a nil slice.
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

// HasCols returns an error if the DataFrame does not contain all of the `colNames` supplied.
func (df *DataFrame) HasCols(colNames ...string) error {
	for _, name := range colNames {
		_, err := indexOfContainer(name, df.values)
		if err != nil {
			return fmt.Errorf("HasCols(): %v", err)
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
	df.InPlace().Subset(index)
	return df
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Subset(index []int) {
	for k := range df.dataframe.values {
		err := df.dataframe.values[k].subsetRows(index)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("Subset(): %v", err))
			return
		}
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j].subsetRows(index)
	}
	return
}

// SwapLabels swaps the label levels with names `i` and `j`.
// Returns a new DataFrame.
func (df *DataFrame) SwapLabels(i, j string) *DataFrame {
	df = df.Copy()
	df.InPlace().SwapLabels(i, j)
	return df
}

// SwapLabels swaps the label levels with names `i` and `j`.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SwapLabels(i, j string) {
	index1, err := indexOfContainer(i, df.dataframe.labels)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("SwapLabels(): `i`: %v", err))
		return
	}
	index2, err := indexOfContainer(j, df.dataframe.labels)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("SwapLabels(): `j`: %v", err))
		return
	}
	df.dataframe.labels[index1], df.dataframe.labels[index2] = df.dataframe.labels[index2], df.dataframe.labels[index1]
	return
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Returns a new DataFrame.
func (df *DataFrame) SubsetLabels(index []int) *DataFrame {
	df = df.Copy()
	df.InPlace().SubsetLabels(index)
	return df
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SubsetLabels(index []int) {
	labels, err := subsetContainers(df.dataframe.labels, index)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("SubsetLabels(): %v", err))
		return
	}
	df.dataframe.labels = labels
	return
}

// SubsetCols returns only the labels specified at the index positions, in the order specified.
// Returns a new DataFrame.
func (df *DataFrame) SubsetCols(index []int) *DataFrame {
	df = df.Copy()
	df.InPlace().SubsetCols(index)
	return df
}

// SubsetCols returns only the labels specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SubsetCols(index []int) {
	cols, err := subsetContainers(df.dataframe.values, index)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("SubsetCols(): %v", err))
		return
	}
	df.dataframe.values = cols
	return
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

// NameOfLabel returns the name of the label level at index position `n`.
// If n is out of range, returns "-out of range-"
func (df *DataFrame) NameOfLabel(n int) string {
	return nameOfContainer(df.labels, n)
}

// NameOfCol returns the name of the column at index position `n`.
// If n is out of range, returns "-out of range-"
func (df *DataFrame) NameOfCol(n int) string {
	return nameOfContainer(df.values, n)
}

// IndexOfContainer returns the index position of the first container with a name matching `name` (case-sensitive).
// If `name` does not match any container, -1 is returned.
// If `columns` is true, only column names will be searched.
// If `columns` is false, only label level names will be searched.
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

// IndexOfRows returns the index positions of the rows with stringified value matching `value`
// in the container named `name`.
// To search Series values, supply either the Series name or an empty string ("") as `name`.
func (df *DataFrame) IndexOfRows(name string, value interface{}) []int {
	mergedLabelsAndColumns := append(df.labels, df.values...)
	i, err := indexOfContainer(name, mergedLabelsAndColumns)
	if err != nil {
		errorWarning(fmt.Errorf("IndexOfRows(): %v", err))
		return nil
	}
	return mergedLabelsAndColumns[i].indexOfRows(value)
}

// SliceLabels returns label levels as interface{} slices within an []interface
// that may be supplied as optional `labels` argument to NewSeries() or NewDataFrame().
// NB: If supplying this output to either of these constructors,
// be sure to use the spread operator (...), or else the labels will not be read as separate levels.
func (df *DataFrame) SliceLabels() []interface{} {
	var ret []interface{}
	labels := copyContainers(df.labels)
	for j := range labels {
		ret = append(ret, labels[j].slice)
	}
	return ret
}

// SelectLabels finds the first label level with matching `name`
// and returns the values as a Series.
// The labels in the Series are shared with the labels in the DataFrame.
// If label level name is default (prefixed with *), the prefix is removed.
func (df *DataFrame) SelectLabels(name string) *Series {
	index, err := indexOfContainer(name, df.labels)
	if err != nil {
		return seriesWithError(fmt.Errorf("SelectLabels(): %v", err))
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

// Col finds the first column with matching `name` and returns as a Series.
// Similar to SelectLabels(), but selects column values instead of label values.
func (df *DataFrame) Col(name string) *Series {
	index, err := indexOfContainer(name, df.values)
	if err != nil {
		return seriesWithError(fmt.Errorf("Col(): %v", err))
	}
	return &Series{
		values:     df.values[index],
		labels:     df.labels,
		sharedData: true,
	}
}

// Cols returns all columns with matching `names`.
func (df *DataFrame) Cols(names ...string) *DataFrame {
	vals := make([]*valueContainer, len(names))
	for i, name := range names {
		index, err := indexOfContainer(name, df.values)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("Cols(): %v", err))
		}
		vals[i] = df.values[index]
	}
	return &DataFrame{
		values: vals,
		labels: df.labels,
		name:   df.name,
	}
}

// Head returns the first `n` rows of the DataFrame.
// If `n` is greater than the length of the DataFrame, returns the entire DataFrame.
// In either case, returns a new DataFrame.
func (df *DataFrame) Head(n int) *DataFrame {
	if df.Len() < n {
		n = df.Len()
	}
	retVals := make([]*valueContainer, len(df.values))
	for k := range df.values {
		retVals[k] = df.values[k].head(n)
	}
	retLabels := make([]*valueContainer, df.numLevels())
	for j := range df.labels {
		retLabels[j] = df.labels[j].head(n)
	}
	return &DataFrame{values: retVals, labels: retLabels, name: df.name, colLevelNames: df.colLevelNames}
}

// Tail returns the last `n` rows of the DataFrame.
// If `n` is greater than the length of the DataFrame, returns the entire DataFrame.
// In either case, returns a new DataFrame.
func (df *DataFrame) Tail(n int) *DataFrame {
	if df.Len() < n {
		n = df.Len()
	}
	retVals := make([]*valueContainer, len(df.values))
	for k := range df.values {
		retVals[k] = df.values[k].tail(n)
	}
	retLabels := make([]*valueContainer, df.numLevels())
	for j := range df.labels {
		retLabels[j] = df.labels[j].tail(n)
	}
	return &DataFrame{values: retVals, labels: retLabels, name: df.name, colLevelNames: df.colLevelNames}
}

// Range returns the rows of the DataFrame starting at `first` and `ending` immediately prior to last (left-inclusive, right-exclusive).
// If either `first` or `last` is greater than the length of the DataFrame, a DataFrame error is returned.
// In all cases, returns a new DataFrame.
func (df *DataFrame) Range(first, last int) *DataFrame {
	if first > last {
		return dataFrameWithError(fmt.Errorf("Range(): first is greater than last (%d > %d)", first, last))
	}
	if first >= df.Len() {
		return dataFrameWithError(fmt.Errorf("Range(): first index out of range (%d > %d)", first, df.Len()-1))
	} else if last > df.Len() {
		return dataFrameWithError(fmt.Errorf("Range(): last index out of range (%d > %d)", last, df.Len()))
	}
	retVals := make([]*valueContainer, len(df.values))
	for k := range df.values {
		retVals[k] = df.values[k].rangeSlice(first, last)
	}
	retLabels := make([]*valueContainer, df.numLevels())
	for j := range df.labels {
		retLabels[j] = df.labels[j].rangeSlice(first, last)
	}
	return &DataFrame{values: retVals, labels: retLabels, name: df.name, colLevelNames: df.colLevelNames}
}

// FillNull fills null values and makes them non-null based on `how`,
// a map of container names (either column or label names) and tada.NullFiller structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its NullFiller struct is the strategy used to replace null values in that container.
// `FillForward` fills null values with the most recent non-null value in the container.
// `FillBackward` fills null values with the next non-null value in the container.
// `FillZero` fills null values with the zero value for that container type.
// `FillFloat` converts the container values to float64 and fills null values with the value supplied.
// If no field is selected, the container values are converted to float64 and all null values are filled with 0.
// Returns a new DataFrame.
func (df *DataFrame) FillNull(how map[string]NullFiller) *DataFrame {
	df = df.Copy()
	df.InPlace().FillNull(how)
	return df
}

// FillNull fills null values and makes them non-null based on `how`.
// How is a map of container names (either column or label names) and `NullFillers`.
// For each container name supplied, the first field selected (i.e., not left blank)
// in the `NullFiller` is the strategy used to replace null values.
// `FillForward` fills null values with the most recent non-null value in the container.
// `FillBackward` fills null values with the next non-null value in the container.
// `FillZero` fills null values with the zero value for that container type.
// `FillFloat` converts the container values to float64 and fills null values with the value supplied.
// If no field is selected, the container values are converted to float64 and all null values are filled with 0.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) FillNull(how map[string]NullFiller) {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	for name, filler := range how {
		index, err := indexOfContainer(name, mergedLabelsAndCols)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("FillNull(): %v", err))
			return
		}
		mergedLabelsAndCols[index].fillnull(filler)
	}
	return
}

// DropNull removes rows with a null value in any column.
// If `subset` is supplied, removes any rows with null values in any of the specified columns.
// Returns a new DataFrame.
func (df *DataFrame) DropNull(subset ...string) *DataFrame {
	df = df.Copy()
	df.InPlace().DropNull(subset...)
	return df
}

// DropNull removes rows with a null value in any column.
// If `subset` is supplied, removes any rows with null values in any of the specified columns.
// Modifies the underlying DataFrame.
func (df *DataFrameMutator) DropNull(subset ...string) {
	var index []int
	if len(subset) == 0 {
		index = makeIntRange(0, len(df.dataframe.values))
	} else {
		for _, name := range subset {
			i, err := indexOfContainer(name, df.dataframe.values)
			if err != nil {
				df.dataframe.resetWithError(fmt.Errorf("DropNull(): %v", err))
				return
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
}

// Null returns all the rows with any null values.
// If `subset` is supplied, returns all the rows with all non-null values in the specified columns.
// Returns a new DataFrame.
func (df *DataFrame) Null(subset ...string) *DataFrame {
	var index []int
	if len(subset) == 0 {
		index = makeIntRange(0, len(df.values))
	} else {
		for _, name := range subset {
			i, err := indexOfContainer(name, df.values)
			if err != nil {
				return dataFrameWithError(fmt.Errorf("Null(): %v", err))
			}
			index = append(index, i)
		}
	}

	subIndexes := make([][]int, len(index))
	for k := range index {
		subIndexes[k] = df.values[k].null()
	}
	anyNull := union(subIndexes)
	return df.Subset(anyNull)
}

// FilterCols returns the columns with names that satisfy `lambda` at the supplied column `level`.
// `level` should be 0 unless df has multiple column levels.
func (df *DataFrame) FilterCols(lambda func(string) bool, level int) *DataFrame {
	df = df.Copy()
	df.InPlace().FilterCols(lambda, level)
	return df
}

// FilterCols returns the columns with names that satisfy `lambda` at the supplied column `level`.
// `level` should be 0 unless df has multiple column levels.
func (df *DataFrameMutator) FilterCols(lambda func(string) bool, level int) {
	var subset []int
	names, err := listNamesAtLevel(df.dataframe.values, level, df.dataframe.numColLevels())
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("FilterCols(): %v", err))
		return
	}
	for k := range names {
		if lambda(names[k]) {
			subset = append(subset, k)
		}
	}
	df.SubsetCols(subset)
	return
}

// -- SETTERS

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a label level exists that matches `name`: rename the level to match `input`.
// In this case, `name` must already exist.
//
// If a slice is supplied as `input` and a label level exists that matches `name`: replace the values at this level to match `input`.
// If a slice is supplied as `input` and a label level does not exist that matches `name`: append a new level named `name` and values matching `input`.
// If `input` is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, returns a new DataFrame.
func (df *DataFrame) WithLabels(name string, input interface{}) *DataFrame {
	df.Copy()
	df.InPlace().WithLabels(name, input)
	return df
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a label level exists that matches `name`: rename the level to match `input`.
// In this case, `name` must already exist.
//
// If a slice is supplied as `input` and a label level exists that matches `name`: replace the values at this level to match `input`.
// If a slice is supplied as `input` and a label level does not exist that matches `name`: append a new level named `name` and values matching `input`.
// If `input` is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, modifies the underlying DataFrame in place.
func (df *DataFrameMutator) WithLabels(name string, input interface{}) {
	labels, err := withColumn(df.dataframe.labels, name, input, df.dataframe.Len())
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("WithLabels(): %v", err))
	}
	df.dataframe.labels = labels
}

// WithCol resolves as follows:
//
// If a scalar string is supplied as `input` and a column exists that matches `name`: rename the column to match `input`.
// In this case, `name` must already exist.
//
// If a slice is supplied as `input` and a column exists that matches `name`: replace the values at this column to match `input`.
// If a slice is supplied as `input` and a column does not exist that matches `name`: append a new column named `name` and values matching `input`.
// If `input` is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, returns a new DataFrame.
func (df *DataFrame) WithCol(name string, input interface{}) *DataFrame {
	df.Copy()
	df.InPlace().WithCol(name, input)
	return df
}

// WithCol resolves as follows:
//
// If a scalar string is supplied as `input` and a column exists that matches `name`: rename the column to match `input`.
// In this case, `name` must already exist.
//
// If a slice is supplied as `input` and a column exists that matches `name`: replace the values at this column to match `input`.
// If a slice is supplied as `input` and a column does not exist that matches `name`: append a new column named `name` and values matching `input`.
// If `input` is a slice, it must be the same length as the underlying DataFrame.
//
// In all cases, modifies the underlying DataFrame in place.
func (df *DataFrameMutator) WithCol(name string, input interface{}) {
	cols, err := withColumn(df.dataframe.values, name, input, df.dataframe.Len())
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("WithCol(): %v", err))
	}
	df.dataframe.values = cols
}

// DropLabels drops the first label level matching `name`.
// Returns a new DataFrame.
func (df *DataFrame) DropLabels(name string) *DataFrame {
	df.Copy()
	df.InPlace().DropLabels(name)
	return df
}

// DropLabels drops the first label level matching `name`.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropLabels(name string) {
	newCols, err := dropFromContainers(name, df.dataframe.labels)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("DropLabels(): %v", err))
		return
	}
	df.dataframe.labels = newCols
	return
}

// DropCol drops the first column matching `name`.
// Returns a new DataFrame.
func (df *DataFrame) DropCol(name string) *DataFrame {
	df.Copy()
	df.InPlace().DropCol(name)
	return df
}

// DropCol drops the first column matching `name`.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropCol(name string) {
	newCols, err := dropFromContainers(name, df.dataframe.values)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("DropCol(): %v", err))
		return
	}
	df.dataframe.values = newCols
	return
}

// DropRow removes the row at the specified index.
// Returns a new DataFrame.
func (df *DataFrame) DropRow(index int) *DataFrame {
	df.Copy()
	df.InPlace().DropRow(index)
	return df
}

// DropRow removes the row at the specified index.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropRow(index int) {
	for k := range df.dataframe.values {
		err := df.dataframe.values[k].dropRow(index)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("DropRow(): %v", err))
			return
		}
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j].dropRow(index)
	}
	return
}

// Append adds the `other` labels and values as new rows to the DataFrame.
// If the types of any container do not match, all the values in that container are coerced to string.
// Returns a new DataFrame.
func (df *DataFrame) Append(other *DataFrame) *DataFrame {
	df.Copy()
	df.InPlace().Append(other)
	return df
}

// Append adds the `other` labels and values as new rows to the DataFrame.
// If the types of any container do not match, all the values in that container are coerced to string.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Append(other *DataFrame) {
	if len(other.labels) != len(df.dataframe.labels) {
		df.dataframe.resetWithError(
			fmt.Errorf("Append(): other DataFrame must have same number of label levels as original DataFrame (%d != %d)",
				len(other.labels), len(df.dataframe.labels)))
		return
	}
	if len(other.values) != len(df.dataframe.values) {
		df.dataframe.resetWithError(
			fmt.Errorf("Append(): other DataFrame must have same number of columns as original DataFrame (%d != %d)",
				len(other.values), len(df.dataframe.values)))
		return
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j] = df.dataframe.labels[j].append(other.labels[j])
	}
	for k := range df.dataframe.values {
		df.dataframe.values[k] = df.dataframe.values[k].append(other.values[k])
	}
	return
}

// Relabel resets the DataFrame labels to default labels (e.g., []int from 0 to df.Len()-1, with *0 as name).
// Returns a new Series.
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

// SetLabels appends the column(s) supplied as `colNames` as label levels and drops the column(s).
// The number of `colNames` supplied must be less than the number of columns in the Series.
// Returns a new DataFrame.
func (df *DataFrame) SetLabels(colNames ...string) *DataFrame {
	df.Copy()
	df.InPlace().SetLabels(colNames...)
	return df
}

// SetLabels appends the column(s) supplied as `colNames` as label levels and drops the column(s).
// The number of `colNames` supplied must be less than the number of columns in the Series.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) SetLabels(colNames ...string) {
	if len(colNames) >= len(df.dataframe.values) {
		df.dataframe.resetWithError(fmt.Errorf("SetLabels(): number of colNames must be less than number of columns (%d >= %d)",
			len(colNames), len(df.dataframe.values)))
		return
	}
	for i := 0; i < len(colNames); i++ {
		index, err := indexOfContainer(colNames[i], df.dataframe.values)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("SetLabels(): %v", err))
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
func (df *DataFrame) ResetLabels(index ...int) *DataFrame {
	df.Copy()
	df.InPlace().ResetLabels(index...)
	return df
}

// ResetLabels appends the label level(s) at the supplied index levels as columns and drops the level.
// If no index levels are supplied, all label levels are appended as columns and dropped as levels, and replaced by a default label column.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) ResetLabels(labelLevels ...int) {
	if len(labelLevels) == 0 {
		labelLevels = makeIntRange(0, df.dataframe.numLevels())
	}
	for incrementor, i := range labelLevels {
		// iteratively subset all label levels except the one to be dropped
		adjustedIndex := i - incrementor
		if adjustedIndex >= df.dataframe.numLevels() {
			df.dataframe.resetWithError(fmt.Errorf(
				"ResetLabels(): index out of range (%d > %d)", i, df.dataframe.numLevels()+incrementor))
			return
		}
		newVal := df.dataframe.labels[adjustedIndex]
		// If label level name has default indicator, remove default indicator
		newVal.name = removeDefaultNameIndicator(newVal.name)
		df.dataframe.values = append(df.dataframe.values, newVal)
		exclude := excludeFromIndex(df.dataframe.numLevels(), adjustedIndex)
		df.dataframe.labels, _ = subsetContainers(df.dataframe.labels, exclude)
	}
	if df.dataframe.numLevels() == 0 {
		defaultLabels := makeDefaultLabels(0, df.dataframe.Len(), true)
		df.dataframe.labels = append(df.dataframe.labels, defaultLabels)
	}
	return
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
func (df *DataFrame) SetLabelNames(levelNames []string) *DataFrame {
	if len(levelNames) != len(df.labels) {
		return dataFrameWithError(
			fmt.Errorf("SetLabelNames(): number of `levelNames` must match number of levels in DataFrame (%d != %d)", len(levelNames), len(df.labels)))
	}
	for j := range levelNames {
		df.labels[j].name = levelNames[j]
	}
	return df
}

// SetColNames sets the names of all the columns in the DataFrame and returns the entire DataFrame.
func (df *DataFrame) SetColNames(colNames []string) *DataFrame {
	if len(colNames) != len(df.values) {
		return dataFrameWithError(
			fmt.Errorf("SetColNames(): number of `colNames` must match number of columns in DataFrame (%d != %d)",
				len(colNames), len(df.values)))
	}
	for k := range colNames {
		df.values[k].name = colNames[k]
	}
	return df
}

// -- RESHAPING

func (df *DataFrame) numColLevels() int {
	return len(df.colLevelNames)
}

func (df *DataFrame) numColumns() int {
	return len(df.values)
}

// Transpose switches all row values to column values and label names to column names.
// For example a DataFrame with 2 rows and 1 column has 2 columns and 1 row after transposition.
func (df *DataFrame) Transpose() *DataFrame {
	// row values become column values: 2 row x 1 col -> 2 col x 1 row
	vals := make([][]string, df.Len())
	valsIsNull := make([][]bool, df.Len())
	// each new column has the same number of rows as prior columns
	for i := range vals {
		vals[i] = make([]string, df.numColumns())
		valsIsNull[i] = make([]bool, df.numColumns())
	}
	// label names become column names: 2 row x 1 level -> 2 col x 1 level
	colNames := make([][]string, df.Len())
	// each new column name has the same number of levels as prior label levels
	for i := range colNames {
		colNames[i] = make([]string, df.numLevels())
	}
	// column levels become label levels: 2 level x 1 col -> 2 level x 1 row
	labels := make([][]string, df.numColLevels())
	labelsIsNull := make([][]bool, df.numColLevels())

	// column level names become label level names
	labelNames := make([]string, df.numColLevels())
	// label level names become column level names
	colLevelNames := make([]string, df.numLevels())

	// each new label level has same number of rows as prior columns
	for l := range labels {
		labels[l] = make([]string, df.numColumns())
		labelsIsNull[l] = make([]bool, df.numColumns())
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
		v := df.values[k].string().slice
		for i := range v {
			vals[i][k] = v[i]
			valsIsNull[i][k] = df.values[k].isNull[i]
		}
	}

	retColNames := make([]string, len(vals))
	for k := range colNames {
		retColNames[k] = joinLevelsIntoName(colNames[k])
	}
	// transfer to valueContainers
	retLabels := copyStringsIntoValueContainers(labels, labelsIsNull, labelNames)
	retVals := copyStringsIntoValueContainers(vals, valsIsNull, retColNames)

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
		return dataFrameWithError(fmt.Errorf("PromoteToColLevel(): %v", err))
	}
	// by default, include all original label levels in new labels
	residualLabelIndex := makeIntRange(0, len(df.labels))
	// check whether container refers to label or column
	if index >= len(df.labels) {
		if len(df.values) <= 1 {
			return dataFrameWithError(fmt.Errorf("PromoteToColLevel(): cannot stack only column"))
		}
	} else {
		if len(df.labels) <= 1 {
			return dataFrameWithError(fmt.Errorf("PromoteToColLevel(): cannot stack only label level"))
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
	numNewCols := len(uniqueValuesToPromote) * df.numColumns()
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
	for k := 0; k < df.numColumns(); k++ {
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

// Filter returns all rows that satisfy all of the `filters`,
// which is a map of container names (either column or label names) and tada.FilterFn structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its FilterFn struct provides the filter logic for that container.
//
// Values are coerced from their original type to the selected field type for filtering, but after filtering retains their original type.
// For example, {"foo": FilterFn{Float64: lambda}} converts the values in the foo container to float64,
// applies the true/false lambda function to each row in the container, and returns the rows that return true in their original type.
// Rows with null values are always excluded from the filtered data.
// If no filter is provided, returns a new copy of the DataFrame.
// For equality filtering on one or more containers, see also df.FilterByValue().
// Returns a new DataFrame.
func (df *DataFrame) Filter(filters map[string]FilterFn) *DataFrame {
	df.Copy()
	df.InPlace().Filter(filters)
	return df
}

// Filter returns all rows that satisfy all of the `filters`,
// which is a map of container names (either column or label names) and tada.FilterFn structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its FilterFn struct provides the filter logic for that container.
//
// Values are coerced from their original type to the selected field type for filtering, but after filtering retains their original type.
// For example, {"foo": FilterFn{Float64: lambda}} converts the values in the foo container to float64,
// applies the true/false lambda function to each row in the container, and returns the rows that return true in their original type.
// Rows with null values are always excluded from the filtered data.
// If no filter is provided, does nothing.
// For equality filtering on one or more containers, see also df.FilterByValue().
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Filter(filters map[string]FilterFn) {
	if len(filters) == 0 {
		return
	}

	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	index, err := filter(mergedLabelsAndCols, filters)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("Filter(): %v", err))
		return
	}
	df.Subset(index)
	return
}

// Where iterates over the rows in `df` and evaluates whether each one satisifes `filters`,
// which is a map of container names (either column or label names) and tada.FilterFn structs.
// If yes, returns `ifTrue` at that row position.
// If not, returns `ifFalse` at that row position.
// Values are coerced from their original type to the selected field type for filtering, but after filtering retains their original type.
//
// Returns an unnamed Series with a copy of the labels from the original Series and null status based on the supplied values.
// If an unsupported value type is supplied as either ifTrue or ifFalse, returns an error.
func (df *DataFrame) Where(filters map[string]FilterFn, ifTrue, ifFalse interface{}) (*Series, error) {
	ret := make([]interface{}, df.Len())
	// []int of positions where all filters are true
	mergedLabelsAndColumns := append(df.labels, df.values...)
	index, err := filter(mergedLabelsAndColumns, filters)
	if err != nil {
		return nil, fmt.Errorf("Where(): %v", err)
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
		_, err := setNullsFromInterface([]interface{}{ifTrue})
		// ifTrue is unsupported?
		if err != nil {
			return nil, fmt.Errorf("Where(): ifTrue: %v", err)
		}
		// ifFalse is unsupported?
		return nil, fmt.Errorf("Where(): ifFalse: %v", err)
	}
	return &Series{
		values: &valueContainer{
			slice:  ret,
			isNull: isNull,
		},
		labels: copyContainers(df.labels),
	}, nil
}

// FilterByValue returns a cross section of the rows in the DataFrame satisfying all `filters`,
// which is a map of of container names (either column or label names) to interface{} values.
// A filter is satisfied for a given row value if the stringified value in that container matches the stringified interface{} value.
// Returns a new DataFrame.
func (df *DataFrame) FilterByValue(filters map[string]interface{}) *DataFrame {
	df = df.Copy()
	df.InPlace().FilterByValue(filters)
	return df
}

// FilterByValue returns the rows in the DataFrame satisfying all `filters`,
// which is a map of of container names (either column or label names) to interface{} values.
// A filter is satisfied for a given row value if the stringified value in that container at that row matches the stringified interface{} value.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) FilterByValue(filters map[string]interface{}) {
	mergedLabelsAndColumns := append(df.dataframe.labels, df.dataframe.values...)
	index, err := filterByValue(mergedLabelsAndColumns, filters)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("FilterByValue(): %v", err))
		return
	}
	df.Subset(index)
	return
}

// -- APPLY

// Apply applies a user-defined function to every row in a container based on `lambdas`,
// which is a map of container names (either column or label names) to tada.ApplyFn structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its ApplyFn struct provides the apply logic for that container.
// Values are converted from their original type to the selected field type.
// For example, {"foo": ApplyFn{Float64: lambda}} converts the values in the foo container to float64 and
// applies the lambda function to each row in the container, outputting a new float64 value for each row.
// If a value is null either before or after the lambda function is applied, it is also null after.
// Returns a new DataFrame.
func (df *DataFrame) Apply(lambdas map[string]ApplyFn) *DataFrame {
	df.Copy()
	df.InPlace().Apply(lambdas)
	return df
}

// Apply applies a user-defined function to every row in a container based on `lambdas`,
// which is a map of container names (either column or label names) to tada.ApplyFn structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its ApplyFn struct provides the apply logic for that container.
// Values are converted from their original type to the selected field type.
// For example, {"foo": ApplyFn{Float64: lambda}} converts the values in the foo container to float64 and
// applies the lambda function to each row in the container, outputting a new float64 value for each row.
// If a value is null either before or after the lambda function is applied, it is also null after.// Modifies the underlying DataFrame in place.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Apply(lambdas map[string]ApplyFn) {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	for containerName, lambda := range lambdas {
		err := lambda.validate()
		if err != nil {
			df.dataframe.resetWithError((fmt.Errorf("Apply(): %v", err)))
			return
		}
		index, err := indexOfContainer(containerName, mergedLabelsAndCols)
		if err != nil {
			df.dataframe.resetWithError((fmt.Errorf("Apply(): %v", err)))
		}
		mergedLabelsAndCols[index].apply(lambda)
		// if either prior or new value is null, new value is null
		// ducks error because values are controlled to be of supported type
		newNulls, _ := setNullsFromInterface(mergedLabelsAndCols[index].slice)
		mergedLabelsAndCols[index].isNull = isEitherNull(
			mergedLabelsAndCols[index].isNull, newNulls)
	}
	return
}

// ApplyFormat applies a user-defined formatting function to every row in a container based on `lambdas`,
// which is a map of container names (either column or label names) to tada.ApplyFormatFn structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its ApplyFormatFn struct provides the formatting logic for that container.
// Values are converted from their original type to the selected field type and then to string.
// For example, {"foo": ApplyFormatFn{Float64: lambda}} converts the values in the foo container to float64 and
// applies the lambda function to each row in the container, outputting a new string value for each row.
// If a value is null either before or after the lambda function is applied, it is also null after.
// Returns a new DataFrame.
func (df *DataFrame) ApplyFormat(lambdas map[string]ApplyFormatFn) *DataFrame {
	df.Copy()
	df.InPlace().ApplyFormat(lambdas)
	return df
}

// ApplyFormat applies a user-defined formatting function to every row in a container based on `lambdas`,
// which is a map of container names (either column or label names) to tada.ApplyFormatFn structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its ApplyFormatFn struct provides the formatting logic for that container.
// Values are converted from their original type to the selected field type and then to string.
// For example, {"foo": ApplyFormatFn{Float64: lambda}} converts the values in the foo container to float64 and
// applies the lambda function to each row in the container, outputting a new string value for each row.
// If a value is null either before or after the lambda function is applied, it is also null after.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) ApplyFormat(lambdas map[string]ApplyFormatFn) {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	for containerName, lambda := range lambdas {
		err := lambda.validate()
		if err != nil {
			df.dataframe.resetWithError((fmt.Errorf("ApplyFormat(): %v", err)))
			return
		}
		index, err := indexOfContainer(containerName, mergedLabelsAndCols)
		if err != nil {
			df.dataframe.resetWithError((fmt.Errorf("ApplyFormat(): %v", err)))
		}
		mergedLabelsAndCols[index].applyFormat(lambda)
		// if either prior or new value is null, new value is null
		// ducks error because values are controlled to be of supported type
		newNulls, _ := setNullsFromInterface(mergedLabelsAndCols[index].slice)
		mergedLabelsAndCols[index].isNull = isEitherNull(
			mergedLabelsAndCols[index].isNull, newNulls)
	}
	return
}

// -- MERGERS

// Merge performs a left join of `other` onto `df` using containers with matching names as keys.
// To perform a different type of join or specify the matching keys,
// use df.LookupAdvanced() to isolate values in `other`, and append them with df.WithCol().
//
// Merge identifies the row alignment between `df` and `other` and appends aligned values as new columns on `df`.
// Rows are aligned when
// 1) one or more containers (either column or label level) in `other` share the same name as one or more containers in `df`,
// and 2) the stringified values in the `other` containers match the values in the `df` containers.
// For the following dataframes:
//
// `df`    	`other`
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in `df` is "aligned" with row 0 in `other`, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// After merging, the result will be:
//
// `df`
// FOO BAR QUX
// bar 0   n/a
// baz 1   corge
//
// Finally, all container names (columns and label names) are deduplicated after the merge so that they are unique.
// Returns a new DataFrame.
func (df *DataFrame) Merge(other *DataFrame) *DataFrame {
	df.Copy()
	df.InPlace().Merge(other)
	return df
}

// Merge performs a left join of `other` onto `df` using containers with matching names as keys.
// To perform a different type of join or specify the matching keys,
// use df.LookupAdvanced() to isolate values in `other`, and append them with df.WithCol().
//
// Merge identifies the row alignment between `df` and `other` and appends aligned values as new columns on `df`.
// Rows are aligned when:
// 1) one or more containers (either column or label level) in `other` share the same name as one or more containers in `df`,
// and 2) the stringified values in the `other` containers match the values in the `df` containers.
// For the following dataframes:
//
// `df`    	`other`
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in `df` is "aligned" with row 0 in `other`, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// After merging, the result will be:
//
// `df`
// FOO BAR QUX
// bar 0   n/a
// baz 1   corge
//
// Finally, all container names (columns and label names) are deduplicated after the merge so that they are unique.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Merge(other *DataFrame) {
	lookupDF := df.dataframe.Lookup(other)
	for k := range lookupDF.values {
		df.dataframe.values = append(df.dataframe.values, lookupDF.values[k])
	}
	df.DeduplicateNames()
}

// Lookup performs the lookup portion of a left join of `other` onto `df` using containers with matching names as keys.
// To perform a different type of lookup or specify the matching keys, use df.LookupAdvanced().
//
// Lookup identifies the row alignment between `df` and `other` and returns the aligned values.
// Rows are aligned when:
// 1) one or more containers (either column or label level) in `other` share the same name as one or more containers in `df`,
// and 2) the stringified values in the `other` containers match the values in the `df` containers.
// For the following dataframes:
//
// `df`    	`other`
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in `df` is "aligned" with row 0 in `other`, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// The result of a lookup will be:
//
// FOO BAR
// bar n/a
// baz corge
//
// Returns a new DataFrame.
func (df *DataFrame) Lookup(other *DataFrame) *DataFrame {
	return df.LookupAdvanced(other, "left", nil, nil)
}

// LookupAdvanced performs the lookup portion of a join of `other` onto `df` matching on the container keys specified.
//
// LookupAdvanced identifies the row alignment between `df` and `other` and returns the aligned values.
// Rows are aligned when:
// 1) one or more containers (either column or label level) in `other` share the same name as one or more containers in `df`,
// and 2) the stringified values in the `other` containers match the values in the `df` containers.
// For the following dataframes:
//
// `df`    	`other`
// FOO BAR	FRED QUX
// bar 0	baz  corge
// baz 1	qux  waldo
//
// In LookupAdvanced(other, "left", ["foo"], ["fred"]),
// row 1 in `df` is "aligned" with row 0 in `other`, because those are the rows in which
// both share the same value ("baz") in the keyed containers.
// The result of this lookup will be:
//
// FOO BAR
// bar n/a
// baz corge
//
// Returns a new DataFrame.
func (df *DataFrame) LookupAdvanced(other *DataFrame, how string, leftOn []string, rightOn []string) *DataFrame {
	mergedLabelsAndCols := append(df.labels, df.values...)
	otherMergedLabelsAndCols := append(other.labels, other.values...)
	var leftKeys, rightKeys []int
	var err error
	if len(leftOn) == 0 || len(rightOn) == 0 {
		if !(len(leftOn) == 0 && len(rightOn) == 0) {
			return dataFrameWithError(
				fmt.Errorf("LookupAdvanced(): if either leftOn or rightOn is empty, both must be empty"))
		}
	}
	if len(leftOn) == 0 {
		leftKeys, rightKeys = findMatchingKeysBetweenTwoLabelContainers(
			mergedLabelsAndCols, otherMergedLabelsAndCols)
	} else {
		leftKeys, err = convertColNamesToIndexPositions(leftOn, mergedLabelsAndCols)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("LookupAdvanced(): `leftOn`: %v", err))
		}
		rightKeys, err = convertColNamesToIndexPositions(rightOn, otherMergedLabelsAndCols)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("LookupAdvanced(): `rightOn`: %v", err))
		}
	}
	ret, err := lookupDataFrame(
		how, df.name, df.colLevelNames,
		df.values, df.labels, leftKeys,
		other.values, other.labels, rightKeys, leftOn, rightOn)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("LookupAdvanced(): %v", err))
	}
	return ret
}

// -- SORTERS

// Sort sorts the values `by` zero or more Sorter specifications.
// If no Sorter is supplied, does not sort.
// If no DType is supplied for a Sorter, sorts as float64.
// DType is only used for the process of sorting. Once it has been sorted, data retains its original type.
// Returns a new DataFrame.
func (df *DataFrame) Sort(by ...Sorter) *DataFrame {
	df.Copy()
	df.InPlace().Sort(by...)
	return df
}

// Sort sorts the values `by` zero or more Sorter specifications.
// If no Sorter is supplied, does not sort.
// If no DType is supplied for a Sorter, sorts as float64.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Sort(by ...Sorter) {
	if len(by) == 0 {
		df.dataframe.resetWithError(fmt.Errorf(
			"Sort(): must supply at least one Sorter"))
		return
	}

	mergedLabelsAndValues := append(df.dataframe.labels, df.dataframe.values...)
	// sortContainers iteratively updates the index
	newIndex, err := sortContainers(mergedLabelsAndValues, by)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("Sort(): %v", err))
		return
	}
	// rearrange the data in place with the final index
	df.Subset(newIndex)
}

// -- GROUPERS

// GroupBy groups the DataFrame rows that share the same stringified value
// in the container(s) (columns or labels) specified by `names`.
func (df *DataFrame) GroupBy(names ...string) *GroupedDataFrame {
	var index []int
	var err error
	mergedLabelsAndCols := append(df.labels, df.values...)
	// if no names supplied, group by all label levels and use all label level names
	if len(names) == 0 {
		index = makeIntRange(0, df.numLevels())
	} else {
		index, err = convertColNamesToIndexPositions(names, mergedLabelsAndCols)
		if err != nil {
			return groupedDataFrameWithError(fmt.Errorf("GroupBy(): %v", err))
		}
	}
	return df.groupby(index)
}

// expects index to refer to merged labels and columns
func (df *DataFrame) groupby(index []int) *GroupedDataFrame {
	mergedLabelsAndCols := append(df.labels, df.values...)
	containers, _ := subsetContainers(mergedLabelsAndCols, index)
	newLabels, rowIndices, orderedKeys := reduceContainers(containers)
	names := make([]string, len(index))
	for i, pos := range index {
		names[i] = mergedLabelsAndCols[pos].name
	}
	return &GroupedDataFrame{
		orderedKeys: orderedKeys,
		rowIndices:  rowIndices,
		labels:      newLabels,
		df:          df,
	}
}

// PivotTable creates a spreadsheet-style pivot table as a DataFrame by
// grouping rows using the unique values in `labels`,
// reducing the values in `values` using an `aggFunc` aggregation function, then
// promoting the unique values in `columns` to be new columns.
// `labels`, `columns`, and `values` should all refer to existing container names (either columns or labels).
// Supported `aggFunc`s: sum, mean, median, std, count, min, max.
func (df *DataFrame) PivotTable(labels, columns, values, aggFunc string) *DataFrame {

	mergedLabelsAndCols := append(df.labels, df.values...)
	labelIndex, err := indexOfContainer(labels, mergedLabelsAndCols)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("PivotTable(): `labels`: %v", err))
	}
	colIndex, err := indexOfContainer(columns, mergedLabelsAndCols)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("PivotTable(): `columns`: %v", err))
	}
	_, err = indexOfContainer(values, mergedLabelsAndCols)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("PivotTable(): `values`: %v", err))
	}
	grouper := df.groupby([]int{labelIndex, colIndex})
	var ret *DataFrame
	switch aggFunc {
	case "sum":
		ret = grouper.Sum(values)
	case "mean":
		ret = grouper.Mean(values)
	case "median":
		ret = grouper.Median(values)
	case "std":
		ret = grouper.Std(values)
	case "count":
		ret = grouper.Count(values)
	case "min":
		ret = grouper.Min(values)
	case "max":
		ret = grouper.Max(values)
	default:
		return dataFrameWithError(fmt.Errorf("PivotTable(): `aggFunc`: unsupported (%v)", aggFunc))
	}
	ret = ret.PromoteToColLevel(columns)
	ret.dropColLevel(1)
	return ret
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

// Resample coerces values to time.Time and truncates them by the logic supplied in `how`,
// which is a map of of container names (either column or label names) to tada.Resampler structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its Resampler struct provides the resampling logic for that container.
// Returns a new DataFrame.
func (df *DataFrame) Resample(how map[string]Resampler) *DataFrame {
	df = df.Copy()
	df.InPlace().Resample(how)
	return df
}

// Resample coerces values to time.Time and truncates them by the logic supplied in `how`,
// which is a map of of container names (either column or label names) to tada.Resampler structs.
// For each container name in the map, the first field selected (i.e., not left blank)
// in its Resampler struct provides the resampling logic for that container.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Resample(how map[string]Resampler) {
	mergedLabelsAndColumns := append(df.dataframe.labels, df.dataframe.values...)
	for name, resampler := range how {
		index, err := indexOfContainer(name, mergedLabelsAndColumns)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("Resample(): %v", err))
			return
		}
		mergedLabelsAndColumns[index].resample(resampler)
	}
	return
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

// Row returns the current row in the DataFrame as map[string]Element.
// The map keys are the names of containers (including label levels).
// The value in each map is an Element containing an interface value and a boolean denoting if the value is null.
// If multiple columns have the same header, only the Elements of the left-most column are returned.
func (iter *DataFrameIterator) Row() map[string]Element {
	ret := make(map[string]Element)
	for k := iter.df.numColumns() - 1; k >= 0; k-- {
		ret[iter.df.values[k].name] = iter.df.values[k].iterRow(iter.current)
	}
	for j := iter.df.numLevels() - 1; j >= 0; j-- {
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

// SumColumns finds each column matching a supplied `colName`, coerces its values to float64, and adds them row-wise.
// The resulting Series is named `name`.
// If any column has a null value for a given row, that row is considered null.
func (df *DataFrame) SumColumns(name string, colNames ...string) *Series {
	if len(colNames) == 0 {
		return seriesWithError(fmt.Errorf("SumColumns(): `colNames` cannot be empty"))
	}
	var ret *Series
	for i, name := range colNames {
		_, err := indexOfContainer(name, df.values)
		if err != nil {
			return seriesWithError(fmt.Errorf("SumColumns(): %v", err))
		}
		if i == 0 {
			ret = df.Col(name)
		} else {
			ret = ret.Add(df.Col(name), false)
		}
	}
	ret.SetName(name)
	return ret
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

// Std coerces the values in each column to float64 and calculates the standard deviation of each column.
func (df *DataFrame) Std() *Series {
	return df.math("std", std)
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
