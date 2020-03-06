package tada

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"reflect"
	"regexp"

	"github.com/olekukonko/tablewriter"
	"github.com/ptiger10/tablediff"
)

// -- CONSTRUCTORS

// MakeSlicesFromCrossProduct stub
func MakeSlicesFromCrossProduct(values []interface{}) ([]interface{}, error) {
	for k := range values {
		if !isSlice(values[k]) {
			return nil, fmt.Errorf("MakeSlicesFromCrossProduct(): position %d: must be slice", k)
		}
	}
	var numNewRows int
	for k := range values {
		v := reflect.ValueOf(values[k])
		if k == 0 {
			numNewRows = v.Len()
		} else {
			numNewRows *= v.Len()
		}
	}
	ret := make([]interface{}, len(values))
	for k := range values {
		v := reflect.ValueOf(values[k])
		newValues := reflect.MakeSlice(v.Type(), numNewRows, numNewRows)
		numRepeats := numNewRows / v.Len()
		// for first slice, repeat each value individaully
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

// NewDataFrame stub
// If supplying `labels` as []interface{}, be sure to use the spread operator (...),
// or else the labels will not be read properly.
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

// Copy stub
func (df *DataFrame) Copy() *DataFrame {
	colLevelNames := make([]string, len(df.colLevelNames))
	copy(colLevelNames, df.colLevelNames)

	return &DataFrame{
		values: copyContainers(df.values),
		// values: df.values,
		labels:        copyContainers(df.labels),
		err:           df.err,
		colLevelNames: colLevelNames,
		// colLevelNames: df.colLevelNames,
		name: df.name,
	}
}

// ConcatSeries stub
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

// Cast casts the underlying container values (column or label level) to []float64, []string, or []time.Time.
// Use cast to improve performance when calling multiple operations on values.
func (df *DataFrame) Cast(containerAsType map[string]DType) error {
	mergedLabelsAndCols := append(df.labels, df.values...)
	for name, dtype := range containerAsType {
		index, err := indexOfContainer(name, mergedLabelsAndCols)
		if err != nil {
			return fmt.Errorf("Cast(): %v", err)
		}
		mergedLabelsAndCols[index].cast(dtype)
	}
	return nil
}

// ReadCSV stub
func ReadCSV(csv [][]string, config *ReadConfig) *DataFrame {
	if len(csv) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadCSV(): csv must have at least one row"))
	}
	if len(csv[0]) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadCSV(): csv must have at least one column"))
	}
	config = defaultConfigIfNil(config)

	if config.MajorDimIsCols {
		return readCSVByCols(csv, config)
	}
	return readCSVByRows(csv, config)
}

// ImportCSV stub
func ImportCSV(path string, config *ReadConfig) (*DataFrame, error) {
	config = defaultConfigIfNil(config)
	data, err := ioutil.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("ImportCSV(): %s", err)
	}
	numRows, numCols, err := extractCSVDimensions(data, config.Delimiter)
	if numRows == 0 {
		return nil, fmt.Errorf("ImportCSV(): must have at least one row")
	}
	retVals := makeByteMatrix(numCols, numRows)
	retNulls := makeBoolMatrix(numCols, numRows)
	r := bytes.NewReader(data)
	err = readCSVBytes(r, retVals, retNulls, config.Delimiter)
	if err != nil {
		return nil, fmt.Errorf("ImportCSV(): %s", err)
	}
	return makeDataFrameFromMatrices(retVals, retNulls, config), nil
}

// ReadInterface stub
func ReadInterface(input [][]interface{}, config *ReadConfig) *DataFrame {
	config = defaultConfigIfNil(config)

	if len(input) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadInterface(): `input` must have at least one row"))
	}
	if len(input[0]) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadInterface(): `input` must have at least one column"))
	}
	// convert [][]interface to [][]string
	str := make([][]string, len(input))
	for j := range str {
		str[j] = make([]string, len(input[0]))
	}
	for i := range input {
		for j := range input[i] {
			str[i][j] = fmt.Sprint(input[i][j])
		}
	}
	if config.MajorDimIsCols {
		return readCSVByCols(str, config)
	}
	return readCSVByRows(str, config)
}

// ReadMatrix stub
func ReadMatrix(mat Matrix) *DataFrame {
	numRows, numCols := mat.Dims()
	csv := make([][]string, numCols)
	for k := range csv {
		csv[k] = make([]string, numRows)
		for i := 0; i < numRows; i++ {
			csv[k][i] = fmt.Sprint(mat.At(i, k))
		}
	}
	return readCSVByCols(csv, &ReadConfig{})
}

// ReadStruct stub
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

// ToSeries stub
func (df *DataFrame) ToSeries() *Series {
	if len(df.values) != 1 {
		return seriesWithError(fmt.Errorf("ToSeries(): DataFrame must have a single column"))
	}
	return &Series{
		values: df.values[0],
		labels: df.labels,
	}
}

// ToCSV converts a DataFrame to a [][]string with rows as the major dimension
func (df *DataFrame) ToCSV(ignoreLabels bool) [][]string {
	transposedStringValues, err := df.toCSVByRows(ignoreLabels)
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

// ExportCSV converts a DataFrame to a [][]string with rows as the major dimension, and writes the output to a csv file.
func (df *DataFrame) ExportCSV(file string, ignoreLabels bool) error {
	transposedStringValues, err := df.toCSVByRows(ignoreLabels)
	if err != nil {
		return fmt.Errorf("ToCSV(): %v", err)
	}
	var b bytes.Buffer
	w := csv.NewWriter(&b)
	// duck error because csv is controlled
	w.WriteAll(transposedStringValues)
	ioutil.WriteFile(file, b.Bytes(), 0666)
	return nil
}

// ToInterface exports a DataFrame to a [][]interface with rows as the major dimension.
func (df *DataFrame) ToInterface(ignoreLabels bool) [][]interface{} {
	transposedStringValues, err := df.toCSVByRows(ignoreLabels)
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

// EqualsCSV converts a dataframe to csv, compares it to another csv, and evaluates whether the two match and isolates their differences
func (df *DataFrame) EqualsCSV(csv [][]string, ignoreLabels bool) (bool, *tablediff.Differences) {
	compare := df.ToCSV(ignoreLabels)
	diffs, eq := tablediff.Diff(compare, csv)
	return eq, diffs
}

// WriteMockCSV writes a mock csv to `w` modeled after `src`.
func WriteMockCSV(src [][]string, w io.Writer, config *ReadConfig, outputRows int) error {
	// whether the major dimension of source is rows or columns, the major dimension of the output csv is rows
	config = defaultConfigIfNil(config)
	numPreviewRows := 10
	inferredTypes := make([]map[string]int, 0)
	dtypes := []string{"float", "int", "string", "datetime", "time", "bool"}
	var headers [][]string
	// default: major dimension is rows
	if !config.MajorDimIsCols {
		if len(src) == 0 {
			return fmt.Errorf("WriteMockCSV(): csv must have at least one row")
		}
		if len(src[0]) == 0 {
			return fmt.Errorf("WriteMockCSV(): csv must have at least one column")
		}
		maxRows := len(src) - config.NumHeaderRows
		if maxRows < numPreviewRows {
			numPreviewRows = maxRows
		}
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

		// offset preview by header rows
		preview := src[config.NumHeaderRows : numPreviewRows+config.NumHeaderRows]
		for i := range preview {
			for k := range preview[i] {
				datum := preview[i][k]
				dtype := inferType(datum)
				inferredTypes[k][dtype]++
			}
		}
		// major dimension is columns
	} else {
		if len(src) == 0 {
			return fmt.Errorf("WriteMockCSV(): csv must have at least one column")
		}
		if len(src[0]) == 0 {
			return fmt.Errorf("WriteMockCSV(): csv must have at least one row")
		}
		maxRows := len(src[0]) - config.NumHeaderRows
		if maxRows < numPreviewRows {
			numPreviewRows = maxRows
		}

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
				// major dimension of output is rows
				headers[l][k] = src[k][l]
			}
		}

		// iterate over each column
		for k := range src {
			// offset by header rows
			values := src[k][config.NumHeaderRows : numPreviewRows+config.NumHeaderRows]
			for i := range values {
				dtype := inferType(values[i])
				inferredTypes[k][dtype]++
			}
		}
	}
	// major dimension of output is rows, for compatibility with csv.NewWriter
	mockCSV := mockCSVFromDTypes(inferredTypes, outputRows)
	mockCSV = append(headers, mockCSV...)
	writer := csv.NewWriter(w)
	return writer.WriteAll(mockCSV)
}

// -- GETTERS

func removeDefaultNameIndicator(name string) string {
	return regexp.MustCompile(`^\*`).ReplaceAllString(name, "")
}

func suppressDefaultName(name string) string {
	if regexp.MustCompile(`^\*`).MatchString(name) {
		return ""
	}
	return name
}

func (df *DataFrame) String() string {
	if df.values == nil {
		if df.Err() != nil {
			return df.Err().Error()
		}
	}
	var data [][]string
	if df.Len() <= optionMaxRows {
		data = df.ToCSV(false)
	} else {
		// truncate rows
		n := optionMaxRows / 2
		topHalf := df.Head(n).ToCSV(false)
		bottomHalf := df.Tail(n).ToCSV(false)[df.numColLevels():]
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
	// demarcate index headers
	for l := 0; l < df.numColLevels(); l++ {
		for j := 0; j < df.numLevels(); j++ {
			data[l][j] = fmt.Sprintf("-%v-", data[l][j])
		}
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
	var buf bytes.Buffer
	table := tablewriter.NewWriter(&buf)
	// optional caption
	if df.name != "" {
		table.SetCaption(true, fmt.Sprintf("name: %v", df.name))
	}

	table.SetHeader(data[0])
	table.AppendBulk(data[1:])
	table.SetAutoMergeCells(optionAutoMerge)

	table.Render()
	return string(buf.Bytes())
}

// At returns the Element at the `row` and `column` index positions. If `row` or `column` is out of range, returns an empty Element.
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

// ListColumns returns the name of all the columns in the DataFrame
func (df *DataFrame) ListColumns() []string {
	return listNames(df.values)
}

// ListLabelNames returns the name and position of all the label levels in the DataFrame
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

// InPlace returns a DataFrameMutator, which contains most of the same methods as DataFrame but never returns a new DataFrame.
// If you want to save memory and improve performance and do not need to preserve the original DataFrame, consider using InPlace().
func (df *DataFrame) InPlace() *DataFrameMutator {
	return &DataFrameMutator{dataframe: df}
}

// Subset returns only the rows specified at the index positions, in the order specified. Returns a new DataFrame.
func (df *DataFrame) Subset(index []int) *DataFrame {
	df = df.Copy()
	df.InPlace().Subset(index)
	return df
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Subset(index []int) {
	if reflect.DeepEqual(index, []int{-999}) {
		df.dataframe.resetWithError(errors.New(
			"Subset(): invalid filter (every filter must have at least one filter function; if ColName is supplied, it must be valid)"))
	}
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

// DeduplicateNames stub
func (df *DataFrame) DeduplicateNames() *DataFrame {
	df = df.Copy()
	df.InPlace().DeduplicateNames()
	return df
}

// DeduplicateNames stub
func (df *DataFrameMutator) DeduplicateNames() {
	mergedLabelsAndCols := append(df.dataframe.labels, df.dataframe.values...)
	deduplicateContainerNames(mergedLabelsAndCols)
}

// IndexOf stub. If `name` does not match any container, -1 is returned.
// If `columns` is true, only column names will be searched.
// If `columns` is false, only label level names will be searched.
func (df *DataFrame) IndexOf(name string, columns bool) int {
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

// GetLabels returns label levels as slices within an []interface
// that may be supplied as optional `labels` argument to NewSeries() or NewDataFrame().
func (df *DataFrame) GetLabels() []interface{} {
	var ret []interface{}
	labels := copyContainers(df.labels)
	for j := range labels {
		ret = append(ret, labels[j].slice)
	}
	return ret
}

// SelectLabels finds the first level with matching `name` and returns as a Series with all existing label levels (including itself).
// If label level name is default (prefixed with *), removes the prefix.
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

// Col finds the first column with matching `name` and returns as a Series. Similar to SelectLabels, but to select a column instead.
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

// Cols returns all column with matching `names`.
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

// Head returns the first `n` rows of the Series. If `n` is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
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

// Tail returns the last `n` rows of the Series. If `n` is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
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

// FillNull fills all the null values in the specified container name (column or label level) and makes them not-null.
// By default, applies NullFiller to Series values.
// Returns a new Series.
func (df *DataFrame) FillNull(how map[string]NullFiller) *DataFrame {
	df = df.Copy()
	df.InPlace().FillNull(how)
	return df
}

// FillNull fills all the null values and makes them not-null.
// By default, applies NullFiller to Series values.
// Modifies the underlying Series.
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

// DropNull removes rows with null values.
// If `subset` is supplied, removes any rows with null values in any of the specified columns.
// Returns a new DataFrame.
func (df *DataFrame) DropNull(subset ...string) *DataFrame {
	df = df.Copy()
	df.InPlace().DropNull(subset...)
	return df
}

// DropNull removes rows with null values.
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

// FilterCols returns the column positions of all columns (excluding labels) that satisfy `lambda`.
// If a column contains multiple levels, its name is a single pipe-delimited string and may be split within the lambda function.
func (df *DataFrame) FilterCols(lambda func(string) bool) []int {
	var ret []int
	for k := range df.values {
		if lambda(df.values[k].name) {
			ret = append(ret, k)
		}
	}
	return ret
}

// -- SETTERS

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a column of labels exists that matches `name`: rename the level to match `input`
//
// If a slice is supplied as `input` and a column of labels exists that matches `name`: replace the values at this level to match `input`
//
// If a slice is supplied as `input` and a column of labels does not exist that matches `name`: append a new level with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying DataFrame, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, returns a new DataFrame.
func (df *DataFrame) WithLabels(name string, input interface{}) *DataFrame {
	df.Copy()
	df.InPlace().WithLabels(name, input)
	return df
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a column of labels exists that matches `name`: rename the level to match `input`
//
// If a slice is supplied as `input` and a column of labels exists that matches `name`: replace the values at this level to match `input`
//
// If a slice is supplied as `input` and a column of labels does not exist that matches `name`: append a new level with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying DataFrame, or supplying scalar string and `name` that does not match an existing label level.
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
// If a scalar string is supplied as `input` and a column exists that matches `name`: rename the column to match `input`
//
// If a slice is supplied as `input` and a column exists that matches `name`: replace the values at this column to match `input`
//
// If a slice is supplied as `input` and a column does not exist that matches `name`: append a new column with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying DataFrame, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, returns a new DataFrame.
func (df *DataFrame) WithCol(name string, input interface{}) *DataFrame {
	df.Copy()
	df.InPlace().WithCol(name, input)
	return df
}

// WithCol resolves as follows:
//
// If a scalar string is supplied as `input` and a column exists that matches `name`: rename the column to match `input`
//
// If a slice is supplied as `input` and a column exists that matches `name`: replace the values at this column to match `input`
//
// If a slice is supplied as `input` and a column does not exist that matches `name`: append a new column with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying DataFrame, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, modifies the underlying DataFrame in place.
func (df *DataFrameMutator) WithCol(name string, input interface{}) {
	cols, err := withColumn(df.dataframe.values, name, input, df.dataframe.Len())
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("WithCol(): %v", err))
	}
	df.dataframe.values = cols
}

// DropLabels drops the first label level matching `name`
// Returns a new DataFrame.
func (df *DataFrame) DropLabels(name string) *DataFrame {
	df.Copy()
	df.InPlace().DropLabels(name)
	return df
}

// DropLabels drops the first label level matching `name`
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) DropLabels(name string) {
	newCols, err := dropFromContainers(name, df.dataframe.labels)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("DropCol(): %v", err))
		return
	}
	df.dataframe.labels = newCols
	return
}

// DropCol drops the first column matching `name`
// Returns a new DataFrame.
func (df *DataFrame) DropCol(name string) *DataFrame {
	df.Copy()
	df.InPlace().DropCol(name)
	return df
}

// DropCol drops the first column matching `name`
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
// Returns a new Series.
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

// Relabel stub
func (df *DataFrame) Relabel(levelNames []string) *DataFrame {
	df = df.Copy()
	df.InPlace().Relabel(levelNames)
	return df
}

// Relabel stub
func (df *DataFrameMutator) Relabel(levelNames []string) {
	for _, name := range levelNames {
		lvl, err := indexOfContainer(name, df.dataframe.labels)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("Relabel(): %v", err))
			return
		}
		df.dataframe.labels[lvl].relabel()
	}
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

// Name returns the name of the DataFrame
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

// reshape

func (df *DataFrame) numColLevels() int {
	return len(df.colLevelNames)
}

func (df *DataFrame) numColumns() int {
	return len(df.values)
}

// Transpose stub
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
		splitColName := splitLabelIntoLevels(df.values[k].name, df.numColLevels() > 1)
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
		retColNames[k] = joinLevelsIntoLabel(colNames[k])
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

// PromoteToColLevel pivots an existing container (either column or label level) into a new column level.
// If promoting would use either the last column or index level, it returns an error.
// Adds new columns - each unique value in the stacked column is stacked above each existing column.
// Can remove rows - returns only one row per unique label combination.
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
			newHeader := joinLevelsIntoLabel([]string{uniqueValue, df.values[k].name})
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

	// -- transfer values and to final form

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

// Filter stub
func (df *DataFrame) Filter(filters map[string]FilterFn) []int {
	if len(filters) == 0 {
		return makeIntRange(0, df.Len())
	}

	mergedLabelsAndCols := append(df.labels, df.values...)
	ret, err := filter(mergedLabelsAndCols, filters)
	if err != nil {
		return []int{-999}
	}
	return ret
}

// -- APPLY

// Apply applies a user-defined `lambda` function to every row in a particular column and coerces all values to match the lambda type.
// Apply may be applied to any label level or column by specifying the container name as a key in the `lambdas` map.
// If a value is considered null either prior to or after the lambda function is applied, it is considered null after.
// Returns a new DataFrame.
func (df *DataFrame) Apply(lambdas map[string]ApplyFn) *DataFrame {
	df.Copy()
	df.InPlace().Apply(lambdas)
	return df
}

// Apply applies a user-defined `lambda` function to every row in a particular column and coerces all values to match the lambda type.
// Apply may be applied to any label level or column by specifying the container name as a key in the `lambdas` map.
// If a value is considered null either prior to or after the lambda function is applied, it is considered null after.
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
		mergedLabelsAndCols[index].isNull = isEitherNull(
			mergedLabelsAndCols[index].isNull,
			setNullsFromInterface(mergedLabelsAndCols[index].slice))
	}
	return
}

// ApplyFormat stub
func (df *DataFrame) ApplyFormat(lambdas map[string]ApplyFormatFn) *DataFrame {
	df.Copy()
	df.InPlace().ApplyFormat(lambdas)
	return df
}

// ApplyFormat stub
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
		mergedLabelsAndCols[index].isNull = isEitherNull(
			mergedLabelsAndCols[index].isNull,
			setNullsFromInterface(mergedLabelsAndCols[index].slice))
	}
	return
}

// -- MERGERS

// Merge stub
func (df *DataFrame) Merge(other *DataFrame) *DataFrame {
	df.Copy()
	df.InPlace().Merge(other)
	return df
}

// Merge adds the columns of the `other` DataFrame where there is label alignment on containers with matching names.
// Merged containers are deduplicated so that they are unique.
func (df *DataFrameMutator) Merge(other *DataFrame) {
	lookupDF := df.dataframe.Lookup(other)
	for k := range lookupDF.values {
		df.dataframe.values = append(df.dataframe.values, lookupDF.values[k])
	}
	df.DeduplicateNames()
}

// Lookup stub
func (df *DataFrame) Lookup(other *DataFrame) *DataFrame {
	return df.LookupAdvanced(other, "left", nil, nil)
}

// LookupAdvanced stub
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

// Sort stub
func (df *DataFrame) Sort(by ...Sorter) *DataFrame {
	df.Copy()
	df.InPlace().Sort(by...)
	return df
}

// Sort stub
func (df *DataFrameMutator) Sort(by ...Sorter) {
	if len(by) == 0 {
		df.dataframe.resetWithError(fmt.Errorf(
			"Sort(): must supply at least one Sorter"))
		return
	}

	// original index
	mergedLabelsAndValues := append(df.dataframe.labels, df.dataframe.values...)
	newIndex, err := sortContainers(mergedLabelsAndValues, by)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("Sort(): %v", err))
		return
	}
	// rearrange the data in place with the final index
	df.Subset(newIndex)
}

// -- GROUPERS

// GroupBy stub
// includes label levels and columns
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

// PivotTable stub
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
	default:
		return dataFrameWithError(fmt.Errorf("PivotTable(): `aggFunc`: unsupported (%v)", aggFunc))
	}
	ret = ret.PromoteToColLevel(columns)
	ret.dropColLevel(1)
	return ret
}

// inplace
func (df *DataFrame) dropColLevel(level int) *DataFrame {
	df.colLevelNames = append(df.colLevelNames[:level], df.colLevelNames[level+1:]...)
	for k := range df.values {
		priorNames := splitLabelIntoLevels(df.values[k].name, true)
		newNames := append(priorNames[:level], priorNames[level+1:]...)
		df.values[k].name = joinLevelsIntoLabel(newNames)
	}
	return df
}

// -- ITERATORS

// IterRows returns a slice of maps that return the underlying data for every row in the DataFrame.
// The key in each map is a column header, including label level headers.
// The value in each map is an Element containing an interface value and whether or not the value is null.
// If multiple label levels or columns have the same header, only the Elements of the right-most column are returned.
func (df *DataFrame) IterRows() []map[string]Element {
	ret := make([]map[string]Element, df.Len())
	for i := 0; i < df.Len(); i++ {
		// all label levels + all columns
		ret[i] = make(map[string]Element, df.numLevels()+len(df.values))
		for j := range df.labels {
			key := df.labels[j].name
			ret[i][key] = df.labels[j].iterRow(i)
		}
		for k := range df.values {
			key := df.values[k].name
			ret[i][key] = df.values[k].iterRow(i)
		}
	}
	return ret
}

// -- MATH

func (df *DataFrame) math(name string, mathFunction func([]float64, []bool, []int) (float64, bool)) *Series {
	retVals := make([]float64, len(df.values))
	retIsNull := make([]bool, len(df.values))
	labels := make([]string, len(df.values))
	labelsIsNull := make([]bool, len(df.values))

	for k := range df.values {
		retVals[k], retIsNull[k] = mathFunction(
			df.values[k].float64().slice,
			df.values[k].isNull,
			makeIntRange(0, df.Len()))

		labels[k] = df.values[k].name
		labelsIsNull[k] = false
	}
	return &Series{
		values: &valueContainer{slice: retVals, isNull: retIsNull, name: name},
		labels: []*valueContainer{{slice: labels, isNull: labelsIsNull, name: "*0"}},
	}
}

// Sum coerces the values in each column to float64 and sums each column.
func (df *DataFrame) Sum() *Series {
	return df.math("sum", sum)
}

// Mean stub
func (df *DataFrame) Mean() *Series {
	return df.math("mean", mean)
}

// Median stub
func (df *DataFrame) Median() *Series {
	return df.math("median", median)
}

// Std stub
func (df *DataFrame) Std() *Series {
	return df.math("std", std)
}

// Count stub
func (df *DataFrame) Count() *Series {
	return df.math("count", count)
}

// Min stub
func (df *DataFrame) Min() *Series {
	return df.math("min", min)
}

// Max stub
func (df *DataFrame) Max() *Series {
	return df.math("max", max)
}
