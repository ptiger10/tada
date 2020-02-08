package tada

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"io/ioutil"
	"reflect"
	"regexp"
	"strings"
)

// -- CONSTRUCTORS

// NewDataFrame stub
func NewDataFrame(slices []interface{}, labels ...interface{}) *DataFrame {
	// handle values
	var values []*valueContainer
	for i, slice := range slices {
		if !isSlice(slice) {
			return &DataFrame{err: fmt.Errorf(
				"NewDataFrame(): unsupported kind (%v) in `slices` (position %v); must be slice", reflect.TypeOf(slice), i)}
		}
		if reflect.ValueOf(slice).Len() == 0 {
			return &DataFrame{err: fmt.Errorf("NewDataFrame(): empty slice in slices (position %v): cannot be empty", i)}
		}
		isNull := setNullsFromInterface(slice)
		if isNull == nil {
			return &DataFrame{err: fmt.Errorf(
				"NewDataFrame(): unable to calculate null values ([]%v not supported)", reflect.TypeOf(slice).Elem())}
		}
		// handle special case of []Element: convert to []interface{}
		elements := handleElementsSlice(slice)
		if elements != nil {
			slice = elements
		}
		values = append(values, &valueContainer{slice: slice, isNull: isNull, name: fmt.Sprintf("%d", i)})
	}

	// handle labels
	retLabels := make([]*valueContainer, len(labels))
	if len(retLabels) == 0 {
		// default labels
		defaultLabels, isNull := makeDefaultLabels(0, reflect.ValueOf(slices[0]).Len())
		retLabels = append(retLabels, &valueContainer{slice: defaultLabels, isNull: isNull, name: "*0"})
	} else {
		for i := range retLabels {
			slice := labels[i]
			if !isSlice(slice) {
				return dataFrameWithError(fmt.Errorf("NewDataFrame(): unsupported label kind (%v) at level %d; must be slice", reflect.TypeOf(slice), i))
			}
			isNull := setNullsFromInterface(slice)
			if isNull == nil {
				return dataFrameWithError(fmt.Errorf(
					"NewDataFrame(): unable to calculate null values at level %d ([]%v not supported)", i, reflect.TypeOf(slice).Elem()))
			}
			// handle special case of []Element: convert to []interface{}
			elements := handleElementsSlice(slice)
			if elements != nil {
				slice = elements
			}
			retLabels[i] = &valueContainer{slice: slice, isNull: isNull, name: fmt.Sprintf("*%d", i)}
		}
	}

	return &DataFrame{values: values, labels: retLabels}
}

// Copy stub
func (df *DataFrame) Copy() *DataFrame {
	values := make([]*valueContainer, len(df.values))
	for j := range df.values {
		values[j] = df.values[j].copy()
	}

	labels := make([]*valueContainer, len(df.labels))
	for j := range df.labels {
		labels[j] = df.labels[j].copy()
	}

	return &DataFrame{
		values: values,
		labels: labels,
		err:    df.err,
		name:   df.name,
	}
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

func (df *DataFrame) toCSVByRows() [][]string {
	ret := make([][]string, df.numColLevels()+df.Len())
	for i := range ret {
		ret[i] = make([]string, df.numLevels()+df.numColumns())
	}
	for j := range df.labels {
		// write label headers, index at first header row
		ret[df.numColLevels()-1][j] = df.labels[j].name
		v := df.labels[j].str().slice
		// write label values, offset by header rows
		for i := range v {
			ret[i+df.numColLevels()][j] = v[i]
		}
	}
	// if there are multiple column headers, those rows will be blank above the index header
	for k := range df.values {
		multiColHeaders := splitLabelIntoLevels(df.values[k].name)
		for l := 0; l < df.numColLevels(); l++ {
			// write multi column headers, offset by label levels
			ret[l][k+df.numLevels()] = multiColHeaders[l]
		}
		v := df.values[k].str().slice
		// write label values, offset by header rows and label levels
		for i := range v {
			ret[i+df.numColLevels()][k+df.numLevels()] = v[i]
		}
	}
	return ret
}

func readCSVByRows(csv [][]string, cfg ReadConfig) *DataFrame {
	levelSeparator := "|"
	numCols := len(csv[0]) - cfg.NumLabelCols
	numRows := len(csv) - cfg.NumHeaderRows

	// prepare intermediary values containers
	vals := make([][]string, numCols)
	valsIsNull := make([][]bool, numCols)
	valsNames := make([][]string, numCols)
	for k := range vals {
		vals[k] = make([]string, numRows)
		valsIsNull[k] = make([]bool, numRows)
		valsNames[k] = make([]string, cfg.NumHeaderRows)
	}

	// prepare intermediary label containers
	labels := make([][]string, cfg.NumLabelCols)
	labelsIsNull := make([][]bool, cfg.NumLabelCols)
	labelsNames := make([][]string, cfg.NumLabelCols)
	for j := range labels {
		labels[j] = make([]string, numRows)
		labelsIsNull[j] = make([]bool, numRows)
		labelsNames[j] = make([]string, cfg.NumHeaderRows)
	}

	// iterate over csv and transpose rows and columns
	for row := range csv {
		for column := range csv[row] {
			if row < cfg.NumHeaderRows {
				if column < cfg.NumLabelCols {
					// write header rows to labels, no offset
					labelsNames[column][row] = csv[row][column]
				} else {
					// write header rows to cols, offset for label cols
					valsNames[column-cfg.NumLabelCols][row] = csv[row][column]
				}
				continue
			}
			if column < cfg.NumLabelCols {
				// write values to labels, offset for header rows
				labels[column][row-cfg.NumHeaderRows] = csv[row][column]
				labelsIsNull[column][row-cfg.NumHeaderRows] = isNullString(csv[row][column])
			} else {
				// write values to cols, offset for label cols and header rows
				vals[column-cfg.NumLabelCols][row-cfg.NumHeaderRows] = csv[row][column]
				valsIsNull[column-cfg.NumLabelCols][row-cfg.NumHeaderRows] = isNullString(csv[row][column])
			}
		}
	}

	// transfer values to final value containers
	retLabels := make([]*valueContainer, len(labels))
	retVals := make([]*valueContainer, len(vals))
	for k := range retVals {
		retVals[k] = &valueContainer{
			slice:  vals[k],
			isNull: valsIsNull[k],
			name:   strings.Join(valsNames[k], levelSeparator),
		}
	}
	for j := range retLabels {
		retLabels[j] = &valueContainer{
			slice:  labels[j],
			isNull: labelsIsNull[j],
			name:   strings.Join(labelsNames[j], levelSeparator),
		}
	}
	// create default labels if no labels
	if len(retLabels) == 0 {
		labels, isNull := makeDefaultLabels(0, numRows)
		retLabels = append(retLabels, &valueContainer{slice: labels, isNull: isNull, name: "*0"})
	}
	return &DataFrame{
		values: retVals,
		labels: retLabels,
	}
}

func readCSVByCols(csv [][]string, cfg ReadConfig) *DataFrame {
	levelSeparator := "|"
	numRows := len(csv[0]) - cfg.NumHeaderRows
	numCols := len(csv) - cfg.NumLabelCols

	// prepare intermediary values containers
	vals := make([][]string, numCols)
	valsIsNull := make([][]bool, numCols)
	valsNames := make([]string, numCols)

	// prepare intermediary label containers
	labels := make([][]string, cfg.NumLabelCols)
	labelsIsNull := make([][]bool, cfg.NumLabelCols)
	labelsNames := make([]string, cfg.NumLabelCols)

	// iterate over all cols to get header names
	for j := 0; j < cfg.NumLabelCols; j++ {
		// write label headers, no offset
		labelsNames[j] = strings.Join(csv[j][:cfg.NumHeaderRows], levelSeparator)
	}
	for k := 0; k < numCols; k++ {
		// write col headers, offset for label cols
		valsNames[k] = strings.Join(csv[k+cfg.NumLabelCols][:cfg.NumHeaderRows], levelSeparator)
	}
	for col := range csv {
		if col < cfg.NumLabelCols {
			// write label values, offset for header rows
			valsToWrite := csv[col][cfg.NumHeaderRows:]
			labels[col] = valsToWrite
			labelsIsNull[col] = setNullsFromInterface(valsToWrite)
		} else {
			// write column values, offset for label cols and header rows
			valsToWrite := csv[col+cfg.NumLabelCols][cfg.NumHeaderRows:]
			vals[col] = valsToWrite
			valsIsNull[col] = setNullsFromInterface(valsToWrite)
		}
	}

	// transfer values to final value containers
	retLabels := make([]*valueContainer, len(labels))
	retVals := make([]*valueContainer, len(vals))
	for k := range retVals {
		retVals[k] = &valueContainer{
			slice:  vals[k],
			isNull: valsIsNull[k],
			name:   valsNames[k],
		}
	}
	for j := range retLabels {
		retLabels[j] = &valueContainer{
			slice:  labels[j],
			isNull: labelsIsNull[j],
			name:   labelsNames[j],
		}
	}
	// create default labels if no labels
	if len(retLabels) == 0 {
		labels, isNull := makeDefaultLabels(0, numRows)
		retLabels = append(retLabels, &valueContainer{slice: labels, isNull: isNull, name: "*0"})
	}
	return &DataFrame{
		values: retVals,
		labels: retLabels,
	}

}

// ReadCSV stub
func ReadCSV(path string, config ...ReadConfig) *DataFrame {
	var cfg ReadConfig
	if len(config) >= 1 {
		cfg = config[0]
	}

	data, err := ioutil.ReadFile(path)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("ReadCSV(): %s", err))
	}
	reader := csv.NewReader(bytes.NewReader(data))
	if cfg.Delimiter != 0 {
		reader.Comma = cfg.Delimiter
	}

	csv, err := reader.ReadAll()
	if err != nil {
		return dataFrameWithError(fmt.Errorf("ReadCSV(): %s", err))
	}

	if len(csv) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadCSV(): csv must have at least one row"))
	}
	if len(csv[0]) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadCSV(): csv must at least one column"))
	}
	return readCSVByRows(csv, cfg)

}

// ReadInterface stub
func ReadInterface(input [][]interface{}, majorDimRows bool, config ...ReadConfig) *DataFrame {
	var cfg ReadConfig
	if len(config) >= 1 {
		cfg = config[0]
	}

	if len(input) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadInterface(): `input` must have at least one row"))
	}
	if len(input[0]) == 0 {
		return dataFrameWithError(fmt.Errorf("ReadInterface(): `input` must at least one column"))
	}
	str := make([][]string, len(input))
	for j := range str {
		str[j] = make([]string, len(input[0]))
	}
	for i := range input {
		for j := range input[i] {
			str[i][j] = fmt.Sprint(input[i][j])
		}
	}
	if majorDimRows {
		return readCSVByRows(str, cfg)
	}
	return readCSVByCols(str, cfg)
}

// ReadStructs stub
func ReadStructs(interface{}) *DataFrame {
	return nil
}

// -- GETTERS

// Len returns the number of rows in each column of the DataFrame.
func (df *DataFrame) Len() int {
	return reflect.ValueOf(df.values[0].slice).Len()
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

// ListLevels returns the name and position of all the label levels in the DataFrame
func (df *DataFrame) ListLevels() []string {
	return listNames(df.values)
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
	labels, err := subsetCols(df.dataframe.labels, index)
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
	cols, err := subsetCols(df.dataframe.values, index)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("SubsetCols(): %v", err))
		return
	}
	df.dataframe.values = cols
	return
}

// Col finds the first column with matching `name` and returns as a Series.
func (df *DataFrame) Col(name string) *Series {
	index, err := findColWithName(name, df.values)
	if err != nil {
		return seriesWithError(fmt.Errorf("Col(): %v", err))
	}
	return &Series{
		values: df.values[index],
		labels: df.labels,
	}
}

// Cols returns all column with matching `names`.
func (df *DataFrame) Cols(names ...string) *DataFrame {
	vals := make([]*valueContainer, len(names))
	for i, name := range names {
		index, err := findColWithName(name, df.values)
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
	return &DataFrame{values: retVals, labels: retLabels, name: df.name}
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
	return &DataFrame{values: retVals, labels: retLabels, name: df.name}
}

// Range returns the rows of the DataFrame starting at `first` and `ending` with last (inclusive).
// If either `first` or `last` is greater than the length of the DataFrame, a DataFrame error is returned.
// In all cases, returns a new DataFrame.
func (df *DataFrame) Range(first, last int) *DataFrame {
	if first >= df.Len() {
		return dataFrameWithError(fmt.Errorf("Range(): first index out of range (%d > %d)", first, df.Len()-1))
	} else if last >= df.Len() {
		return dataFrameWithError(fmt.Errorf("Range(): last index out of range (%d > %d)", last, df.Len()-1))
	}
	retVals := make([]*valueContainer, len(df.values))
	for k := range df.values {
		retVals[k] = df.values[k].rangeSlice(first, last)
	}
	retLabels := make([]*valueContainer, df.numLevels())
	for j := range df.labels {
		retLabels[j] = df.labels[j].rangeSlice(first, last)
	}
	return &DataFrame{values: retVals, labels: retLabels, name: df.name}
}

// Valid returns rows with all non-null values.
// If `subset` is supplied, returns all the rows with all non-null values in the specified columns.
// Returns a new DataFrame.
func (df *DataFrame) Valid(subset ...string) *DataFrame {
	var index []int
	if len(subset) == 0 {
		index = makeIntRange(0, len(df.values))
	} else {
		for _, name := range subset {
			i, err := findColWithName(name, df.values)
			if err != nil {
				return dataFrameWithError(fmt.Errorf("Valid(): %v", err))
			}
			index = append(index, i)
		}

	}

	subIndexes := make([][]int, len(index))
	for k := range index {
		subIndexes[k] = df.values[k].valid()
	}
	allValid := intersection(subIndexes)
	return df.Subset(allValid)
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
			i, err := findColWithName(name, df.values)
			if err != nil {
				return dataFrameWithError(fmt.Errorf("Valid(): %v", err))
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

// WithRow stub
func (df *DataFrame) WithRow(label string, values []interface{}) *DataFrame {
	return nil
}

// DropCol drops the first column matching `name`
// Returns a new DataFrame.
func (df *DataFrame) DropCol(name string) *DataFrame {
	df.Copy()
	df.InPlace().DropCol(name)
	return df
}

// DropCol drops the first column matching `name`
func (df *DataFrameMutator) DropCol(name string) {
	toExclude, err := findColWithName(name, df.dataframe.values)
	if err != nil {
		df.dataframe.resetWithError(fmt.Errorf("DropCol(): %v", err))
	}
	index := excludeFromIndex(len(df.dataframe.values), toExclude)
	df.SubsetCols(index)
	return
}

// Drop removes the row at the specified index.
// Returns a new DataFrame.
func (df *DataFrame) Drop(index int) *DataFrame {
	df.Copy()
	df.InPlace().Drop(index)
	return df
}

// Drop removes the row at the specified index.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Drop(index int) {
	for k := range df.dataframe.values {
		err := df.dataframe.values[k].dropRow(index)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("Drop(): %v", err))
			return
		}
	}
	for j := range df.dataframe.labels {
		df.dataframe.labels[j].dropRow(index)
	}
	return
}

// SetLabels removes the row at the specified index.
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
	}
	for i := 0; i < len(colNames); i++ {
		index, err := findColWithName(colNames[i], df.dataframe.values)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("SetLabels(): %v", err))
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
func (df *DataFrameMutator) ResetLabels(index ...int) {
	if len(index) == 0 {
		index = makeIntRange(0, df.dataframe.numLevels())
	}
	for _, i := range index {
		if i >= df.dataframe.numLevels() {
			df.dataframe.resetWithError(fmt.Errorf("ResetLabels(): index out of range (%d > %d)", i, df.dataframe.numLevels()-1))
		}
		newVal := df.dataframe.labels[i]
		newVal.name = regexp.MustCompile(`^\*`).ReplaceAllString(newVal.name, "")
		df.dataframe.values = append(df.dataframe.values, newVal)
		df.dataframe.labels, _ = subsetCols(df.dataframe.labels, excludeFromIndex(df.dataframe.numLevels(), i))
	}
	if df.dataframe.numLevels() == 0 {
		labels, isNull := makeDefaultLabels(0, df.dataframe.Len())
		df.dataframe.labels[0] = &valueContainer{slice: labels, isNull: isNull, name: "*0"}
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

// SetCols sets the names of all the columns in the DataFrame and returns the entire DataFrame.
func (df *DataFrame) SetCols(colNames []string) *DataFrame {
	if len(colNames) != len(df.values) {
		return dataFrameWithError(
			fmt.Errorf("SetCols(): number of colNames must match number of columns in DataFrame (%d != %d)", len(colNames), len(df.values)))
	}
	for k := range colNames {
		df.values[k].name = colNames[k]
	}
	return df
}

// reshape

func (df *DataFrame) numColLevels() int {
	return len(splitLabelIntoLevels(df.values[0].name))
}

func (df *DataFrame) numColumns() int {
	return len(df.values)
}

// Transpose stub
func (df *DataFrame) Transpose() *DataFrame {
	// label level names become column level names -- not implemented

	// row values become column values: 2 row x 1 col -> 2 col x 1 row
	vals := make([][]string, df.Len())
	valsIsNull := make([][]bool, df.Len())
	// each new column has the same number of rows as prior columns
	for k := range vals {
		vals[k] = make([]string, df.numColumns())
		valsIsNull[k] = make([]bool, df.numColumns())
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

	// each new label level has same number of rows as prior columns
	for j := range labels {
		labels[j] = make([]string, df.numColumns())
		labelsIsNull[j] = make([]bool, df.numColumns())
	}

	// iterate over labels
	for j := range df.labels {
		v := df.labels[j].str().slice
		for i := range v {
			colNames[i][j] = v[i]
		}
	}
	// iterate over columns
	for k := range df.values {
		splitColName := splitLabelIntoLevels(df.values[k].name)
		for l := range splitColName {
			labels[l][k] = splitColName[l]
			labelsIsNull[l][k] = false
		}
		// save label level name -- not implemented
		v := df.values[k].str().slice
		for i := range v {
			vals[i][k] = v[i]
			valsIsNull[i][k] = df.values[k].isNull[i]
		}
	}
	// transfer to valueContainers
	retLabels := make([]*valueContainer, df.numColLevels())
	retVals := make([]*valueContainer, df.Len())
	// labels
	for j := range labels {
		retLabels[j] = &valueContainer{
			slice:  labels[j],
			isNull: labelsIsNull[j],
			// append label name -- not implemented
		}
	}
	// values
	for k := range retVals {
		retVals[k] = &valueContainer{
			slice:  vals[k],
			isNull: valsIsNull[k],
			name:   joinLevelsIntoLabel(colNames[k]),
		}
	}
	return &DataFrame{
		values: retVals,
		labels: retLabels,
		name:   df.name,
	}
}

// PromoteCol stub
func (df *DataFrame) PromoteCol(name string) *DataFrame {
	return nil
}

// LabelToCol stub
func (df *DataFrame) LabelToCol(label string) *DataFrame {
	return nil
}

// ColToLabel stub
func (df *DataFrame) ColToLabel(name string) *DataFrame {
	return nil
}

// -- FILTERS

// Filter stub
func (df *DataFrame) Filter(filters ...FilterFn) []int {
	if len(filters) == 0 {
		return makeIntRange(0, df.Len())
	}
	// subIndexes contains the index positions computed across all the filters
	var subIndexes [][]int
	for _, filter := range filters {
		// if ColName is empty, apply filter to all columns
		if filter.ColName == "" {
			var dfWideSubIndexes [][]int
			for k := range df.values {
				subIndex, err := df.values[k].filter(filter)
				if err != nil {
					return []int{-999}
				}
				dfWideSubIndexes = append(dfWideSubIndexes, subIndex)
			}
			subIndexes = append(subIndexes, intersection(dfWideSubIndexes))
			continue
		}
		// if ColName is not empty, find name in either columns or labels
		var data *valueContainer
		index, isCol, err := findNameInColumnsOrLabels(filter.ColName, df.values, df.labels)
		// could not match on either columns or labels
		if err != nil {
			return []int{-999}
		}
		if isCol {
			data = df.values[index]
		} else {
			data = df.labels[index]
		}

		subIndex, err := data.filter(filter)
		if err != nil {
			return []int{-999}
		}
		subIndexes = append(subIndexes, subIndex)
	}
	// reduce the subindexes to a single index that shares all the values
	index := intersection(subIndexes)
	return index
}

// -- APPLY

// Apply applies a user-defined `lambda` function to every row in a particular column and coerces all values to match the lambda type.
// Apply may be applied to any label level or column by specifying a ColName in `lambda`.
// If no ColName is specified in `lambda`, the function is applied to every column.
// If a value is considered null either prior to or after the lambda function is applied, it is considered null after.
// Returns a new DataFrame.
func (df *DataFrame) Apply(lambda ApplyFn) *DataFrame {
	df.Copy()
	df.InPlace().Apply(lambda)
	return df
}

// Apply applies a user-defined `lambda` function to every row in a particular column and coerces all values to match the lambda type.
// Apply may be applied to any label level or column by specifying a ColName in `lambda`.
// If no ColName is specified in `lambda`, the function is applied to every column.
// If a value is considered null either prior to or after the lambda function is applied, it is considered null after.
// Modifies the underlying DataFrame in place.
func (df *DataFrameMutator) Apply(lambda ApplyFn) {
	err := lambda.validate()
	if err != nil {
		df.dataframe.resetWithError((fmt.Errorf("Apply(): %v", err)))
		return
	}
	// if ColName is empty, apply lambda to all columns
	if lambda.ColName == "" {
		for k := range df.dataframe.values {
			df.dataframe.values[k].slice = df.dataframe.values[k].apply(lambda)
			df.dataframe.values[k].isNull = isEitherNull(
				df.dataframe.values[k].isNull,
				setNullsFromInterface(df.dataframe.values[k].slice))
		}
	} else {
		// if ColName is not empty, find name in either columns or labels
		index, isCol, err := findNameInColumnsOrLabels(lambda.ColName, df.dataframe.values, df.dataframe.labels)
		if err != nil {
			df.dataframe.resetWithError((fmt.Errorf("Apply(): %v", err)))
		}
		// apply to col
		if isCol {
			df.dataframe.values[index].slice = df.dataframe.values[index].apply(lambda)
			df.dataframe.values[index].isNull = isEitherNull(
				df.dataframe.values[index].isNull,
				setNullsFromInterface(df.dataframe.values[index].slice))
			//apply to label level
		} else {
			df.dataframe.labels[index].slice = df.dataframe.labels[index].apply(lambda)
			df.dataframe.labels[index].isNull = isEitherNull(
				df.dataframe.labels[index].isNull,
				setNullsFromInterface(df.dataframe.labels[index].slice))
		}
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

// Merge stub
func (df *DataFrameMutator) Merge(other *DataFrame) {
	lookupDF := df.dataframe.Lookup(other, "left", nil, nil)
	for k := range lookupDF.values {
		df.dataframe.values = append(df.dataframe.values, lookupDF.values[k])
	}
}

// Lookup stub
func (df *DataFrame) Lookup(other *DataFrame, how string, leftOn []string, rightOn []string) *DataFrame {
	mergedLabelsAndCols := append(df.labels, df.values...)
	otherMergedLabelsAndCols := append(other.labels, other.values...)
	var leftKeys, rightKeys []int
	var err error
	if len(leftOn) == 0 || len(rightOn) == 0 {
		if !(len(leftOn) == 0 && len(rightOn) == 0) {
			return dataFrameWithError(
				fmt.Errorf("Lookup(): if either leftOn or rightOn is empty, both must be empty"))
		}
	}
	if len(leftOn) == 0 {
		leftKeys, rightKeys = findMatchingKeysBetweenTwoLabelContainers(
			mergedLabelsAndCols, otherMergedLabelsAndCols)
	} else {
		leftKeys, err = convertColNamesToIndexPositions(leftOn, mergedLabelsAndCols)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("Lookup(): %v", err))
		}
		rightKeys, err = convertColNamesToIndexPositions(rightOn, otherMergedLabelsAndCols)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("Lookup(): %v", err))
		}
	}
	ret, err := lookupDataFrame(
		how, df.name, df.values, df.labels, leftKeys,
		other.values, other.labels, rightKeys, leftOn, rightOn)
	if err != nil {
		return dataFrameWithError(fmt.Errorf("Lookup(): %v", err))
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
	// original index
	index := makeIntRange(0, df.dataframe.Len())
	var vals *valueContainer
	// no Sorters supplied -> error
	if len(by) == 0 {
		df.dataframe.resetWithError(fmt.Errorf(
			"Sort(): must supply at least one Sorter"))
		return
	}
	for i := len(by) - 1; i >= 0; i-- {
		// Sorter with empty ColName -> error
		if by[i].ColName == "" {
			df.dataframe.resetWithError(fmt.Errorf(
				"Sort(): Sorter (position %d) does not have ColName", i))
			return
		}
		colPosition, isCol, err := findNameInColumnsOrLabels(by[i].ColName, df.dataframe.values, df.dataframe.labels)
		if err != nil {
			df.dataframe.resetWithError(fmt.Errorf("Sort(): %v", err))
			return
		}
		if isCol {
			vals = df.dataframe.values[colPosition].copy()
		} else {
			vals = df.dataframe.labels[colPosition].copy()

		}
		// overwrite index with new index
		index = vals.sort(by[i].DType, by[i].Descending, index)
	}
	df.Subset(index)
}

// -- GROUPERS

// GroupBy stub
// includes label levels and columns
func (df *DataFrame) GroupBy(names ...string) GroupedDataFrame {
	var index []int
	var err error
	mergedLabelsAndCols := append(df.labels, df.values...)
	// if no names supplied, group by all label levels
	if len(names) == 0 {
		index = makeIntRange(0, df.numLevels())
	} else {
		index, err = convertColNamesToIndexPositions(names, mergedLabelsAndCols)
		if err != nil {
			return GroupedDataFrame{err: fmt.Errorf("GroupBy(): %v", err)}
		}
	}
	g, _, orderedKeys := labelsToMap(mergedLabelsAndCols, index)
	return GroupedDataFrame{
		groups:      g,
		orderedKeys: orderedKeys,
		df:          df,
	}
}

// PivotTable stub
func (df *DataFrame) PivotTable(labels, columns, values, aggFn string) *DataFrame {
	// group by
	// stack
	// select
	// apply aggfn
	return nil
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
			df.values[k].float().slice,
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
