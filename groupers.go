package tada

import (
	"fmt"
	"reflect"
	"time"
)

// -- support for generics for grouped types
type empty struct{}

func (e empty) float64() float64    { return 0 }
func (e empty) string() string      { return "" }
func (e empty) dateTime() time.Time { return time.Time{} }

// -- GROUPED SERIES

// Err returns the underlying error, if any.
func (g *GroupedSeries) Err() error {
	return g.err
}

func (g *GroupedSeries) String() string {
	return g.Series().String()
}

// GetGroup returns the grouped rows sharing the same group key as a new Series.
func (g *GroupedSeries) GetGroup(group string) *Series {
	for i, key := range g.orderedKeys {
		if key == group {
			return g.series.Subset(g.rowIndices[i])
		}
	}
	return seriesWithError(fmt.Errorf("getting group: group (%v) not in groups", group))
}

// Transform applies lambda to every group and returns the modified values in the same order as the original
// Series values, preserving the original Series labels and named name.
// Each input into each lambda call will be a slice of values that may then be type-asserted by the user.
// The output of each lambda call must be a slice that is the same length as the input.
// All outputs must be of the same type (though this may be a different type than the input).
func (g *GroupedSeries) Transform(name string, lambda func(interface{}) interface{}) *Series {
	vals, err := groupedInterfaceTransformFunc(
		g.series.values.slice, name, g.rowIndices, lambda)
	if err != nil {
		return seriesWithError(fmt.Errorf("transforming grouped Series: %v", err))
	}
	return &Series{
		values: vals,
		labels: copyContainers(g.series.labels),
	}
}

func (g *GroupedSeries) interfaceReduceFunc(name string, fn func(interface{}) interface{}) (*Series, error) {
	var sharedData bool
	if g.series.values.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.series.values.name)
	}
	retVals, err := groupedInterfaceReduceFunc(
		g.series.values.slice, name, g.aligned, g.rowIndices, fn)
	if err != nil {
		return nil, err
	}
	// default: grouped labels
	retLabels := g.labels
	if g.aligned {
		// if aligned: all labels
		retLabels = g.series.labels
		sharedData = true
	}
	return &Series{
		values:     retVals,
		labels:     retLabels,
		sharedData: sharedData,
	}, nil
}

// for each group, returns the row at the selected index as a new row in a new Series
func (g *GroupedSeries) indexReduceFunc(name string, index int) *Series {
	var sharedData bool
	if g.series.values.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.series.values.name)
	}
	retVals := groupedIndexReduceFunc(
		g.series.values.slice, g.series.values.isNull, name, g.aligned, index, g.rowIndices)
	// default: grouped labels
	retLabels := g.labels
	if g.aligned {
		// if aligned: all labels
		retLabels = g.series.labels
		sharedData = true
	}
	return &Series{
		values:     retVals,
		labels:     retLabels,
		sharedData: sharedData,
	}
}

// for each group, returns a count
func (g *GroupedSeries) countReduceFunc(name string, fn func(interface{}, []bool, []int) (int, bool)) *Series {
	var sharedData bool
	if g.series.values.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.series.values.name)
	}
	retVals := groupedCountReduceFunc(g.series.values.slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)

	retLabels := g.labels
	if g.aligned {
		// if aligned: all labels
		retLabels = g.series.labels
		sharedData = true
	}
	return &Series{
		values:     retVals,
		labels:     retLabels,
		sharedData: sharedData,
	}
}

// Reduce iterates over the groups in g and reduces each group of values into a single value
// using the function supplied in lambda.
// Reduce returns a new Series named "name_originalColName" where each reduced group is represented by a single row.
// The reduction lambda function is the first field selected (i.e., not left blank) in the GroupReduceFn.
func (g *GroupedSeries) Reduce(name string, lambda GroupReduceFn) *Series {
	// remove all nulls before running each set of values through custom user function
	if lambda.Float64 != nil {
		fn := convertSimplifiedFloat64ReduceFunc(lambda.Float64)
		return g.float64ReduceFunc(name, fn)
	} else if lambda.String != nil {
		fn := convertSimplifiedStringReduceFunc(lambda.String)
		return g.stringReduceFunc(name, fn)
	} else if lambda.DateTime != nil {
		fn := convertSimplifiedDateTimeReduceFunc(lambda.DateTime)
		return g.dateTimeReduceFunc(name, fn)
	} else if lambda.Interface != nil {
		newSeries, err := g.interfaceReduceFunc(name, lambda.Interface)
		if err != nil {
			return seriesWithError(fmt.Errorf("reducing grouped Series: %v", err))
		}
		return newSeries
	}
	return seriesWithError(fmt.Errorf("reducing grouped Series: no lambda function provided"))
}

// Sum coerces values to float64 and calculates the sum of each group.
func (g *GroupedSeries) Sum() *Series {
	return g.float64ReduceFunc("sum", sum)
}

// Mean coerces values to float64 and calculates the mean of each group.
func (g *GroupedSeries) Mean() *Series {
	return g.float64ReduceFunc("mean", mean)
}

// Median coerces values to float64 and calculates the median of each group.
func (g *GroupedSeries) Median() *Series {
	return g.float64ReduceFunc("median", median)
}

// Std coerces values to float64 and calculates the standard deviation of each group.
func (g *GroupedSeries) Std() *Series {
	return g.float64ReduceFunc("std", std)
}

// Count returns the number of non-null values in each group.
func (g *GroupedSeries) Count() *Series {
	return g.countReduceFunc("count", count)
}

// NUnique returns the number of unique values in each group.
func (g *GroupedSeries) NUnique() *Series {
	return g.countReduceFunc("nunique", nunique)
}

// Min coerces values to float64 and calculates the minimum of each group.
func (g *GroupedSeries) Min() *Series {
	return g.float64ReduceFunc("min", min)
}

// Max coerces values to float64 and calculates the maximum of each group.
func (g *GroupedSeries) Max() *Series {
	return g.float64ReduceFunc("max", max)
}

// Earliest coerces the Series values to time.Time and calculates the earliest timestamp in each group.
func (g *GroupedSeries) Earliest() *Series {
	return g.dateTimeReduceFunc("earliest", earliest)
}

// Latest coerces the Series values to time.Time and calculates the latest timestamp in each group.
func (g *GroupedSeries) Latest() *Series {
	return g.dateTimeReduceFunc("latest", latest)
}

// Nth returns the row at position n (if it exists) within each group.
func (g *GroupedSeries) Nth(n int) *Series {
	return g.indexReduceFunc("nth", n)
}

// First returns the first row in each group.
func (g *GroupedSeries) First() *Series {
	return g.indexReduceFunc("first", 0)
}

// Last returns the last row in each group.
func (g *GroupedSeries) Last() *Series {
	return g.indexReduceFunc("last", -1)
}

// Align changes subsequent reduce operations for this group to return a Series aligned with the original Series labels
// (the default behavior is to return a Series with one label per group).
// If the original Series is:
//
// FOO
// baz 0
// baz 1
// bar 2
// bar 4
//
// and it is grouped by the "foo" label, then the default g.Sum() reducer would return:
//
// FOO
// baz 1
// bar 6
//
// After g.Align(), the g.Sum() reducer would return:
//
// FOO
// baz 1
// baz 1
// bar 6
// bar 6
func (g *GroupedSeries) Align() *GroupedSeries {
	g.aligned = true
	return g
}

// HavingCount removes any groups from g that do not satisfy the boolean function supplied in lambda.
// For each group, the input into lambda is the total number of values in the group (null or not-null).
func (g *GroupedSeries) HavingCount(lambda func(int) bool) *GroupedSeries {
	indexToKeep := make([]int, 0)
	retRowIndices := make([][]int, 0)
	retOrderedKeys := make([]string, 0)
	for i, index := range g.rowIndices {
		if lambda(len(index)) {
			indexToKeep = append(indexToKeep, i)
			retRowIndices = append(retRowIndices, index)
			retOrderedKeys = append(retOrderedKeys, g.orderedKeys[i])
		}
	}
	labels := copyContainers(g.labels)
	subsetContainerRows(labels, indexToKeep)

	return &GroupedSeries{
		orderedKeys: retOrderedKeys,
		rowIndices:  retRowIndices,
		labels:      labels,
		series:      g.series,
		aligned:     g.aligned,
	}
}

// Series returns the GroupedSeries as a Series, sorted by group, with groups in the order in which they appear in the original Series.
func (g *GroupedSeries) Series() *Series {
	index := make([]int, g.series.Len())
	var counter int
	for _, group := range g.rowIndices {
		for _, i := range group {
			index[counter] = i
			counter++
		}
	}
	s := g.series.Subset(index).Copy()
	s.sharedData = false
	return s
}

// RollingN iterates over each row in Series and groups each set of n subsequent rows after the current row.
func (s *Series) RollingN(n int) *GroupedSeries {
	if n < 1 {
		return groupedSeriesWithError(fmt.Errorf("rolling n: n must be greater than zero (not %v)", n))
	}
	rowIndices := make([][]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		var rowIndex []int
		if i+n <= s.Len() {
			rowIndex = makeIntRange(i, i+n)
		} else {
			rowIndex = make([]int, 0)
		}
		rowIndices[i] = rowIndex
	}
	return &GroupedSeries{
		rowIndices: rowIndices,
		series:     s,
		aligned:    true,
	}
}

// RollingDuration iterates over each row in Series, coerces the values to time.Time, and groups each set of subsequent rows that are within d of the current row.
func (s *Series) RollingDuration(d time.Duration) *GroupedSeries {
	// assumes positive duration
	if d < 0 {
		return groupedSeriesWithError(fmt.Errorf("rolling duration: d must be greater than zero (not %v)", d))
	}
	vals := s.values.dateTime().slice
	rowIndices := make([][]int, s.Len())
	for i := 0; i < s.Len(); i++ {
		eligibleRows := []int{i}
		nextRow := i + 1
		for {
			if nextRow < s.Len() {
				if withinWindow(vals[i], vals[nextRow], d) {
					eligibleRows = append(eligibleRows, nextRow)
					nextRow++
					continue
				}
			}
			// keep appending nextRow until it is not within duration or the end of series
			break
		}
		rowIndices[i] = eligibleRows
	}
	return &GroupedSeries{
		rowIndices: rowIndices,
		series:     s,
		aligned:    true,
	}
}

// Next advances to next grouped Series. Returns false at end of iteration.
func (g *GroupedSeriesIterator) Next() bool {
	g.current++
	return g.current < len(g.rowIndices)
}

// Series returns the current grouped Series.
func (g *GroupedSeriesIterator) Series() *Series {
	return g.s.Subset(g.rowIndices[g.current])
}

// Iterator returns an iterator which may be used to access each group of rows as a new Series, in the order in which the groups originally appeared.
func (g *GroupedSeries) Iterator() *GroupedSeriesIterator {
	return &GroupedSeriesIterator{
		current:    -1,
		rowIndices: g.rowIndices,
		s:          g.series,
	}
}

// Len returns the number of group labels.
func (g *GroupedSeries) Len() int {
	return len(g.rowIndices)
}

// ListGroups returns a list of group keys in the order in which they originally appeared.
func (g *GroupedSeries) ListGroups() []string {
	return g.orderedKeys
}

// GetLabels returns the grouped label levels as interface{} slices within an []interface returns the group's labels as slices within an []interface
// that may be supplied as optional labels argument to NewSeries() or NewDataFrame().
func (g *GroupedSeries) GetLabels() []interface{} {
	var ret []interface{}
	labels := copyContainers(g.labels)
	for j := range labels {
		ret = append(ret, labels[j].slice)
	}
	return ret
}

// -- GROUPED DATAFRAME

// Err returns the underlying error, if any
func (g *GroupedDataFrame) Err() error {
	return g.err
}

func (g *GroupedDataFrame) String() string {
	return g.DataFrame().String()
}

func (g *GroupedDataFrame) indexReduceFunc(name string, cols []string, index int) *DataFrame {
	if len(cols) == 0 {
		cols = g.df.ListColNames()
	}
	adjustedColNames := make([]string, len(cols))
	for k := range cols {
		adjustedColNames[k] = fmt.Sprintf("%v_%v", name, cols[k])
	}
	retVals := make([]*valueContainer, len(cols))
	for k, colName := range cols {
		colIndex, _ := indexOfContainer(colName, g.df.values)
		retVals[k] = groupedIndexReduceFunc(
			g.df.values[colIndex].slice, g.df.values[k].isNull, adjustedColNames[k], false, index, g.rowIndices)
	}
	if g.df.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.df.name)
	}
	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}

func (g *GroupedDataFrame) interfaceReduceFunc(name string, cols []string, fn func(interface{}) interface{}) (*DataFrame, error) {
	if len(cols) == 0 {
		cols = g.df.ListColNames()
	}
	adjustedColNames := make([]string, len(cols))
	for k := range cols {
		adjustedColNames[k] = fmt.Sprintf("%v_%v", name, cols[k])
	}
	var err error
	retVals := make([]*valueContainer, len(cols))
	for k, colName := range cols {
		index, _ := indexOfContainer(colName, g.df.values)
		retVals[k], err = groupedInterfaceReduceFunc(
			g.df.values[index].slice, adjustedColNames[k], false, g.rowIndices, fn)
		if err != nil {
			return nil, err
		}
	}
	if g.df.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.df.name)
	}

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}, nil
}

func (g *GroupedDataFrame) countReduceFunc(name string, cols []string, fn func(interface{}, []bool, []int) (int, bool)) *DataFrame {
	if len(cols) == 0 {
		cols = g.df.ListColNames()
	}
	adjustedColNames := make([]string, len(cols))
	for k := range cols {
		adjustedColNames[k] = fmt.Sprintf("%v_%v", name, cols[k])
	}
	retVals := make([]*valueContainer, len(cols))
	for k, colName := range cols {
		index, _ := indexOfContainer(colName, g.df.values)
		retVals[k] = groupedCountReduceFunc(
			g.df.values[index].slice, g.df.values[k].isNull, adjustedColNames[k], false, g.rowIndices, fn)
	}
	if g.df.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.df.name)
	}

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}

// GetGroup returns the grouped rows sharing the same group key as a new DataFrame.
func (g *GroupedDataFrame) GetGroup(group string) *DataFrame {
	for i, key := range g.orderedKeys {
		if key == group {
			return g.df.Subset(g.rowIndices[i])
		}
	}
	return dataFrameWithError(fmt.Errorf("getting group: group (%v) not in groups", group))
}

// Sum coerces the column values in colNames to float64 and calculates the sum of each group.
func (g *GroupedDataFrame) Sum(colNames ...string) *DataFrame {
	return g.float64ReduceFunc("sum", colNames, sum)
}

// Mean coerces the column values in colNames to float64 and calculates the mean of each group.
func (g *GroupedDataFrame) Mean(colNames ...string) *DataFrame {
	return g.float64ReduceFunc("mean", colNames, mean)
}

// Median coerces the column values in colNames to float64 and calculates the median of each group.
func (g *GroupedDataFrame) Median(colNames ...string) *DataFrame {
	return g.float64ReduceFunc("median", colNames, median)
}

// Std coerces the column values in colNames to float64 and calculates the standard deviation of each group.
func (g *GroupedDataFrame) Std(colNames ...string) *DataFrame {
	return g.float64ReduceFunc("std", colNames, std)
}

// Min coerces the column values in colNames to float64 and calculates the minimum of each group.
func (g *GroupedDataFrame) Min(colNames ...string) *DataFrame {
	return g.float64ReduceFunc("min", colNames, min)
}

// Max coerces the column values in colNames to float64 and calculates the maximum of each group.
func (g *GroupedDataFrame) Max(colNames ...string) *DataFrame {
	return g.float64ReduceFunc("max", colNames, max)
}

// Count returns the number of non-null values in each group for the columns in colNames.
func (g *GroupedDataFrame) Count(colNames ...string) *DataFrame {
	return g.countReduceFunc("count", colNames, count)
}

// NUnique returns the number of unique, non-null values in each group for the columns in colNames.
func (g *GroupedDataFrame) NUnique(colNames ...string) *DataFrame {
	return g.countReduceFunc("nunique", colNames, nunique)
}

// Earliest coerces the column values in colNames to time.Time and calculates the earliest timestamp of each group.
func (g *GroupedDataFrame) Earliest(colNames ...string) *DataFrame {
	return g.dateTimeReduceFunc("earliest", colNames, earliest)
}

// Latest coerces the column values in colNames to time.Time and calculates the latest timestamp of each group.
func (g *GroupedDataFrame) Latest(colNames ...string) *DataFrame {
	return g.dateTimeReduceFunc("latest", colNames, latest)
}

// Nth returns the row at position n (if it exists) within each group for the columns in colNames.
func (g *GroupedDataFrame) Nth(index int, colNames ...string) *DataFrame {
	return g.indexReduceFunc("nth", colNames, index)
}

// First returns the first row within each group for the columns in colNames.
func (g *GroupedDataFrame) First(colNames ...string) *DataFrame {
	return g.indexReduceFunc("first", colNames, 0)
}

// Last returns the last row within each group for the columns in colNames.
func (g *GroupedDataFrame) Last(colNames ...string) *DataFrame {
	return g.indexReduceFunc("last", colNames, -1)
}

// Col isolates the Series at containerName, which may be either a label level or column in the underlying DataFrame.
// Returns a GroupedSeries with the same groups and labels as in the GroupedDataFrame.
func (g *GroupedDataFrame) Col(colName string) *GroupedSeries {
	index, err := indexOfContainer(colName, g.df.values)
	if err != nil {
		return groupedSeriesWithError(fmt.Errorf("getting column from grouped Series: %v", err))
	}
	series := &Series{
		values:     g.df.values[index],
		labels:     g.df.labels,
		sharedData: true,
	}
	return &GroupedSeries{
		orderedKeys: g.orderedKeys,
		rowIndices:  g.rowIndices,
		labels:      g.labels,
		series:      series,
	}
}

// Reduce iterates over the groups in g and reduces each group of values into a single value
// applying the function supplied in lambda to the column values in cols.
// Reduce returns a new DataFrame named "name_originalDataFrameName",
// with columns named "name_originalColName",
// where each reduced group is represented by a single row.
// The reduction lambda function is the first field selected (i.e., not left blank) in the GroupReduceFn.
func (g *GroupedDataFrame) Reduce(name string, cols []string, lambda GroupReduceFn) *DataFrame {
	// remove all nulls before running each set of values through custom user function
	if lambda.Float64 != nil {
		fn := convertSimplifiedFloat64ReduceFunc(lambda.Float64)
		return g.float64ReduceFunc(name, cols, fn)
	} else if lambda.String != nil {
		fn := convertSimplifiedStringReduceFunc(lambda.String)
		return g.stringReduceFunc(name, cols, fn)
	} else if lambda.DateTime != nil {
		fn := convertSimplifiedDateTimeReduceFunc(lambda.DateTime)
		return g.dateTimeReduceFunc(name, cols, fn)
	} else if lambda.Interface != nil {
		newDataFrame, err := g.interfaceReduceFunc(name, cols, lambda.Interface)
		if err != nil {
			return dataFrameWithError(fmt.Errorf("reducing grouped DataFrame: %v", err))
		}
		return newDataFrame
	}
	return dataFrameWithError(fmt.Errorf("reducing grouped DataFrame: no lambda function provided"))
}

// HavingCount removes any groups from g that do not satisfy the boolean function supplied in lambda.
// For each group, the input into lambda is the total number of values in the group (null or not-null).
func (g *GroupedDataFrame) HavingCount(lambda func(int) bool) *GroupedDataFrame {
	indexToKeep := make([]int, 0)
	retRowIndices := make([][]int, 0)
	retOrderedKeys := make([]string, 0)
	for i, index := range g.rowIndices {
		if lambda(len(index)) {
			indexToKeep = append(indexToKeep, i)
			retRowIndices = append(retRowIndices, index)
			retOrderedKeys = append(retOrderedKeys, g.orderedKeys[i])
		}
	}
	labels := copyContainers(g.labels)
	subsetContainerRows(labels, indexToKeep)

	return &GroupedDataFrame{
		orderedKeys: retOrderedKeys,
		rowIndices:  retRowIndices,
		labels:      labels,
		df:          g.df,
	}
}

// DataFrame returns the GroupedDataFrame as a DataFrame, sorted by group, with groups in the order in which they appear in the original DataFrame.
func (g *GroupedDataFrame) DataFrame() *DataFrame {
	index := make([]int, g.df.Len())
	var counter int
	for _, group := range g.rowIndices {
		for _, i := range group {
			index[counter] = i
			counter++
		}
	}
	return g.df.Subset(index).Copy()
}

// Next advances to next grouped DataFrame. Returns false at end of iteration.
func (g *GroupedDataFrameIterator) Next() bool {
	g.current++
	return g.current < len(g.rowIndices)
}

// DataFrame returns the current grouped DataFrame.
func (g *GroupedDataFrameIterator) DataFrame() *DataFrame {
	return g.df.Subset(g.rowIndices[g.current])
}

// Iterator returns an iterator which may be used to access each group of rows as a new DataFrame, in the order in which the groups originally appeared.
func (g *GroupedDataFrame) Iterator() *GroupedDataFrameIterator {
	return &GroupedDataFrameIterator{
		current:    -1,
		rowIndices: g.rowIndices,
		df:         g.df,
	}
}

// ListGroups returns a list of group keys in the order in which they originally appeared.
func (g *GroupedDataFrame) ListGroups() []string {
	return g.orderedKeys
}

// GetLabels returns the grouped label levels as interface{} slices within an []interface
// that may be supplied as optional labels argument to NewSeries() or NewDataFrame().
func (g *GroupedDataFrame) GetLabels() []interface{} {
	var ret []interface{}
	labels := copyContainers(g.labels)
	for j := range g.labels {
		ret = append(ret, labels[j].slice)
	}
	return ret
}

// Len returns the number of group labels.
func (g *GroupedDataFrame) Len() int {
	return len(g.rowIndices)
}

// -- SPECIAL REDUCE FUNCTIONS

func groupedInterfaceReduceFunc(
	slice interface{},
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func(slice interface{}) interface{}) (*valueContainer, error) {

	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	v := reflect.ValueOf(slice)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = v.Len()
	}
	// must deduce output type
	sampleRows := subsetInterfaceSlice(slice, rowIndices[0])
	sampleOutput := fn(sampleRows)

	retVals := reflect.MakeSlice(reflect.SliceOf(reflect.TypeOf(sampleOutput)), retLength, retLength)
	for i, rowIndex := range rowIndices {
		subsetRows := subsetInterfaceSlice(slice, rowIndex)
		output := fn(subsetRows)

		src := reflect.ValueOf(output)
		if !aligned {
			// default: write each output once and in sequential order into retVals
			dst := retVals.Index(i)
			dst.Set(src)
		} else {
			// if aligned: write each output multiple times and out of order into retVals
			for _, index := range rowIndex {
				dst := retVals.Index(index)
				dst.Set(src)
			}
		}
	}
	ret, err := makeValueContainerFromInterface(retVals.Interface(), name)
	if err != nil {
		return nil, fmt.Errorf("interface{} output: %v", err)
	}
	return ret, nil
}

// transforms each original Series row and maintains their order, regardless of whether groupedSeries is aligned
func groupedInterfaceTransformFunc(
	slice interface{},
	name string,
	rowIndices [][]int,
	fn func(slice interface{}) interface{}) (*valueContainer, error) {

	// default: return length is equal to the number of groups
	retLength := reflect.ValueOf(slice).Len()

	// must deduce output type
	sampleRows := subsetInterfaceSlice(slice, rowIndices[0])
	sampleOutput := fn(sampleRows)
	if !isSlice(sampleOutput) {
		return nil, fmt.Errorf("group 0: output must be slice (%v != slice)",
			reflect.TypeOf(sampleOutput).Kind())
	}

	retVals := reflect.MakeSlice(reflect.TypeOf(sampleOutput), retLength, retLength)
	for i, rowIndex := range rowIndices {
		subsetRows := subsetInterfaceSlice(slice, rowIndex)
		output := fn(subsetRows)

		if !isSlice(output) {
			return nil, fmt.Errorf("group %d: output must be slice (%v != slice)",
				i, reflect.TypeOf(output).Kind())
		}
		if reflect.ValueOf(output).Len() != reflect.ValueOf(subsetRows).Len() {
			return nil, fmt.Errorf("group %d: length of output slice must match length of input slice "+
				"(%d != %d)", i, reflect.ValueOf(output).Len(), reflect.ValueOf(subsetRows).Len())
		}
		// write each output multiple times and out of order into retVals
		for incrementor, index := range rowIndex {
			src := reflect.ValueOf(output).Index(incrementor)
			dst := retVals.Index(index)
			dst.Set(src)
		}
	}
	ret, err := makeValueContainerFromInterface(retVals.Interface(), name)
	if err != nil {
		return nil, fmt.Errorf("interface{} output: %v", err)
	}
	return ret, nil
}

func groupedIndexReduceFunc(
	vals interface{},
	nulls []bool,
	name string,
	aligned bool,
	index int,
	rowIndices [][]int) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	v := reflect.ValueOf(vals)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = v.Len()
	}

	retVals := reflect.MakeSlice(v.Type(), retLength, retLength)
	retNulls := make([]bool, retLength)
	for i, rowIndex := range rowIndices {
		// modify index if negative
		modifiedIndex := index
		if index < 0 {
			// if original index is negative, try to index from right-to-left
			modifiedIndex = len(rowIndex) + index
		}
		if modifiedIndex >= len(rowIndex) || modifiedIndex < 0 {
			retNulls[i] = true
			continue
		}
		// look up the row position contained at modifiedIndex
		toLookup := rowIndex[modifiedIndex]
		output, isNull := v.Index(toLookup), nulls[toLookup]
		if !aligned {
			// default: write each output once and in sequential order
			retVals.Index(i).Set(output)
			retNulls[i] = isNull
		} else {
			// if aligned: write each output multiple times and out of order
			for _, i := range rowIndex {
				retVals.Index(i).Set(output)
				retNulls[modifiedIndex] = isNull
			}
		}
	}
	return &valueContainer{
		slice:  retVals.Interface(),
		isNull: retNulls,
		name:   name,
	}
}

func groupedCountReduceFunc(slice interface{}, nulls []bool, name string, aligned bool, rowIndices [][]int,
	fn func(interface{}, []bool, []int) (int, bool)) *valueContainer {
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(nulls)
	}
	retVals := make([]int, retLength)
	retNulls := make([]bool, retLength)
	for i, rowIndex := range rowIndices {
		output, isNull := fn(slice, nulls, rowIndex)
		if !aligned {
			// default: write each output once and in sequential order into retVals
			retVals[i] = output
			retNulls[i] = isNull
		} else {
			// if aligned: write each output multiple times and out of order into retVals
			for _, index := range rowIndex {
				retVals[index] = output
				retNulls[index] = isNull
			}
		}
	}
	return &valueContainer{
		slice:  retVals,
		isNull: retNulls,
		name:   name,
	}
}
