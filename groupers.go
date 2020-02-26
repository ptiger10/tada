package tada

import (
	"fmt"
	"reflect"
	"strings"
	"time"
)

// GroupedSeries

// Err returns the underlying error, if any
func (g *GroupedSeries) Err() error {
	return g.err
}

func (g *GroupedSeries) String() string {
	groups := make([]string, len(g.orderedKeys))
	for i, k := range g.orderedKeys {
		groups[i] = k
	}
	return "Groups: " + strings.Join(groups, ",")
}

// GetGroup stub
func (g *GroupedSeries) GetGroup(group string) *Series {
	for m, key := range g.orderedKeys {
		if key == group {
			return g.series.Subset(g.rowIndices[m])
		}
	}
	return seriesWithError(fmt.Errorf("GetGroup(): `group` (%v) not in groups", group))
}

func groupedFloatFunc(
	vals []float64,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func(val []float64, isNull []bool, index []int) (float64, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(vals)
	}
	retVals := make([]float64, retLength)
	retNulls := make([]bool, retLength)
	for i, rowIndex := range rowIndices {
		output, isNull := fn(vals, nulls, rowIndex)
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

func groupedStringFunc(
	vals []string,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func(val []string, isNull []bool, index []int) (string, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(vals)
	}
	retVals := make([]string, retLength)
	retNulls := make([]bool, retLength)
	for i, rowIndex := range rowIndices {
		output, isNull := fn(vals, nulls, rowIndex)
		if !aligned {
			// default: write each output once and in sequential order
			retVals[i] = output
			retNulls[i] = isNull
		} else {
			// if aligned: write each output multiple times and out of order
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

func groupedDateTimeFunc(
	vals []time.Time,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func(val []time.Time, isNull []bool, index []int) (time.Time, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(vals)
	}
	retVals := make([]time.Time, retLength)
	retNulls := make([]bool, retLength)
	for i, rowIndex := range rowIndices {
		output, isNull := fn(vals, nulls, rowIndex)
		if !aligned {
			// default: write each output once and in sequential order
			retVals[i] = output
			retNulls[i] = isNull
		} else {
			// if aligned: write each output multiple times and out of order
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

func (g *GroupedSeries) floatFunc(name string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *Series {
	var sharedData bool
	if g.aligned {
		name = fmt.Sprintf("%v_%v", g.series.values.name, name)
	}
	retVals := groupedFloatFunc(
		g.series.values.float().slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)
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

func groupedIndexFunc(
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
		// calculate last index position on the fly
		modifiedIndex := index
		if index < 0 {
			// if original index is negative, try to index from right-to-left
			modifiedIndex = len(rowIndex) + index
		}
		if modifiedIndex >= len(rowIndex) || modifiedIndex < 0 {
			retNulls[i] = true
			continue
		}
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

func (g *GroupedSeries) indexFunc(name string, index int) *Series {
	var sharedData bool
	if g.aligned {
		name = fmt.Sprintf("%v_%v", g.series.values.name, name)
	}
	retVals := groupedIndexFunc(
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

// Nth stub
func (g *GroupedSeries) Nth(index int) *Series {
	return g.indexFunc("nth", index)
}

// First stub
func (g *GroupedSeries) First() *Series {
	return g.indexFunc("first", 0)
}

// Last stub
func (g *GroupedSeries) Last() *Series {
	return g.indexFunc("last", -1)
}

func (g *GroupedSeries) stringFunc(name string, fn func(val []string, isNull []bool, index []int) (string, bool)) *Series {
	var sharedData bool
	if g.aligned {
		name = fmt.Sprintf("%v_%v", g.series.values.name, name)
	}
	retVals := groupedStringFunc(
		g.series.values.str().slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)
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

func (g *GroupedSeries) dateTimeFunc(name string, fn func(val []time.Time, isNull []bool, index []int) (time.Time, bool)) *Series {
	var sharedData bool
	if g.aligned {
		name = fmt.Sprintf("%v_%v", g.series.values.name, name)
	}
	retVals := groupedDateTimeFunc(
		g.series.values.dateTime().slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)
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

// Apply stub
func (g *GroupedSeries) Apply(name string, lambda GroupApplyFn) *Series {
	// remove all nulls before running each set of values through custom user function
	if lambda.F64 != nil {
		fn := convertUserFloatFunc(lambda.F64)
		return g.floatFunc(name, fn)
	} else if lambda.String != nil {
		fn := convertUserStringFunc(lambda.String)
		return g.stringFunc(name, fn)
	} else if lambda.DateTime != nil {
		fn := convertUserDateTimeFunc(lambda.DateTime)
		return g.dateTimeFunc(name, fn)
	}
	return seriesWithError(fmt.Errorf("Apply(): no lambda function provided"))
}

// Sum stub
func (g *GroupedSeries) Sum() *Series {
	return g.floatFunc("sum", sum)
}

// Mean stub
func (g *GroupedSeries) Mean() *Series {
	return g.floatFunc("mean", mean)
}

// Median stub
func (g *GroupedSeries) Median() *Series {
	return g.floatFunc("median", median)
}

// Std stub
func (g *GroupedSeries) Std() *Series {
	return g.floatFunc("std", std)
}

// Count stub
func (g *GroupedSeries) Count() *Series {
	return g.floatFunc("count", count)
}

// Min stub
func (g *GroupedSeries) Min() *Series {
	return g.floatFunc("min", min)
}

// Max stub
func (g *GroupedSeries) Max() *Series {
	return g.floatFunc("max", max)
}

// NUnique stub
func (g *GroupedSeries) NUnique() *Series {
	return g.stringFunc("nunique", nunique)
}

// Earliest stub
func (g *GroupedSeries) Earliest() *Series {
	return g.dateTimeFunc("earliest", earliest)
}

// Latest stub
func (g *GroupedSeries) Latest() *Series {
	return g.dateTimeFunc("latest", latest)
}

// Err returns the underlying error, if any
func (g *GroupedDataFrame) Err() error {
	return g.err
}

func (g *GroupedDataFrame) floatFunc(
	name string, cols []string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *DataFrame {
	if len(cols) == 0 {
		cols = make([]string, len(g.df.values))
		for k := range cols {
			cols[k] = g.df.values[k].name
		}
	}
	retVals := make([]*valueContainer, len(cols))
	for k := range retVals {
		retVals[k] = groupedFloatFunc(
			g.df.values[k].float().slice, g.df.values[k].isNull, cols[k], false, g.rowIndices, fn)
	}
	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}

func (g *GroupedDataFrame) stringFunc(
	name string, cols []string, fn func(val []string, isNull []bool, index []int) (string, bool)) *DataFrame {
	if len(cols) == 0 {
		cols = make([]string, len(g.df.values))
		for k := range cols {
			cols[k] = g.df.values[k].name
		}
	}
	retVals := make([]*valueContainer, len(cols))
	for k := range retVals {
		retVals[k] = groupedStringFunc(
			g.df.values[k].str().slice, g.df.values[k].isNull, cols[k], false, g.rowIndices, fn)
	}
	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}

func (g *GroupedDataFrame) dateTimeFunc(
	name string, cols []string, fn func(val []time.Time, isNull []bool, index []int) (time.Time, bool)) *DataFrame {
	if len(cols) == 0 {
		cols = make([]string, len(g.df.values))
		for k := range cols {
			cols[k] = g.df.values[k].name
		}
	}
	retVals := make([]*valueContainer, len(cols))
	for k := range retVals {
		retVals[k] = groupedDateTimeFunc(
			g.df.values[k].dateTime().slice, g.df.values[k].isNull, cols[k], false, g.rowIndices, fn)
	}
	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}

func (g *GroupedDataFrame) indexFunc(name string, cols []string, index int) *DataFrame {
	if len(cols) == 0 {
		cols = make([]string, len(g.df.values))
		for k := range cols {
			cols[k] = g.df.values[k].name
		}
	}
	retVals := make([]*valueContainer, len(cols))
	for k := range retVals {
		retVals[k] = groupedIndexFunc(
			g.df.values[k].slice, g.df.values[k].isNull, cols[k], false, index, g.rowIndices)
	}
	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}

// GetGroup stub
func (g *GroupedDataFrame) GetGroup(group string) *DataFrame {
	for m, key := range g.orderedKeys {
		if key == group {
			return g.df.Subset(g.rowIndices[m])
		}
	}
	return dataFrameWithError(fmt.Errorf("GetGroup(): `group` (%v) not in groups", group))
}

// Sum stub
func (g *GroupedDataFrame) Sum(colNames ...string) *DataFrame {
	return g.floatFunc("sum", colNames, sum)
}

// Mean stub
func (g *GroupedDataFrame) Mean(colNames ...string) *DataFrame {
	return g.floatFunc("mean", colNames, mean)
}

// Median stub
func (g *GroupedDataFrame) Median(colNames ...string) *DataFrame {
	return g.floatFunc("median", colNames, median)
}

// Std stub
func (g *GroupedDataFrame) Std(colNames ...string) *DataFrame {
	return g.floatFunc("std", colNames, std)
}

// Count stub
func (g *GroupedDataFrame) Count(colNames ...string) *DataFrame {
	return g.floatFunc("count", colNames, count)
}

// Min stub
func (g *GroupedDataFrame) Min(colNames ...string) *DataFrame {
	return g.floatFunc("min", colNames, min)
}

// Max stub
func (g *GroupedDataFrame) Max(colNames ...string) *DataFrame {
	return g.floatFunc("max", colNames, max)
}

// NUnique stub
func (g *GroupedDataFrame) NUnique(colNames ...string) *DataFrame {
	return g.stringFunc("nunique", colNames, nunique)
}

// Nth stub
func (g *GroupedDataFrame) Nth(index int, colNames ...string) *DataFrame {
	return g.indexFunc("nth", colNames, index)
}

// First stub
func (g *GroupedDataFrame) First(colNames ...string) *DataFrame {
	return g.indexFunc("first", colNames, 0)
}

// Last stub
func (g *GroupedDataFrame) Last(colNames ...string) *DataFrame {
	return g.indexFunc("last", colNames, -1)
}

// Earliest stub
func (g *GroupedDataFrame) Earliest(colNames ...string) *DataFrame {
	return g.dateTimeFunc("earliest", colNames, earliest)
}

// Latest stub
func (g *GroupedDataFrame) Latest(colNames ...string) *DataFrame {
	return g.dateTimeFunc("latest", colNames, latest)
}

// Align stub
func (g *GroupedSeries) Align() *GroupedSeries {
	g.aligned = true
	return g
}

// HavingCount stub
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

// Col isolates the Series at `containerName`, which may be either a label level or column in the underlying DataFrame.
// Returns a GroupedSeries with the same groups and labels as in the GroupedDataFrame.
func (g *GroupedDataFrame) Col(colName string) *GroupedSeries {
	index, err := findContainerWithName(colName, g.df.values)
	if err != nil {
		return &GroupedSeries{
			err: fmt.Errorf("Col(): %v", err),
		}
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

// Apply stub
func (g *GroupedDataFrame) Apply(name string, cols []string, lambda GroupApplyFn) *DataFrame {
	// remove all nulls before running each set of values through custom user function
	if lambda.F64 != nil {
		fn := convertUserFloatFunc(lambda.F64)
		return g.floatFunc(name, cols, fn)
	} else if lambda.String != nil {
		fn := convertUserStringFunc(lambda.String)
		return g.stringFunc(name, cols, fn)
	} else if lambda.DateTime != nil {
		fn := convertUserDateTimeFunc(lambda.DateTime)
		return g.dateTimeFunc(name, cols, fn)
	}
	return dataFrameWithError(fmt.Errorf("Apply(): no lambda function provided"))
}

// RollingN stub
func (s *Series) RollingN(n int) *GroupedSeries {
	if n < 1 {
		return &GroupedSeries{err: fmt.Errorf("RollingN(): `n` must be greater than zero (not %v)", n)}
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

// RollingDuration stub
func (s *Series) RollingDuration(d time.Duration) *GroupedSeries {
	// assumes positive duration
	if d < 0 {
		return &GroupedSeries{err: fmt.Errorf("RollingDuration(): `d` must be greater than zero (not %v)", d)}
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

// IterGroups stub
func (g *GroupedSeries) IterGroups() []*Series {
	ret := make([]*Series, len(g.rowIndices))
	for m, key := range g.orderedKeys {
		ret[m] = g.GetGroup(key)
	}
	return ret
}

// ListGroups stub
func (g *GroupedSeries) ListGroups() []string {
	return g.orderedKeys
}

// HavingCount stub
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

// IterGroups stub
func (g *GroupedDataFrame) IterGroups() []*DataFrame {
	ret := make([]*DataFrame, len(g.rowIndices))
	for m, key := range g.orderedKeys {
		ret[m] = g.GetGroup(key)
	}
	return ret
}

// ListGroups stub
func (g *GroupedDataFrame) ListGroups() []string {
	return g.orderedKeys
}
