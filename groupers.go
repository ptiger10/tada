package tada

import (
	"errors"
	"fmt"
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
	if len(g.rowIndices) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
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
	}
	return &Series{
		values: retVals,
		labels: retLabels,
	}
}

func (g *GroupedSeries) stringFunc(name string, fn func(val []string, isNull []bool, index []int) (string, bool)) *Series {
	if len(g.rowIndices) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
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
	}
	return &Series{
		values: retVals,
		labels: retLabels,
	}
}

func (g *GroupedSeries) dateTimeFunc(name string, fn func(val []time.Time, isNull []bool, index []int) (time.Time, bool)) *Series {
	if len(g.rowIndices) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
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
	}
	return &Series{
		values: retVals,
		labels: retLabels,
	}
}

// Apply stub
func (g *GroupedSeries) Apply(name string, lambda GroupApplyFn) *Series {
	// remove all nulls before running each set of values through custom user function
	if lambda.F64 != nil {
		fn := func(vals []float64, isNull []bool, index []int) (float64, bool) {
			var atLeastOneValid bool
			inputVals := make([]float64, 0)
			for _, i := range index {
				if !isNull[i] {
					inputVals = append(inputVals, vals[i])
					atLeastOneValid = true
				}
			}
			if !atLeastOneValid {
				return 0, true
			}
			return lambda.F64(inputVals), false
		}
		return g.floatFunc(name, fn)
	} else if lambda.String != nil {
		fn := func(vals []string, isNull []bool, index []int) (string, bool) {
			var atLeastOneValid bool
			inputVals := make([]string, 0)
			for _, i := range index {
				if !isNull[i] {
					inputVals = append(inputVals, vals[i])
					atLeastOneValid = true
				}
			}
			if !atLeastOneValid {
				return "", true
			}
			return lambda.String(inputVals), false
		}
		return g.stringFunc(name, fn)
	} else if lambda.DateTime != nil {
		fn := func(vals []time.Time, isNull []bool, index []int) (time.Time, bool) {
			var atLeastOneValid bool
			inputVals := make([]time.Time, 0)
			for _, i := range index {
				if !isNull[i] {
					inputVals = append(inputVals, vals[i])
					atLeastOneValid = true
				}
			}
			if !atLeastOneValid {
				return time.Time{}, true
			}
			return lambda.DateTime(inputVals), false
		}
		return g.dateTimeFunc(name, fn)
	}
	return seriesWithError(fmt.Errorf("no lambda function provided"))
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

// First stub
func (g *GroupedSeries) First() *Series {
	return g.stringFunc("first", first)
}

// Last stub
func (g *GroupedSeries) Last() *Series {
	return g.stringFunc("last", last)
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

func (g *GroupedDataFrame) stringFunc(
	name string, cols []string, fn func(val []string, isNull []bool, index []int) (string, bool)) *DataFrame {
	if len(g.rowIndices) == 0 {
		return dataFrameWithError(errors.New("GroupBy(): no groups"))
	}
	groupedVals := makeStringMatrix(len(cols), len(g.rowIndices))
	groupedNulls := makeBoolMatrix(len(cols), len(g.rowIndices))

	for k := range cols {
		referenceVals := g.df.values[k].str().slice
		// iterate over rowIndices
		for i, rowIndex := range g.rowIndices {
			output, isNull := fn(referenceVals, g.df.values[k].isNull, rowIndex)
			groupedVals[k][i] = output
			groupedNulls[k][i] = isNull
		}
	}
	retVals := copyStringsIntoValueContainers(groupedVals, groupedNulls, cols)

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}

	// numLevels := len(g.labels)

	// // isolate columns to return
	// colIndex := make([]int, len(cols))
	// if len(cols) == 0 {
	// 	colIndex = makeIntRange(0, len(g.df.values))
	// } else {
	// 	for i, col := range cols {
	// 		idx, err := findColWithName(col, g.df.values)
	// 		if err != nil {
	// 			return dataFrameWithError(fmt.Errorf("GroupBy(): %v", err))
	// 		}
	// 		colIndex[i] = idx
	// 	}
	// }
	// // prepare [][]string container to receive row-level values
	// vals := make([][]string, len(colIndex))
	// valuesIsNull := make([][]bool, len(colIndex))
	// names := make([]string, len(colIndex))
	// for col := range colIndex {
	// 	vals[col] = make([]string, len(g.groups))
	// 	valuesIsNull[col] = make([]bool, len(g.groups))
	// 	names[col] = g.df.values[col].name
	// }

	// // prepare [][]string container to receive row-level labels
	// labelLevels := make([][]string, numLevels)
	// labelIsNull := make([][]bool, numLevels)
	// for lvl := 0; lvl < numLevels; lvl++ {
	// 	labelLevels[lvl] = make([]string, len(g.groups))
	// 	labelIsNull[lvl] = make([]bool, len(g.groups))
	// }
	// // iterate over rows
	// for rowNumber, key := range g.orderedKeys {
	// 	for i, col := range colIndex {
	// 		output, isNull := fn(
	// 			g.df.values[col].str().slice, g.df.values[col].isNull, g.groups[key])
	// 		vals[i][rowNumber] = output
	// 		valuesIsNull[i][rowNumber] = isNull
	// 	}
	// 	splitKey := splitLabelIntoLevels(key, true)
	// 	for j := range splitKey {
	// 		labelLevels[j][rowNumber] = splitKey[j]
	// 		labelIsNull[j][rowNumber] = false
	// 	}
	// }

	// // convert intermediate containers to valueContainers
	// retLabels := make([]*valueContainer, len(labelLevels))
	// retValues := make([]*valueContainer, len(colIndex))
	// for j := range retLabels {
	// 	retLabels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: g.levelNames[j]}
	// }
	// for k := range retValues {
	// 	retValues[k] = &valueContainer{slice: vals[k], isNull: valuesIsNull[k], name: names[k]}
	// }
	// return &DataFrame{
	// 	values:        retValues,
	// 	labels:        retLabels,
	// 	name:          name,
	// 	colLevelNames: g.df.colLevelNames,
	// }
}

func (g *GroupedDataFrame) mathFunc(
	name string, cols []string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *DataFrame {
	if len(g.rowIndices) == 0 {
		return dataFrameWithError(errors.New("GroupBy(): no groups"))
	}
	groupedVals := makeFloatMatrix(len(cols), len(g.rowIndices))
	groupedNulls := makeBoolMatrix(len(cols), len(g.rowIndices))

	for k := range cols {
		referenceVals := g.df.values[k].float().slice
		// iterate over rowsIndices
		for i, rowIndex := range g.rowIndices {
			output, isNull := fn(referenceVals, g.df.values[k].isNull, rowIndex)
			groupedVals[k][i] = output
			groupedNulls[k][i] = isNull
		}
	}
	retVals := copyFloatsIntoValueContainers(groupedVals, groupedNulls, cols)

	return &DataFrame{
		values: retVals,
		labels: g.labels,
	}

	// if len(g.groups) == 0 {
	// 	return dataFrameWithError(errors.New("no groups"))
	// }
	// numLevels := len(g.levelNames)

	// // isolate columns to return
	// colIndex := make([]int, len(cols))
	// if len(cols) == 0 {
	// 	colIndex = makeIntRange(0, len(g.df.values))
	// } else {
	// 	for i, col := range cols {
	// 		idx, err := findColWithName(col, g.df.values)
	// 		if err != nil {
	// 			return dataFrameWithError(fmt.Errorf("GroupBy(): %v", err))
	// 		}
	// 		colIndex[i] = idx
	// 	}
	// }

	// // prepare [][]float64 container to receive row-level values
	// vals := make([][]float64, len(colIndex))
	// valuesIsNull := make([][]bool, len(colIndex))
	// names := make([]string, len(colIndex))
	// for col := range colIndex {
	// 	vals[col] = make([]float64, len(g.groups))
	// 	valuesIsNull[col] = make([]bool, len(g.groups))
	// 	names[col] = g.df.values[col].name
	// }

	// // prepare [][]string container to receive row-level labels
	// labelLevels := make([][]string, numLevels)
	// labelIsNull := make([][]bool, numLevels)
	// for lvl := 0; lvl < numLevels; lvl++ {
	// 	labelLevels[lvl] = make([]string, len(g.groups))
	// 	labelIsNull[lvl] = make([]bool, len(g.groups))
	// }
	// // iterate over rows
	// for rowNumber, key := range g.orderedKeys {
	// 	for i, col := range colIndex {
	// 		output, isNull := fn(
	// 			g.df.values[col].float().slice, g.df.values[col].isNull, g.groups[key])
	// 		vals[i][rowNumber] = output
	// 		valuesIsNull[i][rowNumber] = isNull
	// 	}
	// 	splitKey := splitLabelIntoLevels(key, true)
	// 	for j := range splitKey {
	// 		labelLevels[j][rowNumber] = splitKey[j]
	// 		labelIsNull[j][rowNumber] = false
	// 	}
	// }

	// // convert intermediate containers to valueContainers
	// retLabels := make([]*valueContainer, len(labelLevels))
	// retValues := make([]*valueContainer, len(colIndex))
	// for j := range retLabels {
	// 	retLabels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: g.levelNames[j]}
	// }
	// for k := range retValues {
	// 	retValues[k] = &valueContainer{slice: vals[k], isNull: valuesIsNull[k], name: names[k]}
	// }
	// return &DataFrame{
	// 	values:        retValues,
	// 	labels:        retLabels,
	// 	name:          name,
	// 	colLevelNames: g.df.colLevelNames,
	// }
}

// Sum stub
func (g *GroupedDataFrame) Sum(colNames ...string) *DataFrame {
	return g.mathFunc("sum", colNames, sum)
}

// Mean stub
func (g *GroupedDataFrame) Mean(colNames ...string) *DataFrame {
	return g.mathFunc("mean", colNames, mean)
}

// Median stub
func (g *GroupedDataFrame) Median(colNames ...string) *DataFrame {
	return g.mathFunc("median", colNames, median)
}

// Std stub
func (g *GroupedDataFrame) Std(colNames ...string) *DataFrame {
	return g.mathFunc("std", colNames, std)
}

// Count stub
func (g *GroupedDataFrame) Count(colNames ...string) *DataFrame {
	return g.mathFunc("count", colNames, count)
}

// Min stub
func (g *GroupedDataFrame) Min(colNames ...string) *DataFrame {
	return g.mathFunc("min", colNames, min)
}

// Max stub
func (g *GroupedDataFrame) Max(colNames ...string) *DataFrame {
	return g.mathFunc("max", colNames, max)
}

// First stub
func (g *GroupedDataFrame) First(colNames ...string) *DataFrame {
	return g.stringFunc("first", colNames, first)
}

// Last stub
func (g *GroupedDataFrame) Last(colNames ...string) *DataFrame {
	return g.stringFunc("last", colNames, last)
}

// Align stub
func (g *GroupedSeries) Align() *GroupedSeries {
	g.aligned = true
	return g
}

// // Align isolates the column matching `colName` and aligns subsequent group aggregations with the original DataFrame labels.
// func (g *GroupedDataFrame) Align(colName string) *GroupedSeries {
// 	_, err := findColWithName(colName, g.df.values)
// 	if err != nil {
// 		return &GroupedSeries{
// 			err: err,
// 		}
// 	}
// 	return &GroupedSeries{
// 		groups:      g.groups,

// 		series:      g.df.Col(colName),
// 		aligned:     true,
// 	}
// }
