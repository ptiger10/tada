package tada

import (
	"errors"
	"fmt"
	"strings"
)

// GroupedSeries

// Err returns the underlying error, if any
func (g *GroupedSeries) Err() error {
	return g.err
}

func (g *GroupedSeries) String() string {
	groups := make([]string, len(g.groups))
	var counter int
	for k := range g.groups {
		counter++
		groups[counter] = k
	}
	return "Groups: " + strings.Join(groups, ",")
}

// GetGroup stub
func (g *GroupedSeries) GetGroup(group string) *Series {
	i, ok := g.groups[group]
	if !ok {
		return seriesWithError(fmt.Errorf("GetGroup(): `group` (%v) not in groups", group))
	}
	return g.series.Subset(g.rowIndices[i])
}

func (g *GroupedSeries) stringFunc(name string, fn func(val []string, isNull []bool, index []int) (string, bool)) *Series {
	if len(g.rowIndices) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	groupedVals := make([]string, len(g.rowIndices))
	valueIsNull := make([]bool, len(g.rowIndices))

	referenceVals := g.series.values.str().slice
	// iterate over rowsIndices
	for i, rowIndex := range g.rowIndices {
		output, isNull := fn(referenceVals, g.series.values.isNull, rowIndex)
		groupedVals[i] = output
		valueIsNull[i] = isNull
	}
	return &Series{
		values: &valueContainer{slice: groupedVals, isNull: valueIsNull, name: name},
		labels: g.labels,
	}
}

// Apply stub
func (g *GroupedSeries) Apply(name string, lambda GroupApplyFn) *Series {
	if lambda.F64 != nil {
		fn := func(val []float64, isNull []bool, index []int) (float64, bool) {
			return lambda.F64(val), false
		}
		if g.aligned {
			return g.applyAlignedFloat(name, lambda.F64)
		}
		return g.mathFunc(name, fn)
	} else if lambda.String != nil {

	}
	return nil
}

func (g *GroupedSeries) applyAlignedFloat(name string, fn func([]float64) float64) *Series {
	vals := make([]float64, g.series.Len())
	isNull := make([]bool, len(vals))
	referenceVals := g.series.values.float().slice
	for _, index := range g.rowIndices {
		subsetVals := make([]float64, 0)
		for _, i := range index {
			if !isNull[i] {
				subsetVals = append(subsetVals, referenceVals[i])
			}
		}
		output := fn(subsetVals)
		for _, i := range index {
			vals[i] = output
		}
	}
	values := &valueContainer{slice: vals, isNull: isNull, name: name}
	return &Series{
		values: values,
		labels: g.series.labels,
	}
}

func (g *GroupedSeries) alignedMath(name string, fn func([]float64, []bool, []int) (float64, bool)) *Series {
	vals := make([]float64, g.series.Len())
	isNull := make([]bool, len(vals))
	referenceVals := g.series.values.float().slice
	for _, index := range g.rowIndices {
		output, outputIsNull := fn(referenceVals, g.series.values.isNull, index)
		for _, i := range index {
			vals[i] = output
			isNull[i] = outputIsNull
		}
	}
	values := &valueContainer{slice: vals, isNull: isNull, name: name}
	return &Series{
		values: values,
		labels: g.series.labels,
	}
}

func (g *GroupedSeries) mathFunc(name string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *Series {
	if len(g.groups) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	referenceVals := g.series.values.float().slice
	vals := make([]float64, len(g.groups))

	valueIsNull := make([]bool, len(g.groups))

	for i, index := range g.rowIndices {
		// evaluate func
		output, isNull := fn(referenceVals, g.series.values.isNull, index)
		vals[i] = output
		valueIsNull[i] = isNull
	}

	return &Series{
		values: &valueContainer{slice: vals, isNull: valueIsNull, name: name},
		labels: g.labels,
	}
}

// Sum stub
func (g *GroupedSeries) Sum() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_sum", sum)
	}
	return g.mathFunc("sum", sum)
}

// Mean stub
func (g *GroupedSeries) Mean() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_mean", mean)
	}
	return g.mathFunc("mean", mean)
}

// Median stub
func (g *GroupedSeries) Median() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_median", median)
	}
	return g.mathFunc("median", median)
}

// Std stub
func (g *GroupedSeries) Std() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_std", std)
	}
	return g.mathFunc("std", std)
}

// Count stub
func (g *GroupedSeries) Count() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_count", count)
	}
	return g.mathFunc("count", count)
}

// Min stub
func (g *GroupedSeries) Min() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_min", min)
	}
	return g.mathFunc("min", min)
}

// Max stub
func (g *GroupedSeries) Max() *Series {
	if g.aligned {
		return g.alignedMath(g.series.values.name+"_max", max)
	}
	return g.mathFunc("max", max)
}

// First stub
func (g *GroupedSeries) First() *Series {
	return g.stringFunc("first", first)
}

// Last stub
func (g *GroupedSeries) Last() *Series {
	return g.stringFunc("last", last)
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
		// iterate over rowsIndices
		for i, rowIndex := range g.rowIndices {
			output, isNull := fn(referenceVals, g.df.values[k].isNull, rowIndex)
			groupedVals[k][i] = output
			groupedNulls[k][i] = isNull
		}
	}
	retVals := copyStringsIntoValueContainers(groupedVals, groupedNulls, cols)

	return &DataFrame{
		values: retVals,
		labels: g.labels,
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
