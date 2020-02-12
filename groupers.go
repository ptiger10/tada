package tada

import (
	"errors"
	"fmt"
	"strings"
)

// GroupedSeries

// Err returns the underlying error, if any
func (g GroupedSeries) Err() error {
	return g.err
}

// control for inadvertent splitting just because the name has the level separator using `toSplit`
func splitLabelIntoLevels(label string, toSplit bool) []string {
	if toSplit {
		return strings.Split(label, optionLevelSeparator)
	}
	return []string{label}
}

func joinLevelsIntoLabel(levels []string) string {
	return strings.Join(levels, optionLevelSeparator)
}

func (g GroupedSeries) stringFunc(name string, fn func(val []string, isNull []bool, index []int) (string, bool)) *Series {
	if len(g.groups) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	vals := make([]string, len(g.groups))
	numLevels := len(g.labelNames)
	valueIsNull := make([]bool, len(g.groups))

	labelLevels := make([][]string, numLevels)
	labelIsNull := make([][]bool, numLevels)
	labelNames := make([]string, numLevels)
	for lvl := 0; lvl < numLevels; lvl++ {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
		labelNames[lvl] = g.series.labels[lvl].name
	}

	// iterate over rows
	for rowNumber, key := range g.orderedKeys {
		output, isNull := fn(g.series.values.str().slice, g.series.values.isNull, g.groups[key])
		vals[rowNumber] = output
		valueIsNull[rowNumber] = isNull
		splitKey := splitLabelIntoLevels(key, true)
		for j := range splitKey {
			labelLevels[j][rowNumber] = splitKey[j]
			labelIsNull[j][rowNumber] = false
		}
	}
	labels := make([]*valueContainer, len(labelLevels))
	for j := range labelLevels {
		labels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: labelNames[j]}
	}
	return &Series{
		values: &valueContainer{slice: vals, isNull: valueIsNull, name: name},
		labels: labels,
	}
}

func (g GroupedSeries) mathFunc(name string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *Series {
	if len(g.groups) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	vals := make([]float64, len(g.groups))
	numLevels := len(g.labelNames)
	labelLevels := make([][]string, numLevels)
	labelIsNull := make([][]bool, numLevels)
	labelNames := make([]string, numLevels)

	valueIsNull := make([]bool, len(g.groups))
	for lvl := 0; lvl < numLevels; lvl++ {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
		labelNames[lvl] = g.series.labels[lvl].name
	}

	for rowNumber, key := range g.orderedKeys {
		output, isNull := fn(g.series.values.float().slice, g.series.values.isNull, g.groups[key])
		vals[rowNumber] = output
		valueIsNull[rowNumber] = isNull
		splitKey := splitLabelIntoLevels(key, true)
		for j := range splitKey {
			labelLevels[j][rowNumber] = splitKey[j]
			labelIsNull[j][rowNumber] = false
		}
	}
	labels := make([]*valueContainer, len(labelLevels))
	for j := range labelLevels {
		labels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: labelNames[j]}
	}
	return &Series{
		values: &valueContainer{slice: vals, isNull: valueIsNull, name: name},
		labels: labels,
	}
}

// Sum stub
func (g GroupedSeries) Sum() *Series {
	return g.mathFunc("sum", sum)
}

// Mean stub
func (g GroupedSeries) Mean() *Series {
	return g.mathFunc("mean", mean)
}

// Median stub
func (g GroupedSeries) Median() *Series {
	return g.mathFunc("median", median)
}

// Std stub
func (g GroupedSeries) Std() *Series {
	return g.mathFunc("std", std)
}

// Count stub
func (g GroupedSeries) Count() *Series {
	return g.mathFunc("count", count)
}

// Min stub
func (g GroupedSeries) Min() *Series {
	return g.mathFunc("min", min)
}

// Max stub
func (g GroupedSeries) Max() *Series {
	return g.mathFunc("max", max)
}

// First stub
func (g GroupedSeries) First() *Series {
	return g.stringFunc("first", first)
}

// Last stub
func (g GroupedSeries) Last() *Series {
	return g.stringFunc("last", last)
}

func (g GroupedDataFrame) stringFunc(
	name string, cols []string, fn func(val []string, isNull []bool, index []int) (string, bool)) *DataFrame {
	numLevels := len(g.labelNames)

	// isolate columns to return
	colIndex := make([]int, len(cols))
	if len(cols) == 0 {
		colIndex = makeIntRange(0, len(g.df.values))
	} else {
		for i, col := range cols {
			idx, err := findColWithName(col, g.df.values)
			if err != nil {
				return dataFrameWithError(fmt.Errorf("GroupBy(): %v", err))
			}
			colIndex[i] = idx
		}
	}
	// prepare [][]string container to receive row-level values
	vals := make([][]string, len(colIndex))
	valuesIsNull := make([][]bool, len(colIndex))
	names := make([]string, len(colIndex))
	for col := range colIndex {
		vals[col] = make([]string, len(g.groups))
		valuesIsNull[col] = make([]bool, len(g.groups))
		names[col] = g.df.values[col].name
	}

	// prepare [][]string container to receive row-level labels
	labelLevels := make([][]string, numLevels)
	labelIsNull := make([][]bool, numLevels)
	for lvl := 0; lvl < numLevels; lvl++ {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
	}
	// iterate over rows
	for rowNumber, key := range g.orderedKeys {
		for i, col := range colIndex {
			output, isNull := fn(
				g.df.values[col].str().slice, g.df.values[col].isNull, g.groups[key])
			vals[i][rowNumber] = output
			valuesIsNull[i][rowNumber] = isNull
		}
		splitKey := splitLabelIntoLevels(key, true)
		for j := range splitKey {
			labelLevels[j][rowNumber] = splitKey[j]
			labelIsNull[j][rowNumber] = false
		}
	}

	// convert intermediate containers to valueContainers
	retLabels := make([]*valueContainer, len(labelLevels))
	retValues := make([]*valueContainer, len(colIndex))
	for j := range retLabels {
		retLabels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: g.labelNames[j]}
	}
	for k := range retValues {
		retValues[k] = &valueContainer{slice: vals[k], isNull: valuesIsNull[k], name: names[k]}
	}
	return &DataFrame{
		values:        retValues,
		labels:        retLabels,
		name:          name,
		colLevelNames: g.df.colLevelNames,
	}
}

func (g GroupedDataFrame) mathFunc(
	name string, cols []string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *DataFrame {
	if len(g.groups) == 0 {
		return dataFrameWithError(errors.New("no groups"))
	}
	numLevels := len(g.labelNames)

	// isolate columns to return
	colIndex := make([]int, len(cols))
	if len(cols) == 0 {
		colIndex = makeIntRange(0, len(g.df.values))
	} else {
		for i, col := range cols {
			idx, err := findColWithName(col, g.df.values)
			if err != nil {
				return dataFrameWithError(fmt.Errorf("GroupBy(): %v", err))
			}
			colIndex[i] = idx
		}
	}

	// prepare [][]float64 container to receive row-level values
	vals := make([][]float64, len(colIndex))
	valuesIsNull := make([][]bool, len(colIndex))
	names := make([]string, len(colIndex))
	for col := range colIndex {
		vals[col] = make([]float64, len(g.groups))
		valuesIsNull[col] = make([]bool, len(g.groups))
		names[col] = g.df.values[col].name
	}

	// prepare [][]string container to receive row-level labels
	labelLevels := make([][]string, numLevels)
	labelIsNull := make([][]bool, numLevels)
	for lvl := 0; lvl < numLevels; lvl++ {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
	}
	// iterate over rows
	for rowNumber, key := range g.orderedKeys {
		for i, col := range colIndex {
			output, isNull := fn(
				g.df.values[col].float().slice, g.df.values[col].isNull, g.groups[key])
			vals[i][rowNumber] = output
			valuesIsNull[i][rowNumber] = isNull
		}
		splitKey := splitLabelIntoLevels(key, true)
		for j := range splitKey {
			labelLevels[j][rowNumber] = splitKey[j]
			labelIsNull[j][rowNumber] = false
		}
	}

	// convert intermediate containers to valueContainers
	retLabels := make([]*valueContainer, len(labelLevels))
	retValues := make([]*valueContainer, len(colIndex))
	for j := range retLabels {
		retLabels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: g.labelNames[j]}
	}
	for k := range retValues {
		retValues[k] = &valueContainer{slice: vals[k], isNull: valuesIsNull[k], name: names[k]}
	}
	return &DataFrame{
		values:        retValues,
		labels:        retLabels,
		name:          name,
		colLevelNames: g.df.colLevelNames,
	}
}

// Sum stub
func (g GroupedDataFrame) Sum(colNames ...string) *DataFrame {
	return g.mathFunc("sum", colNames, sum)
}

// Mean stub
func (g GroupedDataFrame) Mean(colNames ...string) *DataFrame {
	return g.mathFunc("mean", colNames, mean)
}

// Median stub
func (g GroupedDataFrame) Median(colNames ...string) *DataFrame {
	return g.mathFunc("median", colNames, median)
}

// Std stub
func (g GroupedDataFrame) Std(colNames ...string) *DataFrame {
	return g.mathFunc("std", colNames, std)
}

// Count stub
func (g GroupedDataFrame) Count(colNames ...string) *DataFrame {
	return g.mathFunc("count", colNames, count)
}

// Min stub
func (g GroupedDataFrame) Min(colNames ...string) *DataFrame {
	return g.mathFunc("min", colNames, min)
}

// Max stub
func (g GroupedDataFrame) Max(colNames ...string) *DataFrame {
	return g.mathFunc("max", colNames, max)
}

// First stub
func (g GroupedDataFrame) First(colNames ...string) *DataFrame {
	return g.stringFunc("first", colNames, first)
}

// Last stub
func (g GroupedDataFrame) Last(colNames ...string) *DataFrame {
	return g.stringFunc("last", colNames, last)
}
