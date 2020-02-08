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

func splitLabelIntoLevels(label string) []string {
	return strings.Split(label, "|")
}

func joinLevelsIntoLabel(levels []string) string {
	return strings.Join(levels, "|")
}

func (g GroupedSeries) mathFunc(name string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *Series {
	if len(g.groups) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	vals := make([]float64, len(g.groups))
	sampleKey := g.orderedKeys[0]
	numLabelsPerRow := len(splitLabelIntoLevels(sampleKey))
	labelLevels := make([][]string, numLabelsPerRow)
	labelIsNull := make([][]bool, numLabelsPerRow)
	labelNames := make([]string, numLabelsPerRow)

	valueIsNull := make([]bool, len(g.groups))
	for lvl := 0; lvl < numLabelsPerRow; lvl++ {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
		labelNames[lvl] = g.series.labels[lvl].name
	}

	for rowNumber, key := range g.orderedKeys {
		output, isNull := fn(g.series.values.float().slice, g.series.values.isNull, g.groups[key])
		vals[rowNumber] = output
		valueIsNull[rowNumber] = isNull
		splitKey := splitLabelIntoLevels(key)
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

func (g GroupedDataFrame) mathFunc(
	name string, cols []string, fn func(val []float64, isNull []bool, index []int) (float64, bool)) *DataFrame {
	if len(g.groups) == 0 {
		return dataFrameWithError(errors.New("GroupBy(): no groups"))
	}
	sampleKey := g.orderedKeys[0]
	numLabelsPerRow := len(splitLabelIntoLevels(sampleKey))

	// isolate columns to return
	var colIndex []int
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
	labelLevels := make([][]string, numLabelsPerRow)
	labelIsNull := make([][]bool, numLabelsPerRow)
	labelNames := make([]string, numLabelsPerRow)
	for lvl := 0; lvl < numLabelsPerRow; lvl++ {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
		labelNames[lvl] = g.df.labels[lvl].name
	}
	// iterate over rows
	for rowNumber, key := range g.orderedKeys {
		for i, col := range colIndex {
			output, isNull := fn(
				g.df.values[col].float().slice, g.df.values[col].isNull, g.groups[key])
			vals[i][rowNumber] = output
			valuesIsNull[i][rowNumber] = isNull
		}
		splitKey := splitLabelIntoLevels(key)
		for j := range splitKey {
			labelLevels[j][rowNumber] = splitKey[j]
			labelIsNull[j][rowNumber] = false
		}
	}

	// convert intermediate containers to valueContainers
	retLabels := make([]*valueContainer, len(labelLevels))
	retValues := make([]*valueContainer, len(colIndex))
	for j := range retLabels {
		retLabels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j], name: labelNames[j]}
	}
	for k := range retValues {
		retValues[k] = &valueContainer{slice: vals[k], isNull: valuesIsNull[k], name: names[k]}
	}
	return &DataFrame{
		values: retValues,
		labels: retLabels,
		name:   name,
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
