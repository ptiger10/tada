// This file was automatically generated.
// Any changes will be lost if this file is regenerated.
// Run "make generate" in the `tada_generics` repo to regenerate from template.

package tada

import (
	"fmt"
	"time"
)

func convertSimplifiedFloat64ReduceFunc(
	simplifiedFn func([]float64) float64) func(
	[]float64, []bool, []int) (float64, bool) {

	fn := func(slice []float64, isNull []bool, index []int) (float64, bool) {
		var atLeastOneValid bool
		inputVals := make([]float64, 0)
		for _, i := range index {
			if !isNull[i] {
				inputVals = append(inputVals, slice[i])
				atLeastOneValid = true
			}
		}
		if !atLeastOneValid {
			return empty{}.float64(), true
		}
		return simplifiedFn(inputVals), false
	}
	return fn
}

func groupedFloat64ReduceFunc(
	slice []float64,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func([]float64, []bool, []int) (float64, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(slice)
	}
	retVals := make([]float64, retLength)
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

func (g *GroupedSeries) float64ReduceFunc(name string, fn func(slice []float64, isNull []bool, index []int) (float64, bool)) *Series {
	var sharedData bool
	if g.series.values.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.series.values.name)
	}
	retVals := groupedFloat64ReduceFunc(
		g.series.values.float64().slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)
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

func (g *GroupedDataFrame) float64ReduceFunc(
	name string, cols []string, fn func(slice []float64, isNull []bool, index []int) (float64, bool)) *DataFrame {
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
		retVals[k] = groupedFloat64ReduceFunc(
			g.df.values[index].float64().slice, g.df.values[k].isNull, adjustedColNames[k], false, g.rowIndices, fn)
	}

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          fmt.Sprintf("%v_%v", name, g.df.name),
	}
}

func convertSimplifiedStringReduceFunc(
	simplifiedFn func([]string) string) func(
	[]string, []bool, []int) (string, bool) {

	fn := func(slice []string, isNull []bool, index []int) (string, bool) {
		var atLeastOneValid bool
		inputVals := make([]string, 0)
		for _, i := range index {
			if !isNull[i] {
				inputVals = append(inputVals, slice[i])
				atLeastOneValid = true
			}
		}
		if !atLeastOneValid {
			return empty{}.string(), true
		}
		return simplifiedFn(inputVals), false
	}
	return fn
}

func groupedStringReduceFunc(
	slice []string,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func([]string, []bool, []int) (string, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(slice)
	}
	retVals := make([]string, retLength)
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

func (g *GroupedSeries) stringReduceFunc(name string, fn func(slice []string, isNull []bool, index []int) (string, bool)) *Series {
	var sharedData bool
	if g.series.values.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.series.values.name)
	}
	retVals := groupedStringReduceFunc(
		g.series.values.string().slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)
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

func (g *GroupedDataFrame) stringReduceFunc(
	name string, cols []string, fn func(slice []string, isNull []bool, index []int) (string, bool)) *DataFrame {
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
		retVals[k] = groupedStringReduceFunc(
			g.df.values[index].string().slice, g.df.values[k].isNull, adjustedColNames[k], false, g.rowIndices, fn)
	}

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          fmt.Sprintf("%v_%v", name, g.df.name),
	}
}

func convertSimplifiedDateTimeReduceFunc(
	simplifiedFn func([]time.Time) time.Time) func(
	[]time.Time, []bool, []int) (time.Time, bool) {

	fn := func(slice []time.Time, isNull []bool, index []int) (time.Time, bool) {
		var atLeastOneValid bool
		inputVals := make([]time.Time, 0)
		for _, i := range index {
			if !isNull[i] {
				inputVals = append(inputVals, slice[i])
				atLeastOneValid = true
			}
		}
		if !atLeastOneValid {
			return empty{}.dateTime(), true
		}
		return simplifiedFn(inputVals), false
	}
	return fn
}

func groupedDateTimeReduceFunc(
	slice []time.Time,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func([]time.Time, []bool, []int) (time.Time, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(slice)
	}
	retVals := make([]time.Time, retLength)
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

func (g *GroupedSeries) dateTimeReduceFunc(name string, fn func(slice []time.Time, isNull []bool, index []int) (time.Time, bool)) *Series {
	var sharedData bool
	if g.series.values.name != "" {
		name = fmt.Sprintf("%v_%v", name, g.series.values.name)
	}
	retVals := groupedDateTimeReduceFunc(
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

func (g *GroupedDataFrame) dateTimeReduceFunc(
	name string, cols []string, fn func(slice []time.Time, isNull []bool, index []int) (time.Time, bool)) *DataFrame {
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
		retVals[k] = groupedDateTimeReduceFunc(
			g.df.values[index].dateTime().slice, g.df.values[k].isNull, adjustedColNames[k], false, g.rowIndices, fn)
	}

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          fmt.Sprintf("%v_%v", name, g.df.name),
	}
}
