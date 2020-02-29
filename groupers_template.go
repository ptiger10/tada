package tada

import (
	"fmt"
)

//go:generate genny -in=$GOFILE -out=groupers_autogen.go gen "genericType=float64,string,time.Time"

func convertSimplifiedgenericTypeReduceFunc(
	simplifiedFn func([]genericType) genericType) func(
	[]genericType, []bool, []int) (genericType, bool) {

	fn := func(slice []genericType, isNull []bool, index []int) (genericType, bool) {
		var atLeastOneValid bool
		inputVals := make([]genericType, 0)
		for _, i := range index {
			if !isNull[i] {
				inputVals = append(inputVals, slice[i])
				atLeastOneValid = true
			}
		}
		if !atLeastOneValid {
			return empty{}.genericType(), true
		}
		return simplifiedFn(inputVals), false
	}
	return fn
}

func groupedgenericTypeReduceFunc(
	slice []genericType,
	nulls []bool,
	name string,
	aligned bool,
	rowIndices [][]int,
	fn func([]genericType, []bool, []int) (genericType, bool)) *valueContainer {
	// default: return length is equal to the number of groups
	retLength := len(rowIndices)
	if aligned {
		// if aligned: return length is overwritten to equal the length of original data
		retLength = len(slice)
	}
	retVals := make([]genericType, retLength)
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

func (g *GroupedSeries) genericTypeReduceFunc(name string, fn func(slice []genericType, isNull []bool, index []int) (genericType, bool)) *Series {
	var sharedData bool
	if g.aligned {
		name = fmt.Sprintf("%v_%v", g.series.values.name, name)
	}
	retVals := groupedgenericTypeReduceFunc(
		g.series.values.genericType().slice, g.series.values.isNull, name, g.aligned, g.rowIndices, fn)
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

func (g *GroupedDataFrame) genericTypeReduceFunc(
	name string, cols []string, fn func(slice []genericType, isNull []bool, index []int) (genericType, bool)) *DataFrame {
	if len(cols) == 0 {
		cols = make([]string, len(g.df.values))
		for k := range cols {
			cols[k] = g.df.values[k].name
		}
	}
	retVals := make([]*valueContainer, len(cols))
	for k := range retVals {
		retVals[k] = groupedgenericTypeReduceFunc(
			g.df.values[k].genericType().slice, g.df.values[k].isNull, cols[k], false, g.rowIndices, fn)
	}

	return &DataFrame{
		values:        retVals,
		labels:        g.labels,
		colLevelNames: []string{"*0"},
		name:          name,
	}
}
