package tada

import (
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

// First stub
func (g *GroupedDataFrame) First(colNames ...string) *DataFrame {
	return g.stringFunc("first", colNames, first)
}

// Last stub
func (g *GroupedDataFrame) Last(colNames ...string) *DataFrame {
	return g.stringFunc("last", colNames, last)
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

// Align isolates the column matching `colName` and aligns subsequent group aggregations with the original DataFrame labels.
func (g *GroupedDataFrame) Align(colName string) *GroupedSeries {
	_, err := findColWithName(colName, g.df.values)
	if err != nil {
		return &GroupedSeries{
			err: fmt.Errorf("Align(): %v", err),
		}
	}
	return &GroupedSeries{
		orderedKeys: g.orderedKeys,
		rowIndices:  g.rowIndices,
		labels:      g.labels,
		series:      g.df.Col(colName),
		aligned:     true,
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
		rowIndex := make([]int, 0)
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
				if withinDuration(vals[i], vals[nextRow], d) {
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
