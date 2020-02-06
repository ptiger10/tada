package tada

import (
	"fmt"
	"reflect"
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
		values = append(values, &valueContainer{slice: slice, isNull: isNull})
	}

	// handle labels
	retLabels := make([]*valueContainer, len(labels))
	if len(retLabels) == 0 {
		// default labels
		defaultLabels, isNull := makeDefaultLabels(0, reflect.ValueOf(slices[0]).Len())
		retLabels = append(retLabels, &valueContainer{slice: defaultLabels, isNull: isNull, name: "*0"})
	} else {
		for i := range retLabels {
			input := labels[i]
			if !isSlice(input) {
				return dataFrameWithError(fmt.Errorf("NewDataFrame(): unsupported label kind (%v) at level %d; must be slice", reflect.TypeOf(input), i))
			}
			isNull := setNullsFromInterface(input)
			if isNull == nil {
				return dataFrameWithError(fmt.Errorf(
					"NewDataFrame(): unable to calculate null values at level %d ([]%v not supported)", i, reflect.TypeOf(input).Elem()))
			}
			// handle special case of []Element: convert to []interface{}
			elements, ok := input.([]Element)
			if ok {
				ret := make([]interface{}, len(isNull))
				for i := range ret {
					ret[i] = elements[i].val
				}
				input = ret
			}
			retLabels[i] = &valueContainer{slice: input, isNull: isNull, name: fmt.Sprintf("*%d", i)}
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

// ReadCSV stub
func (df *DataFrame) ReadCSV(csv [][]string) *DataFrame {
	return nil
}

// ReadInterface stub
func (df *DataFrame) ReadInterface([][]interface{}) *DataFrame {
	return nil
}

// ReadStructs stub
func (df *DataFrame) ReadStructs(interface{}) *DataFrame {
	return nil
}

// -- GETTERS

// Subset stub
func (df *DataFrame) Subset(index []int) *DataFrame {
	return nil
}

// SubsetLabels stub
func (df *DataFrame) SubsetLabels([]int) *DataFrame {
	return nil
}

// SubsetCols stub
func (df *DataFrame) SubsetCols([]int) *DataFrame {
	return nil
}

// Col stub
func (df *DataFrame) Col(name string) *DataFrame {
	return nil
}

// Cols stub
func (df *DataFrame) Cols(names ...string) *DataFrame {
	return nil
}

// Head stub
func (df *DataFrame) Head(rows int) *DataFrame {
	return nil
}

// Tail stub
func (df *DataFrame) Tail(rows int) *DataFrame {
	return nil
}

// Valid stub
func (df *DataFrame) Valid() *DataFrame {
	return nil
}

// Null stub
func (df *DataFrame) Null() *DataFrame {
	return nil
}

// Index stub
func (df *DataFrame) Index(label string) []int {
	return nil
}

// IndexRange stub
func (df *DataFrame) IndexRange(firstLabel, lastLabel string) []int {
	return nil
}

// ColIndex stub
func (df *DataFrame) ColIndex(name string) []int {
	return nil
}

// ColIndexRange stub
func (df *DataFrame) ColIndexRange(firstName, lastName string) []int {
	return nil
}

// -- SETTERS

// WithLabels stub
func (df *DataFrame) WithLabels(name string, slice interface{}) *DataFrame {
	return nil
}

// WithRow stub
func (df *DataFrame) WithRow(label string, values []interface{}) *DataFrame {
	return nil
}

// WithCol stub
func (df *DataFrame) WithCol(label string, slice interface{}) *DataFrame {
	return nil
}

// Drop stub
func (df *DataFrame) Drop(index []int, dimension Dimension) *DataFrame {
	return nil
}

// DropNull stub
func (df *DataFrame) DropNull(cols ...string) *DataFrame {
	return nil
}

// SetLabels stub
func (df *DataFrame) SetLabels(cols ...string) *DataFrame {
	return nil
}

// ResetLabels stub
func (df *DataFrame) ResetLabels(labelNames ...string) *DataFrame {
	return nil
}

// Name stub
// in place
func (df *DataFrame) Name() {
	return
}

// reshape

// Transpose stub
func (df *DataFrame) Transpose() *DataFrame {
	return nil
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

// filter

// FilterFloat stub
func (df *DataFrame) FilterFloat(func(val float64) bool) *DataFrame {
	return nil
}

// apply

// ApplyFloat stub
func (df *DataFrame) ApplyFloat(func(val float64) float64) *DataFrame {
	return nil
}

// combine

// Merge stub
func (df *DataFrame) Merge(other *DataFrame) *DataFrame {
	return nil
}

// Lookup stub
func (df *DataFrame) Lookup(other *DataFrame, how string, leftOn string, rightOn string, dimension Dimension) *DataFrame {
	return nil
}

// Add stub
func (df *DataFrame) Add(other *DataFrame) *DataFrame {
	return nil
}

// Subtract stub
func (df *DataFrame) Subtract(other *DataFrame) *DataFrame {
	return nil
}

// Multiply stub
func (df *DataFrame) Multiply(other *DataFrame) *DataFrame {
	return nil
}

// Divide stub
func (df *DataFrame) Divide(other *DataFrame) *DataFrame {
	return nil
}

// sort

// Sort stub
func (df *DataFrame) Sort(...Sorter) *DataFrame {
	return nil
}

// grouping

// GroupBy stub
// includes label levels and columns
func (df *DataFrame) GroupBy(names ...string) *GroupedDataFrame {
	return nil
}

// PivotTable stub
func (df *DataFrame) PivotTable(labels, columns, values, aggFn string) *DataFrame {
	return nil
}

// iterator

// IterRows stub
func (df *DataFrame) IterRows() []map[string]Element {
	return nil
}

// IterCols stub
func (df *DataFrame) IterCols() []map[string]Element {
	return nil
}

// math

// Sum stub
func (df *DataFrame) Sum() *Series {
	return nil
}

// Mean stub
func (df *DataFrame) Mean() *Series {
	return nil
}

// Median stub
func (df *DataFrame) Median() *Series {
	return nil
}

// Std stub
func (df *DataFrame) Std() *Series {
	return nil
}

// Len stub
func (df *DataFrame) Len() int {
	return 0
}
