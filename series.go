package tada

import (
	"fmt"
	"reflect"
)

// -- CONSTRUCTORS

// NewSeries constructs a Series from a slice of values and optional label slices.
// Supported underlying slice types: real numbers, string, time.Time, boolean, or interface.
func NewSeries(slice interface{}, labels ...interface{}) *Series {
	// handle values
	if !isSlice(slice) {
		return &Series{err: fmt.Errorf("NewSeries(): unsupported kind (%v); must be slice", reflect.TypeOf(slice))}
	}
	isNull := setNullsFromInterface(slice)
	if isNull == nil {
		return &Series{err: fmt.Errorf(
			"NewSeries(): unable to calculate null values ([]%v not supported)", reflect.TypeOf(slice).Elem())}
	}
	values := &valueContainer{slice: slice, isNull: isNull}

	// handle labels
	retLabels := make([]*valueContainer, len(labels))
	if len(retLabels) == 0 {
		// default labels
		defaultLabels, isNull := makeDefaultLabels(0, reflect.ValueOf(slice).Len())
		retLabels = append(retLabels, &valueContainer{slice: defaultLabels, isNull: isNull, name: "*0"})
	} else {
		for i := range retLabels {
			input := labels[i]
			if !isSlice(input) {
				return seriesWithError(fmt.Errorf("NewSeries(): unsupported label kind (%v) at level %d; must be slice", reflect.TypeOf(input), i))
			}
			isNull := setNullsFromInterface(input)
			if isNull == nil {
				return seriesWithError(fmt.Errorf(
					"NewSeries(): unable to calculate null values at level %d ([]%v not supported)", i, reflect.TypeOf(input).Elem()))
			}
			retLabels[i] = &valueContainer{slice: input, isNull: isNull, name: fmt.Sprintf("*%d", i)}
		}
	}

	return &Series{values: values, labels: retLabels}
}

// Copy returns a deep copy of a Series with no shared references to the original.
func (s *Series) Copy() *Series {
	labels := make([]*valueContainer, len(s.labels))
	for i := 0; i < len(s.labels); i++ {
		labels[i] = s.labels[i].copy()
	}
	return &Series{
		values: s.values.copy(),
		labels: labels,
		err:    s.err,
	}
}

// ToDataFrame converts a Series to a 1-column DataFrame.
func (s *Series) ToDataFrame() *DataFrame {
	s = s.Copy()
	return &DataFrame{
		values: []*valueContainer{s.values},
		labels: s.labels,
		err:    s.err,
	}
}

// -- GETTERS

// Err returns the most recent error attached to the Series, if any.
func (s *Series) Err() error {
	return s.err
}

// Len returns the number of rows in the Series.
func (s *Series) Len() int {
	return reflect.ValueOf(s.values.slice).Len()
}

// Elements returns the underlying value and isNull for each row. If any label level is provided, this returns the Elements of the first label level provided.
func (s *Series) Elements(level ...int) []Element {
	ret := make([]Element, s.Len())
	if len(level) == 0 {
		v := reflect.ValueOf(s.values.slice)
		for i := 0; i < s.Len(); i++ {
			ret[i] = Element{
				val:    v.Index(i).Interface(),
				isNull: s.values.isNull[i],
			}
		}
		// handle optional level
	} else if len(level) > 0 {
		lvl := level[0]
		if lvl >= len(s.labels) {
			return []Element{}
		}
		v := reflect.ValueOf(s.labels[lvl].slice)
		for i := range ret {
			ret[i] = Element{
				val:    v.Index(i).Interface(),
				isNull: s.labels[lvl].isNull[i],
			}
		}
	}
	return ret
}

// Subset returns only the rows specified at the index positions, in the order specified. Returns a new Series.
func (s *Series) Subset(index []int) *Series {
	s = s.Copy()
	s.InPlace().Subset(index)
	return s
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Subset(index []int) {
	err := s.series.values.subsetRows(index)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("Subset(): %v", err))
		return
	}
	for j := 0; j < len(s.series.labels); j++ {
		s.series.labels[j].subsetRows(index)
	}
	return
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Returns a new Series.
func (s *Series) SubsetLabels(index []int) *Series {
	s = s.Copy()
	s.InPlace().SubsetLabels(index)
	return s
}

// SubsetLabels returns only the labels specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SubsetLabels(index []int) {
	l := len(s.series.labels)
	retLabels := make([]*valueContainer, len(index))
	for indexPosition, indexValue := range index {
		if indexValue >= l {
			s.series.resetWithError(fmt.Errorf("SubsetLabels(): index out of range (%d > %d)", indexValue, l-1))
			return
		}
		retLabels[indexPosition] = s.series.labels[indexValue]
	}
	s.series.labels = retLabels
	return
}

// Head returns the first n rows of the Series. If n is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Head(n int) *Series {
	if s.Len() < n {
		n = s.Len()
	}
	index := makeIntRange(0, n)
	return s.Subset(index)
}

// Tail returns the last n rows of the Series. If n is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Tail(rows int) *Series {
	if s.Len() < rows {
		rows = s.Len()
	}
	index := makeIntRange(s.Len()-rows, s.Len())
	return s.Subset(index)
}

// Valid returns all the rows with non-null values.
// Returns a new Series.
func (s *Series) Valid() *Series {
	index := s.values.valid()
	return s.Subset(index)
}

// Null returns all the rows with null values.
// Returns a new Series.
func (s *Series) Null() *Series {
	index := s.values.null()
	return s.Subset(index)
}

// Index stub
func (s *Series) Index(label string) ([]int, error) {
	ret, err := containsLabel(label, s.labels)
	if err != nil {
		return nil, fmt.Errorf("Index(): %v", err)
	}
	return ret, nil
}

// IndexRange stub
func (s *Series) IndexRange(firstLabel, lastLabel string) ([]int, error) {
	index1, err := containsLabel(firstLabel, s.labels)
	if err != nil {
		return nil, fmt.Errorf("IndexRange(): %v", err)
	}
	index2, err := containsLabel(lastLabel, s.labels)
	if err != nil {
		return nil, fmt.Errorf("IndexRange(): %v", err)
	}
	index := makeIntRange(minIntSlice(index1), maxIntSlice(index2)+1)
	return index, nil
}

// setters

// InPlace returns a SeriesMutator, which contains most of the same methods as Series but never returns a new Series.
// If you want to save memory and improve performance and do not need to preserve the original Series, consider using InPlace().
func (s *Series) InPlace() *SeriesMutator {
	return &SeriesMutator{series: s}
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a label level exists that matches `name`: rename the level to match `input`
//
// If a slice is supplied as `input` and a label level exists that matches `name`: replace the values at this level to match `input`
//
// If a slice is supplied as `input` and a label level does not exist that matches `name`: append a new label level with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying Series, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, returns a new Series.
func (s *Series) WithLabels(name string, input interface{}) *Series {
	s.Copy()
	s.InPlace().WithLabels(name, input)
	return s
}

// WithLabels resolves as follows:
//
// If a scalar string is supplied as `input` and a label level exists that matches `name`: rename the level to match `input`
//
// If a slice is supplied as `input` and a label level exists that matches `name`: replace the values at this level to match `input`
//
// If a slice is supplied as `input` and a label level does not exist that matches `name`: append a new label level with a name matching `name` and values matching `input`
//
// Error conditions: supplying slice of unsupported type, supplying slice with a different length than the underlying Series, or supplying scalar string and `name` that does not match an existing label level.
// In all cases, modifies the underlying Series in place.
func (s *SeriesMutator) WithLabels(name string, input interface{}) {
	switch reflect.TypeOf(input).Kind() {

	case reflect.String:
		index := labelWithName(name, s.series.labels)
		if index == -1 {
			s.series.resetWithError(fmt.Errorf("WithLabels(): cannot rename label level: name (%v) does not match any existing level", name))
			return
		}
		s.series.labels[index].name = input.(string)
		return
	case reflect.Slice:
		isNull := setNullsFromInterface(input)
		if isNull == nil {
			s.series.resetWithError(fmt.Errorf("WithLabels(): unable to calculate null values ([]%v not supported)", reflect.TypeOf(input).Elem()))
			return
		}
		if l := reflect.ValueOf(input).Len(); l != s.series.Len() {
			s.series.resetWithError(fmt.Errorf(
				"WithLabels(): cannot replace labels in level %s: length of input does not match length of Series (%d != %d)", name, l, s.series.Len()))
			return
		}
		index := labelWithName(name, s.series.labels)
		if index >= 0 {
			s.series.labels[index].slice = input
			s.series.labels[index].isNull = isNull
		} else if index == -1 {
			s.series.labels = append(s.series.labels, &valueContainer{slice: input, name: name, isNull: isNull})
		}
	default:
		s.series.resetWithError(fmt.Errorf("WithLabels(): unsupported input kind: must be either slice or string"))
	}
}

// WithRow stub
func (s *Series) WithRow(label string, values interface{}) *Series {
	return nil
}

// Drop removes the row at the specified index.
// Returns a new Series.
func (s *Series) Drop(index int) *Series {
	s.Copy()
	s.InPlace().Drop(index)
	return s
}

// Drop removes the row at the specified index.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Drop(index int) {
	err := s.series.values.dropRow(index)
	if err != nil {
		s.series.resetWithError(fmt.Errorf("Drop(): %v", err))
		return
	}
	for j := 0; j < len(s.series.labels); j++ {
		s.series.labels[j].dropRow(index)
	}
}

// DropNull removes the null values in the Series.
// Returns a new Series.
func (s *Series) DropNull() *Series {
	s.Copy()
	s.InPlace().DropNull()
	return s
}

// DropNull removes the null values in the Series.
// Modifies the underlying Series in place.
func (s *SeriesMutator) DropNull() {
	index := s.series.values.valid()
	s.Subset(index)
	return
}

// Name modifies the name of a Series in place and returns the original Series.
func (s *Series) Name(name string) *Series {
	s.values.name = name
	return s
}

// sort

// Sort removes the null values in the Series.
// Returns a new Series.
func (s *Series) Sort(by ...Sorter) *Series {
	s.Copy()
	s.InPlace().Sort(by...)
	return s
}

// Sort stub
func (s *SeriesMutator) Sort(by ...Sorter) {
	// must copy the values to sort to avoid prematurely overwriting underlying data
	// original index
	index := makeIntRange(0, s.series.Len())
	var vals *valueContainer
	// handle default (values as Float in ascending order)
	if len(by) == 0 {
		vals = s.series.values.copy()
		index = vals.sort(Float, false, index)
		s.Subset(index)
		return
	}
	for i := len(by) - 1; i >= 0; i-- {
		// empty ColName -> use Series values
		if by[i].ColName == "" {
			vals = s.series.values.copy()
			vals.subsetRows(index)
		} else {
			lvl := labelWithName(by[i].ColName, s.series.labels)
			if lvl == -1 {
				s.series.resetWithError(fmt.Errorf(
					"Sort(): cannot use label level: name (%v) does not match any existing level", by[i].ColName))
				return
			}
			vals = s.series.labels[lvl].copy()
			vals.subsetRows(index)
		}
		index = vals.sort(by[i].DType, by[i].Descending, index)
	}
	s.Subset(index)
	return
}

// combine

// Merge stub
func (s *Series) Merge(other *Series) *Series {
	return nil
}

// Lookup stub
func (s *Series) Lookup(other *Series, how string, leftOn string, rightOn string) *Series {
	return nil
}

// Add stub
func (s *Series) Add(other *Series) *Series {
	return nil
}

// Subtract stub
func (s *Series) Subtract(other *Series) *Series {
	return nil
}

// Multiply stub
func (s *Series) Multiply(other *Series) *Series {
	return nil
}

// Divide stub
func (s *Series) Divide(other *Series) *Series {
	return nil
}

// grouping

// GroupBy stub
func (s *Series) GroupBy(string) *GroupedSeries {
	return nil
}

// iterator

// IterRows stub
func (s *Series) IterRows() []map[string]Element {
	return nil
}

// math

// Sum stub
func (s *Series) Sum() float64 {
	return 0
}

// Mean stub
func (s *Series) Mean() float64 {
	return 0
}

// Median stub
func (s *Series) Median() float64 {
	return 0
}

// Std stub
func (s *Series) Std() float64 {
	return 0
}
