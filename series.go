package tada

import (
	"fmt"
	"log"
	"math/rand"
	"reflect"
	"time"

	"github.com/ptiger10/tablediff"
)

// -- CONSTRUCTORS

// NewSeries constructs a Series from a slice of values and optional label slices.
// // Slice and all labels must be supported slices.
//
// If no labels are supplied, a default label level is inserted ([]int incrementing from 0).
// Series values are named 0 by default. The default values name is displayed on printing.
// Label levels are named *n (e.g., *0, *1, etc) by default. Default label names are hidden on printing.
//
// Supported slice types: all variants of []float, []int, & []uint,
// []string, []bool, []time.Time, []interface{},
// and 2-dimensional variants of each (e.g., [][]string, [][]float64).
func NewSeries(slice interface{}, labels ...interface{}) *Series {
	if slice == nil && labels == nil {
		return seriesWithError(fmt.Errorf("constructing new Series: slice and labels cannot both be nil"))
	}
	values := new(valueContainer)
	var err error
	if slice != nil {
		// handle values
		values, err = makeValueContainerFromInterface(slice, "0")
		if err != nil {
			return seriesWithError(fmt.Errorf("constructing new Series: slice: %v", err))
		}
	}

	// handle labels
	retLabels, err := makeValueContainersFromInterfaces(labels, true)
	if err != nil {
		return seriesWithError(fmt.Errorf("constructing new Series: labels: %v", err))
	}
	// default labels?
	if len(retLabels) == 0 {
		defaultLabels := makeDefaultLabels(0, reflect.ValueOf(slice).Len(), true)
		retLabels = append(retLabels, defaultLabels)
	}

	// ensure equal-lengthed slices
	var requiredLength int
	if values.slice != nil {
		requiredLength = values.len()
	} else {
		// handle null values case
		requiredLength = retLabels[0].len()
	}
	err = ensureEqualLengths(retLabels, requiredLength)
	if err != nil {
		return seriesWithError(fmt.Errorf("constructing new Series: labels: %v", err))
	}

	return &Series{values: values, labels: retLabels}
}

// Copy returns a deep copy of a Series with no shared references to the original.
func (s *Series) Copy() *Series {
	return &Series{
		values:     s.values.copy(),
		labels:     copyContainers(s.labels),
		err:        s.err,
		sharedData: false,
	}
}

// DataFrame converts a Series to a 1-column DataFrame.
func (s *Series) DataFrame() *DataFrame {
	s = s.Copy()
	return &DataFrame{
		values:        []*valueContainer{s.values},
		labels:        s.labels,
		colLevelNames: []string{"*0"},
		err:           s.err,
	}
}

// EqualRecords reduces s to [][]string records, reads [][]string records from want,
// and evaluates whether the stringified values match.
// If they do not match, returns a tablediff.Differences object that can be printed to isolate their differences.
func (s *Series) EqualRecords(got RecordWriter, want CSVReader) (bool, *tablediff.Differences, error) {
	df := s.DataFrame()
	return df.EqualRecords(got, want)
}

// // CSV converts a Series to a DataFrame and returns as [][]string.
// func (s *Series) CSV(options ...WriteOption) ([][]string, error) {
// 	if s.values == nil {
// 		return nil, fmt.Errorf("converting to csv: cannot export empty Series")
// 	}
// 	df := &DataFrame{
// 		values:        []*valueContainer{s.values},
// 		labels:        s.labels,
// 		colLevelNames: []string{"*0"},
// 		err:           s.err,
// 	}
// 	csv := df.CSVRecords(options...)
// 	return csv, nil
// }

// // Struct writes the values of the df containers into structPointer.
// // Returns an error if df does not contain, from left-to-right, the same container names and types
// // as the exported fields that appear, from top-to-bottom, in structPointer.
// // Exported struct fields must be types that are supported by NewDataFrame().
// // If a "tada" tag is present with the value "isNull", this field must be [][]bool.
// // The null status of each value container in the DataFrame, from left-to-right, will be written into this field in equal-lengthed slices.
// // If df contains additional containers beyond those in structPointer, those are ignored.
// func (s *Series) Struct(structPointer interface{}, options ...WriteOption) error {
// 	df := s.DataFrame()
// 	return df.Struct(structPointer, options...)
// }

// // WriteCSV converts a DataFrame to a csv with rows as the major dimension,
// // and writes the output to w.
// // Null values are replaced with "(null)".
// func (s *Series) WriteCSV(w io.Writer, options ...WriteOption) error {
// 	df := s.DataFrame()
// 	return df.WriteCSV(w, options...)
// }

// -- GETTERS

func (s *Series) String() string {
	if s.err != nil {
		return fmt.Sprintf("Error: %v", s.err)
	}
	return s.DataFrame().String()
}

// Err returns the most recent error attached to the Series, if any.
func (s *Series) Err() error {
	return s.err
}

// At returns the Element at the index position. If index is out of range, returns nil.
func (s *Series) At(index int) *Element {
	if index >= s.Len() {
		return nil
	}
	v := reflect.ValueOf(s.values.slice)
	return &Element{
		Val:    v.Index(index).Interface(),
		IsNull: s.values.isNull[index],
	}
}

// Len returns the number of rows in the Series.
func (s *Series) Len() int {
	return reflect.ValueOf(s.values.slice).Len()
}

// numLevels returns the number of label levels in the Series.
func (s *Series) numLevels() int {
	return len(s.labels)
}

// Cast casts the underlying container values (either label levels or Series values) to
// []float64, []string, []time.Time (aka timezone-aware DateTime), []civil.Date, or []civil.Time.
// To apply to Series values, supply empty string name ("") or the Series name.
// Use cast to improve performance when calling multiple operations on values.
func (s *Series) Cast(containerAsType map[string]DType) {
	mergedLabelsAndValues := append(s.labels, s.values)
	for name, dtype := range containerAsType {
		if name == "" {
			name = s.values.name
		}
		index, err := indexOfContainer(name, mergedLabelsAndValues)
		if err != nil {
			s.resetWithError(fmt.Errorf("type casting: %v", err))
			return
		}
		mergedLabelsAndValues[index].cast(dtype)
	}
	return
}

// NameOfLabel returns the name of the label level at index position n.
// If n is out of range, returns "-out of range-"
func (s *Series) NameOfLabel(n int) string {
	return nameOfContainer(s.labels, n)
}

// IndexOfLabel returns the index position of the first label level with a name matching name (case-sensitive).
// If name does not match any container, -1 is returned.
func (s *Series) IndexOfLabel(name string) int {
	i, err := indexOfContainer(name, s.labels)
	if err != nil {
		return -1
	}
	return i
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Returns a new Series.
func (s *Series) Subset(index []int) *Series {
	s = s.Copy()
	err := s.InPlace().Subset(index)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// Subset returns only the rows specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Subset(index []int) error {
	err := s.series.values.subsetRows(index)
	if err != nil {
		return fmt.Errorf("subsetting rows: %v", err)
	}
	for j := range s.series.labels {
		s.series.labels[j].subsetRows(index)
	}
	return nil
}

// SwapLabels swaps the label levels with names i and j.
// Returns a new Series.
func (s *Series) SwapLabels(i, j string) *Series {
	s = s.Copy()
	err := s.InPlace().SwapLabels(i, j)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// SwapLabels swaps the label levels with names i and j.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SwapLabels(i, j string) error {
	index1, err := indexOfContainer(i, s.series.labels)
	if err != nil {
		return fmt.Errorf("swapping labels: i: %v", err)
	}
	index2, err := indexOfContainer(j, s.series.labels)
	if err != nil {
		return fmt.Errorf("swapping labels: j: %v", err)
	}
	s.series.labels[index1], s.series.labels[index2] = s.series.labels[index2], s.series.labels[index1]
	return nil
}

// SubsetLabels includes only the columns of labels specified at the index positions, in the order specified.
// Returns a new Series.
func (s *Series) SubsetLabels(index []int) *Series {
	s = s.Copy()
	err := s.InPlace().SubsetLabels(index)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// SubsetLabels includes only the columns of labels specified at the index positions, in the order specified.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SubsetLabels(index []int) error {
	labels, err := subsetContainers(s.series.labels, index)
	if err != nil {
		return fmt.Errorf("subsetting labels: %v", err)
	}
	s.series.labels = labels
	return nil
}

// Head returns the first n rows of the Series. If n is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Head(n int) *Series {
	if s.Len() < n {
		n = s.Len()
	}
	retVals := s.values.head(n)
	retLabels := make([]*valueContainer, s.numLevels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].head(n)
	}
	return &Series{values: retVals, labels: retLabels}
}

// Tail returns the last n rows of the Series. If n is greater than the length of the Series, returns the entire Series.
// In either case, returns a new Series.
func (s *Series) Tail(n int) *Series {
	if s.Len() < n {
		n = s.Len()
	}
	retVals := s.values.tail(n)
	retLabels := make([]*valueContainer, s.numLevels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].tail(n)
	}

	return &Series{values: retVals, labels: retLabels}
}

// Range returns the rows of the Series starting at first and ending immediately prior to last (left-inclusive, right-exclusive).
// If either first or last is out of range, a Series error is returned.
// In all cases, returns a new Series.
func (s *Series) Range(first, last int) *Series {
	if first > last {
		return seriesWithError(fmt.Errorf("range: first is greater than last (%d > %d)", first, last))
	}
	if first >= s.Len() {
		return seriesWithError(fmt.Errorf("range: first index out of range (%d > %d)", first, s.Len()-1))
	} else if last > s.Len() {
		return seriesWithError(fmt.Errorf("range: last index out of range (%d > %d)", last, s.Len()))
	}
	retVals := s.values.rangeSlice(first, last)
	retLabels := make([]*valueContainer, s.numLevels())
	for j := range s.labels {
		retLabels[j] = s.labels[j].rangeSlice(first, last)
	}
	return &Series{values: retVals, labels: retLabels}
}

// FillNull fills all the null values and makes them not-null.
// Returns a new Series.
func (s *Series) FillNull(how NullFiller) *Series {
	s = s.Copy()
	s.InPlace().FillNull(how)
	return s
}

// FillNull fills all the null values and makes them not-null.
// Modifies the underlying Series.
func (s *SeriesMutator) FillNull(how NullFiller) {
	s.series.values.fillnull(how)
	return
}

// DropNull returns all the rows with non-null values.
// Returns a new Series.
func (s *Series) DropNull() *Series {
	s = s.Copy()
	s.InPlace().DropNull()
	return s
}

// DropNull returns all the rows with non-null values.
// Modifies the underlying Series.
func (s *SeriesMutator) DropNull() {
	index := s.series.values.valid()
	s.Subset(index)
}

// IsNull returns all the rows with null values.
// Returns a new Series.
func (s *Series) IsNull() *Series {
	index := s.values.null()
	return s.Subset(index)
}

// Shift replaces the value in row i with the value in row i - n, or null if that index is out of range.
// Returns a new Series.
func (s *Series) Shift(n int) *Series {
	s = s.Copy()
	s.InPlace().Shift(n)
	return s
}

// Shift replaces the value in row i with the value in row i - n, or null if that index is out of range.
// // Modifies the underlying Series.
func (s *SeriesMutator) Shift(n int) {
	if s.series.Len() < n {
		n = s.series.Len()
	}
	s.series.values = s.series.values.shift(n)
}

// -- SETTERS

// InPlace returns a SeriesMutator, which contains most of the same methods as Series but never returns a new Series.
// If you want to save memory and improve performance and do not need to preserve the original Series, consider using InPlace().
func (s *Series) InPlace() *SeriesMutator {
	if optionWarnings && s.sharedData {
		log.Print(
			"Shared Data Warning: this Series shares its labels and/or values with the object " +
				"from which it was derived (via Col(), SelectLabels() or Align())," +
				"so InPlace changes will modify the original object too. " +
				"To avoid this, make a new Series with Series.Copy()")
	}
	return &SeriesMutator{series: s}
}

// WithLabels accepts a name and input, which must be string or slice. It resolves as follows:
//
// If a string is supplied as input and a label level exists that matches name: rename the level to match input.
// In this case, name must already exist.
//
// If a slice is supplied as input and a label level exists that matches name: replace the values at this level to match input.
// If a slice is supplied as input and a label level does not exist that matches name: append a new level with name matching name and values matching input.
// If input is a slice, it must be the same length as the underlying Series.
//
// In all cases, returns a new Series.
func (s *Series) WithLabels(name string, input interface{}) *Series {
	s = s.Copy()
	err := s.InPlace().WithLabels(name, input)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// WithLabels accepts a name and input, which must be string or slice. It resolves as follows:
//
// If a string is supplied as input and a label level exists that matches name: rename the level to match input.
// In this case, name must already exist.
//
// If a slice is supplied as input and a label level exists that matches name: replace the values at this level to match input.
// If a slice is supplied as input and a label level does not exist that matches name: append a new level with name matching name and values matching input.
// If input is a slice, it must be the same length as the underlying Series.
//
// In all cases, modifies the underlying Series in place.
func (s *SeriesMutator) WithLabels(name string, input interface{}) error {
	labels, err := withColumn(s.series.labels, name, input, s.series.Len())
	if err != nil {
		return fmt.Errorf("with labels: %v", err)
	}
	s.series.labels = labels
	return nil
}

// WithValues replaces the Series values with input.
// Input must be a slice of the same length as the original Series.
// Returns a new Series.
func (s *Series) WithValues(input interface{}) *Series {
	s = s.Copy()
	err := s.InPlace().WithValues(input)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// WithValues replaces the Series values with input.
// Input must be a slice of the same length as the original Series.
// Modifies the underlying Series.
func (s *SeriesMutator) WithValues(input interface{}) error {
	// synthesize a collection of valueContainers, ensuring that name already exists
	vals, err := withColumn([]*valueContainer{s.series.values}, s.series.values.name, input, s.series.Len())
	if err != nil {
		return fmt.Errorf("with values: %v", err)
	}
	s.series.values = vals[0]
	return nil
}

// Shuffle randomizes the row order of the Series.
// Returns a new Series.
func (s *Series) Shuffle(seed int64) *Series {
	s = s.Copy()
	s.InPlace().Shuffle(seed)
	return s
}

// Shuffle randomizes the row order of the Series.
// Modifies the underlying Series.
func (s *SeriesMutator) Shuffle(seed int64) {
	index := makeIntRange(0, s.series.Len())
	rand.Seed(seed)
	rand.Shuffle(len(index), func(i, j int) { index[i], index[j] = index[j], index[i] })
	s.Subset(index)
}

// DropRow removes the row at the specified index.
// Returns a new Series.
func (s *Series) DropRow(index int) *Series {
	s = s.Copy()
	err := s.InPlace().DropRow(index)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// DropRow removes the row at the specified index.
// Modifies the underlying Series in place.
func (s *SeriesMutator) DropRow(index int) error {
	err := s.series.values.dropRow(index)
	if err != nil {
		return fmt.Errorf("dropping row: %v", err)
	}
	for j := range s.series.labels {
		s.series.labels[j].dropRow(index)
	}
	return nil
}

// DropLabels removes the first label level matching name.
// Returns a new Series.
func (s *Series) DropLabels(name string) *Series {
	s = s.Copy()
	err := s.InPlace().DropLabels(name)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// DropLabels removes the first label level matching name.
// Modifies the underlying Series in place.
func (s *SeriesMutator) DropLabels(name string) error {
	newCols, err := dropFromContainers(name, s.series.labels)
	if err != nil {
		return fmt.Errorf("dropping labels: %v", err)
	}
	s.series.labels = newCols
	return nil
}

// Append adds the other labels and values as new rows to the Series.
// If the types of any container do not match, all the values in that container are coerced to string.
// Returns a new Series.
func (s *Series) Append(other *Series) *Series {
	s = s.Copy()
	err := s.InPlace().Append(other)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// Append adds the other labels and values as new rows to the Series.
// If the types of any container do not match, all the values in that container are coerced to string.
// Returns a new Series.
func (s *SeriesMutator) Append(other *Series) error {
	if len(other.labels) != len(s.series.labels) {
		return fmt.Errorf("append: other Series must have same number of label levels as original Series (%d != %d)",
			len(other.labels), len(s.series.labels))
	}
	for j := range s.series.labels {
		s.series.labels[j] = s.series.labels[j].append(other.labels[j])
	}
	s.series.values = s.series.values.append(other.values)
	return nil
}

// SetName modifies the name of a Series in place and returns the original Series.
func (s *Series) SetName(name string) *Series {
	s.values.name = name
	return s
}

// Relabel resets the Series labels to default labels (e.g., []int from 0 to df.Len()-1, with *0 as name).
// Returns a new Series.
func (s *Series) Relabel() *Series {
	s = s.Copy()
	s.InPlace().Relabel()
	return s
}

// Relabel resets the Series labels to default labels (e.g., []int from 0 to df.Len()-1, with *0 as name).
// Modifies the underlying Series in place.
func (s *SeriesMutator) Relabel() {
	s.series.labels = []*valueContainer{makeDefaultLabels(0, s.series.Len(), true)}
	return
}

// SetLabelNames sets the names of all the label levels in the Series and returns the entire Series.
// If an error is returned, it is written to the Series.
func (s *Series) SetLabelNames(levelNames []string) *Series {
	if len(levelNames) != len(s.labels) {
		return seriesWithError(
			fmt.Errorf("setting label names: number of levelNames must match number of levels in Series (%d != %d)", len(levelNames), len(s.labels)))
	}
	for j := range levelNames {
		s.labels[j].name = levelNames[j]
	}
	return s
}

// ListLabelNames returns the name and position of all the label levels in the Series
func (s *Series) ListLabelNames() []string {
	return listNames(s.labels)
}

// HasLabels returns an error if the Series does not contain all of the labelNames supplied.
func (s *Series) HasLabels(labelNames ...string) error {
	for _, name := range labelNames {
		_, err := indexOfContainer(name, s.labels)
		if err != nil {
			return fmt.Errorf("verifying labels: %v", err)
		}
	}
	return nil
}

// Name returns the name of the Series
func (s *Series) Name() string {
	return s.values.name
}

// -- SORT

// Sort sorts the values by zero or more Sorter specifications.
// If no Sorter is supplied, sorts by Series values (as float64) in ascending order.
// If a Sorter is supplied without a Name or with a name matching the Series name, sorts by Series values.
// If no DType is supplied in a Sorter, sorts as float64.
// DType is only used for the process of sorting. Once it has been sorted, data retains its original type.
// Returns a new Series.
func (s *Series) Sort(by ...Sorter) *Series {
	s = s.Copy()
	err := s.InPlace().Sort(by...)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// Sort sorts the values by zero or more Sorter specifications.
// If no Sorter is supplied, sorts by Series values (as float64) in ascending order.
// If a Sorter is supplied without a Name or with a name matching the Series name, sorts by Series values.
// If no DType is supplied in a Sorter, sorts as float64.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Sort(by ...Sorter) error {
	// default for handling no Sorters: values as Float64 in ascending order
	if len(by) == 0 {
		by = []Sorter{{Name: s.series.values.name, DType: Float64, Descending: false}}
	}
	// replace "" with values
	for i := range by {
		if by[i].Name == "" {
			by[i].Name = s.series.values.name
		}
	}
	mergedLabelsAndValues := append(s.series.labels, s.series.values)
	newIndex, err := sortContainers(mergedLabelsAndValues, by)
	if err != nil {
		return fmt.Errorf("sorting rows: %v", err)
	}
	// rearrange the data in place with the final index
	s.Subset(newIndex)
	return nil
}

// -- FILTERS

// FilterIndex returns the index positions of the rows in container (either the Series name or label name) that satsify filterFn.
// A filter that matches no rows returns empty []int. An out of range container returns nil.
// FilterIndex may be applied to the Series values by supplying either the Series name or an empty string ("") as a key.
func (s *Series) FilterIndex(container string, filterFn FilterFn) []int {
	mergedLabelsAndCols := append(s.labels, s.values)
	if container == "" {
		container = s.values.name
	}
	index, err := filter(mergedLabelsAndCols, map[string]FilterFn{container: filterFn})
	if err != nil {
		return nil
	}
	// no matches? convert from nil to empty slice
	if len(index) == 0 {
		return []int{}
	}
	return index
}

// Filter returns a new Series with only rows that satisfy all of the filters,
// which is a map of container names (either the Series name or label name) and anonymous functions.
// Filter may be applied to the Series values by supplying either the Series name or an empty string ("") as a key.
//
// Rows with null values never satsify a filter.
// If no filter is provided, function does nothing.
// For equality filtering on one or more containers, consider FilterByValue.
// Returns a new Series.
func (s *Series) Filter(filters map[string]FilterFn) *Series {
	s = s.Copy()
	err := s.InPlace().Filter(filters)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// Filter returns a new Series with only rows that satisfy all of the filters,
// which is a map of container names (either the Series name or label name) and anonymous functions.
// Filter may be applied to the Series values by supplying either the Series name or an empty string ("") as a key.
//
// Rows with null values never satsify a filter.
// If no filter is provided, function does nothing.
// For equality filtering on one or more containers, consider FilterByValue.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Filter(filters map[string]FilterFn) error {
	if len(filters) == 0 {
		return nil
	}
	// replace "" with values container name
	for k, v := range filters {
		if k == "" {
			filters[s.series.values.name] = v
			delete(filters, k)
		}
	}

	mergedLabelsAndValues := append(s.series.labels, s.series.values)
	index, err := filter(mergedLabelsAndValues, filters)
	if err != nil {
		return fmt.Errorf("filter: %v", err)
	}
	s.Subset(index)
	return nil
}

// Where iterates over the rows in s and evaluates whether each one satisfies filters,
// which is a map of container names (either the Series name or label name) and tada.FilterFn structs.
// If yes, returns ifTrue at that row position.
// If not, returns ifFalse at that row position.
// Values are coerced from their original type to the selected field type for filtering, but after filtering retains their original type.
//
// Returns an unnamed Series with a copy of the labels from the original Series and null status based on the supplied values.
// If an unsupported value type is supplied as either ifTrue or ifFalse, returns an error.
func (s *Series) Where(filters map[string]FilterFn, ifTrue, ifFalse interface{}) (*Series, error) {
	ret := make([]interface{}, s.Len())
	// []int of positions where all filters are true
	mergedLabelsAndValues := append(s.labels, s.values)
	index, err := filter(mergedLabelsAndValues, filters)
	if err != nil {
		return nil, fmt.Errorf("where: %v", err)
	}
	for _, i := range index {
		ret[i] = ifTrue
	}
	// []int of positions where any filters is not true
	inverseIndex := difference(makeIntRange(0, s.Len()), index)
	for _, i := range inverseIndex {
		ret[i] = ifFalse
	}
	// ducks error because ret is a supported slice
	isNull, _ := setNullsFromInterface(ret)
	return &Series{
		values: newValueContainer(ret, isNull, ""),
		labels: copyContainers(s.labels),
	}, nil
}

// FilterByValue returns the rows in the Series satisfying all filters,
// which is a map of of container names (either the Series name or label name) to interface{} values.
// A filter is satisfied for a given row value if the stringified value in that container at that row matches the stringified interface{} value.
// FilterByValue may be applied to the Series values by supplying either the Series name or an empty string ("") as a key.
// Returns a new Series.
func (s *Series) FilterByValue(filters map[string]interface{}) *Series {
	s = s.Copy()
	err := s.InPlace().FilterByValue(filters)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// FilterByValue returns the rows in the Series satisfying all filters,
// which is a map of of container names (either the Series name or label name) to interface{} values.
// A filter is satisfied for a given row value if the stringified value in that container at that row matches the stringified interface{} value.
// FilterByValue may be applied to the Series values by supplying either the Series name or an empty string ("") as a key.
// Modifies the underlying Series in place.
func (s *SeriesMutator) FilterByValue(filters map[string]interface{}) error {
	mergedLabelsAndValues := append(s.series.labels, s.series.values)
	index, err := filterByValue(mergedLabelsAndValues, filters)
	if err != nil {
		return fmt.Errorf("filtering rows by value: %v", err)
	}
	s.Subset(index)
	return nil
}

// -- APPLY

// Apply applies an anonymous function to every row in a container based on lambda,
// which is an anonymous function.
// A row's null status can be set in-place within the anonymous function by accessing the []bool argument.
// Returns a new Series.
func (s *Series) Apply(lambda ApplyFn) *Series {
	s = s.Copy()
	err := s.InPlace().Apply(lambda)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// Apply applies an anonymous function to every row in a container based on lambda,
// which is an anonymous function.
// A row's null status can be changed in-place within the anonymous function.
// Modifies the underlying Series in place.
func (s *SeriesMutator) Apply(lambda ApplyFn) error {
	err := lambda.validate()
	if err != nil {
		return fmt.Errorf("applying lambda function: %v", err)
	}
	err = s.series.values.apply(lambda, nil)
	if err != nil {
		return fmt.Errorf("applying lambda function: %v", err)
	}
	return nil
}

// SetRows applies lambda, an anonymous function,
// to set the values at the specified row positions.
// The new values must be the same type as the existing values.
// Returns a new Series.
func (s *Series) SetRows(lambda ApplyFn, rows []int) *Series {
	s = s.Copy()
	err := s.InPlace().SetRows(lambda, rows)
	if err != nil {
		return seriesWithError(err)
	}
	return s
}

// SetRows applies lambda, an anonymous function,
// to set the values at the specified row positions.
// The new values must be the same type as the existing values.
// Modifies the underlying Series in place.
func (s *SeriesMutator) SetRows(lambda ApplyFn, rows []int) error {
	err := lambda.validate()
	if err != nil {
		return fmt.Errorf("applying lambda to rows: %v", err)
	}
	err = s.series.values.apply(lambda, rows)
	if err != nil {
		return fmt.Errorf("applying lambda to rows: %v", err)
	}
	return nil
}

// -- MERGERS

// Lookup performs the lookup portion of a join of other onto df.
// Performs a left join unless a different join type is specified as an option.
// If left and right keys are supplied as options, those are used as lookup keys.
// Otherwise, the join will automatically use shared label names or return an error if none exist.
//
//
// Lookup identifies the row alignment between s and other and returns the aligned values.
// Rows are aligned when:
// 1) one or more containers (either column or label level) in other share the same name as one or more containers in s,
// and 2) the stringified values in the other containers match the values in the s containers.
// For the following dataframes:
//
// s    	other
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in s is "aligned" with row 0 in other, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// The result of a lookup will be:
//
// FOO BAR
// bar null
// baz corge
//
// Returns a new Series.
func (s *Series) Lookup(other *Series, options ...JoinOption) (*Series, error) {
	config := setJoinConfig(options)
	var leftKeys, rightKeys []int
	var err error
	if len(config.leftOn) == 0 || len(config.rightOn) == 0 {
		if !(len(config.leftOn) == 0 && len(config.rightOn) == 0) {
			return nil, fmt.Errorf("lookup: if either leftOn or rightOn is empty, both must be empty")
		}
	}
	// no join keys specified? find matching labels
	if len(config.leftOn) == 0 {
		leftKeys, rightKeys, err = findMatchingKeysBetweenTwoContainers(s.labels, other.labels)
		if err != nil {
			return nil, fmt.Errorf("lookup: %v", err)
		}
	} else {
		leftKeys, err = indexOfContainers(config.leftOn, s.labels)
		if err != nil {
			return nil, fmt.Errorf("lookup: leftOn: %v", err)
		}
		rightKeys, err = indexOfContainers(config.rightOn, other.labels)
		if err != nil {
			return nil, fmt.Errorf("lookup: rightOn: %v", err)
		}
	}

	ret, err := lookup(config.how, s.values, s.labels, leftKeys, other.values, other.labels, rightKeys)
	if err != nil {
		return nil, fmt.Errorf("lookup: %v", err)
	}
	return ret, nil
}

// Merge joins other onto s.
// Performs a left join unless a different join type is specified as an option.
// If left and right keys are supplied as options, those are used as lookup keys.
// Otherwise, the join will automatically use shared label names or return an error if none exist.
//
// Merge identifies the row alignment between s and other and appends aligned values as new columns on s.
// Rows are aligned when:
// 1) one or more containers (either column or label level) in other share the same name as one or more containers in s,
// and 2) the stringified values in the other containers match the values in the s containers.
// For the following dataframes:
//
// s    	other
// FOO BAR	FOO QUX
// bar 0	baz corge
// baz 1	qux waldo
//
// Row 1 in s is "aligned" with row 0 in other, because those are the rows in which
// both share the same value ("baz") in a container with the same name ("foo").
// After merging, the result will be:
//
// s
// FOO BAR QUX
// bar 0   null
// baz 1   corge
//
// Finally, all container names (either the Series name or label name) are deduplicated after the merge so that they are unique.
// Returns a new DataFrame.
func (s *Series) Merge(other *Series, options ...JoinOption) (*DataFrame, error) {
	return s.DataFrame().Merge(other.DataFrame(), options...)
}

// Add coerces other and s to float64 values, aligns other with s, and adds the values in aligned rows,
// using the labels in s as an anchor.
// If ignoreNulls is true, then missing or null values are treated as 0.
// Otherwise, if a row in s does not align with any row in other,
// or if row does align but either value is null, then the resulting value is null.
func (s *Series) Add(other *Series, ignoreNulls bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		return v1 + v2
	}
	return s.combineMath(other, ignoreNulls, fn)
}

// Subtract coerces other and s to float64 values, aligns other with s,
// and subtracts the aligned values of other from s,
// using the labels in s as an anchor.
// If ignoreNulls is true, then missing or null values are treated as 0.
// Otherwise, if a row in s does not align with any row in other,
// or if row does align but either value is null, then the resulting value is null.
func (s *Series) Subtract(other *Series, ignoreNulls bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		return v1 - v2
	}
	return s.combineMath(other, ignoreNulls, fn)
}

// Multiply coerces other and s to float64 values, aligns other with s, and multiplies the values in aligned rows,
// using the labels in s as an anchor.
// If ignoreNulls is true, then missing or null values are treated as 0.
// Otherwise, if a row in s does not align with any row in other,
// or if row does align but either value is null, then the resulting value is null.
func (s *Series) Multiply(other *Series, ignoreNulls bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		return v1 * v2
	}
	return s.combineMath(other, ignoreNulls, fn)
}

// Divide coerces other and s to float64 values, aligns other with s,
// and divides the aligned values of s by s,
// using the labels in s as an anchor.
// Dividing by 0 always returns a null value.
// If ignoreNulls is true, then missing or null values are treated as 0.
// Otherwise, if a row in s does not align with any row in other,
// or if row does align but either value is null, then the resulting value is null.
func (s *Series) Divide(other *Series, ignoreNulls bool) *Series {
	fn := func(v1 float64, v2 float64) float64 {
		defer func() {
			recover()
		}()
		return v1 / v2
	}
	return s.combineMath(other, ignoreNulls, fn)
}

// -- GROUPERS

// GroupBy groups the Series rows that share the same stringified value
// in the container(s) (columns or labels) specified by names.
// If error occurs, writes error to GroupedSeries.
func (s *Series) GroupBy(names ...string) *GroupedSeries {
	var index []int
	var err error
	// if no names supplied, group by all label levels
	if len(names) == 0 {
		index = makeIntRange(0, s.numLevels())
	} else {
		index, err = indexOfContainers(names, s.labels)
		if err != nil {
			return groupedSeriesWithError(fmt.Errorf("group by: %v", err))
		}
	}
	containers, _ := subsetContainers(s.labels, index)
	newLabels, rowIndices, orderedKeys := reduceContainers(containers)
	return &GroupedSeries{
		orderedKeys: orderedKeys,
		rowIndices:  rowIndices,
		labels:      newLabels,
		series:      s,
	}
}

// -- ITERATORS

// Iterator returns an iterator which may be used to access the values in each row as map[string]Element.
func (s *Series) Iterator() *SeriesIterator {
	return &SeriesIterator{
		current: -1,
		s:       s,
	}
}

// Next advances to next row. Returns false at end of iteration.
func (iter *SeriesIterator) Next() bool {
	iter.current++
	return iter.current < iter.s.Len()
}

// Row returns the current row in the Series as map[string]Element.
// The map keys are the names of containers (including label levels).
// The name of the Series values column is the same as the name of the Series itself.
// The value in each map is an Element containing an interface value and a boolean denoting if the value is null.
// If multiple columns have the same header, only the Elements of the left-most column are returned.
func (iter *SeriesIterator) Row() map[string]Element {
	ret := make(map[string]Element)
	ret[iter.s.values.name] = iter.s.values.iterRow(iter.current)
	for j := iter.s.numLevels() - 1; j >= 0; j-- {
		ret[iter.s.labels[j].name] = iter.s.labels[j].iterRow(iter.current)
	}
	return ret
}

// -- MATH

// Sum coerces the Series values float64 and sums them.
func (s *Series) Sum() float64 {
	return s.floatFunc(sum)
}

// Mean coerces the Series values to float64 and calculates the mean.
func (s *Series) Mean() float64 {
	return s.floatFunc(mean)
}

// Median coerces the Series values to float64 and calculates the median.
func (s *Series) Median() float64 {
	return s.floatFunc(median)
}

// StdDev coerces the Series values to float64 and calculates the standard deviation.
func (s *Series) StdDev() float64 {
	return s.floatFunc(std)
}

// Count counts the number of non-null Series values.
func (s *Series) Count() int {
	output, _ := count(s.values.slice, s.values.isNull, makeIntRange(0, s.Len()))
	return output
}

// NUnique counts the number of unique, non-null Series values.
func (s *Series) NUnique() int {
	output, _ := nunique(s.values.slice, s.values.isNull, makeIntRange(0, s.Len()))
	return output
}

// Min coerces the Series values to float64 and calculates the minimum.
func (s *Series) Min() float64 {
	return s.floatFunc(min)
}

// Max coerces the Series values to float64 and calculates the maximum.
func (s *Series) Max() float64 {
	return s.floatFunc(max)
}

// Earliest coerces the Series values to time.Time and calculates the earliest timestamp.
func (s *Series) Earliest() time.Time {
	return s.timeFunc(earliest)
}

// Latest coerces the Series values to time.Time and calculates the latest timestamp.
func (s *Series) Latest() time.Time {
	return s.timeFunc(latest)
}

func (s *Series) floatFunc(floatFunction func([]float64, []bool, []int) (float64, bool)) float64 {
	output, _ := floatFunction(
		s.values.float64().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return output
}

func (s *Series) stringFunc(stringFunction func([]string, []bool, []int) (string, bool)) string {
	output, _ := stringFunction(
		s.values.string().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return output
}

func (s *Series) timeFunc(timeFunction func([]time.Time, []bool, []int) (time.Time, bool)) time.Time {
	output, _ := timeFunction(
		s.values.dateTime().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return output
}

// aligns output with original series. analogous to Pandas transform concept
func (s *Series) alignedMath(alignedFunction func([]float64, []bool, []int) []float64) []float64 {
	retVals := alignedFunction(
		s.values.float64().slice,
		s.values.isNull,
		makeIntRange(0, s.Len()))
	return retVals
}

// Resample coerces the Series values to time.Time and truncates them by the logic supplied in tada.Resampler.
// If slice type is civil.Date or civil.Time before resampling, it will be returned as civil.Date or civil.Time after resampling.
//
// Returns a new Series.
func (s *Series) Resample(by Resampler) *Series {
	s = s.Copy()
	s.InPlace().Resample(by)
	return s
}

// Resample coerces the Series values to time.Time and truncates them by the logic supplied in tada.Resampler.
// If slice type is civil.Date or civil.Time before resampling, it will be returned as civil.Date or civil.Time after resampling.
//
// Modifies the underlying Series in place.
func (s *SeriesMutator) Resample(by Resampler) {
	s.series.values.resample(by)
	return
}

// CumSum coerces the Series values to float64 and returns the cumulative sum at each row position.
func (s *Series) CumSum() *Series {
	isNull := make([]bool, s.Len())
	for i := range isNull {
		isNull[i] = false // no null values possible
	}
	return &Series{
		values: newValueContainer(s.alignedMath(cumsum), isNull, "cumsum"),
		labels: s.labels,
	}
}

// Rank coerces the Series values to float64 and returns the rank of each (in ascending order - where 1 is the rank of the lowest value).
// Rows with the same value share the same rank.
func (s *Series) Rank() *Series {
	slice := s.alignedMath(rank)
	isNull := make([]bool, s.Len())
	for i, val := range slice {
		if val == -999 {
			isNull[i] = true
		} else {
			isNull[i] = false
		}
	}
	return &Series{
		values: newValueContainer(slice, isNull, "rank"),
		labels: s.labels,
	}
}

// Bin coerces the Series values to float64 and categorizes each row based on which bin interval it falls within.
// bins should be a slice of sequential edges that form intervals (left exclusive, right inclusive).
// For example, [1, 3, 5] represents the intervals 1-3 (excluding 1, including 3), and 3-5 (excluding 3, including 5).
// If these bins were supplied for a Series with values [3, 4], the returned Series would have values ["1-3", "3-5"].
// Null values are not categorized.
// For default behavior, supply nil as config.
//
// To bin values below or above the bin intervals, or to supply custom labels, supply a tada.Binner as config.
// If custom labels are supplied, the length must be 1 less than the total number of bin edges.
// Otherwise, bin labels are auto-generated from the bin intervals.
func (s *Series) Bin(bins []float64, config *Binner) (*Series, error) {
	if config == nil {
		config = &Binner{}
	}
	retSlice, err := s.values.cut(bins, config.AndLess, config.AndMore, config.Labels)
	if err != nil {
		return nil, fmt.Errorf("Bin(): %v", err)
	}
	// ducks error because values are []string
	nulls, _ := setNullsFromInterface(retSlice)
	return &Series{
		values: newValueContainer(retSlice, nulls, s.values.name),
		labels: s.labels,
	}, nil
}

// Percentile coerces the Series values to float64 returns the percentile rank of each value.
// Uses the "exclusive" definition: a value's percentile is the % of all non-null values in the Series (including itself) that are below it.
func (s *Series) Percentile() *Series {
	floats := s.values.float64()
	floats.index = makeIntRange(0, s.Len())
	retVals := floats.percentile()
	retNulls := make([]bool, len(retVals))
	for i := range retNulls {
		if retVals[i] == -999 {
			retNulls[i] = true
			retVals[i] = 0
		}
	}
	return &Series{
		values: newValueContainer(retVals, retNulls, "percentile"),
		labels: copyContainers(s.labels),
	}
}

// PercentileBin coerces the Series values to float64 and categorizes each value based on which percentile bin interval it falls within.
// Uses the "exclusive" definition: a value's percentile is the % of all non-null values in the Series (including itself) that are below it.
// bins should be a slice of sequential percentile edges (between 0 and 1) that form intervals (left inclusive, right exclusive).
// NB: left inclusive, right exclusive is the opposite of the interval inclusion rules for the Bin() function.
// For example, [0, .5, 1] represents the percentile intervals 0-50% (including 0%, excluding 50%) and 50%-100% (including 50%, excluding 100%).
// If these bins were supplied for a Series with values [1, 1000], the returned Series would have values [0-0.5, 0.5-1],
// because 1 is in the bottom 50% of values and 1000 is in the top 50% of values.
// Null values are not categorized.
// For default behavior, supply nil as config.
//
// To bin values below or above the bin intervals, or to supply custom labels, supply a tada.Binner as config.
// If custom labels are supplied, the length must be 1 less than the total number of bin edges.
// Otherwise, bin labels are auto-generated from the bin intervals.
func (s *Series) PercentileBin(bins []float64, config *Binner) (*Series, error) {
	retSlice, err := s.values.pcut(bins, config)
	if err != nil {
		return nil, fmt.Errorf("percentile bin: %v", err)
	}
	// ducks error because values are []string
	nulls, _ := setNullsFromInterface(retSlice)
	return &Series{
		values: newValueContainer(retSlice, nulls, s.values.name),
		labels: s.labels,
	}, nil
}

// -- Slicers

// GetValuesAsFloat64 coerces the Series values into []float64.
func (s *Series) GetValuesAsFloat64() []float64 {
	output := make([]float64, s.Len())
	copy(output, s.values.float64().slice)
	return output
}

// GetValuesAsString coerces the Series values into []string.
func (s *Series) GetValuesAsString() []string {
	output := make([]string, s.Len())
	copy(output, s.values.string().slice)
	return output

}

// GetValuesAsTime coerces the Series values into []time.Time.
func (s *Series) GetValuesAsTime() []time.Time {
	output := make([]time.Time, s.Len())
	copy(output, s.values.dateTime().slice)
	return output
}

// GetNulls returns whether each value is null or not.
func (s *Series) GetNulls() []bool {
	output := make([]bool, s.Len())
	copy(output, s.values.isNull)
	return output
}

// GetValues returns a copy of the underlying Series data as an interface.
func (s *Series) GetValues() interface{} {
	ret := s.values.copy()
	return ret.slice
}

// GetLabels returns label levels as interface{} slices within an []interface
// that may be supplied as optional labels argument to NewSeries() or NewDataFrame().
func (s *Series) GetLabels() []interface{} {
	var ret []interface{}
	labels := copyContainers(s.labels)
	for j := range labels {
		ret = append(ret, labels[j].slice)
	}
	return ret
}

// Type returns the slice type of the underlying Series values
func (s *Series) Type() reflect.Type {
	return s.values.dtype()
}

// ValueCounts counts the number of appearances of each stringified value in the Series.
func (s *Series) ValueCounts() map[string]int {
	return s.values.valueCounts()
}

// Unique returns the first appearance of all non-null values in the Series.
// If includeLabels is true, a row is considered unique only if its combination of labels and values is unique.
// Returns a new Series.
func (s *Series) Unique(includeLabels bool) *Series {
	var index []int
	if !includeLabels {
		index = s.values.uniqueIndex()
	} else {
		mergedLabelsAndValues := append(s.labels, s.values)
		index = multiUniqueIndex(mergedLabelsAndValues)
	}
	return s.Subset(index)
}

// Reduce reduces all Series values to a single value and null status using lambda.
func (s *Series) Reduce(lambda ReduceFn) (value interface{}, isNull bool) {
	err := lambda.validate()
	if err != nil {
		return nil, true
	}
	return lambda(s.values.slice, s.values.isNull)
}
