package tada

// constructors

// NewSeries stub
func NewSeries(slice interface{}, labels ...interface{}) *Series {
	return nil
}

// Copy stub
func (s *Series) Copy() *Series {
	return nil
}

// getters

// Subset stub
func (s *Series) Subset(index []int) *Series {
	return nil
}

// LabelsSubset stub
func (s *Series) LabelsSubset([]int) *Series {
	return nil
}

// Head stub
func (s *Series) Head(rows int) *Series {
	return nil
}

// Tail stub
func (s *Series) Tail(rows int) *Series {
	return nil
}

// Valid stub
func (s *Series) Valid() *Series {
	return nil
}

// Null stub
func (s *Series) Null() *Series {
	return nil
}

// Index stub
func (s *Series) Index(label string) []int {
	return nil
}

// IndexRange stub
func (s *Series) IndexRange(firstLabel, lastLabel string) []int {
	return nil
}

// setters

// WithLabels stub
func (s *Series) WithLabels(name string, slice interface{}) *Series {
	return nil
}

// WithRow stub
func (s *Series) WithRow(label string, values interface{}) *Series {
	return nil
}

// Drop stub
func (s *Series) Drop([]int) *Series {
	return nil
}

// DropNull stub
func (s *Series) DropNull() *Series {
	return nil
}

// Name stub
// in place
func (s *Series) Name() {
	return
}

// Swap stub
// in place
func (s *Series) Swap(i, j int) {
	return
}

// sort

// Sort stub
func (s *Series) Sort(...Sorter) *Series {
	return nil
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

// Len stub
func (s *Series) Len() int {
	return 0
}
