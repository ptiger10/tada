package tada

import (
	"time"
)

type valueContainer struct {
	slice  interface{}
	isNull []bool
	name   string
}

// Series stub
type Series struct {
	values     *valueContainer
	labels     []*valueContainer
	sharedData bool
	err        error
}

// SeriesMutator stub
type SeriesMutator struct {
	series *Series
}

// DataFrameMutator stub
type DataFrameMutator struct {
	dataframe *DataFrame
}

// DataFrame stub
type DataFrame struct {
	labels        []*valueContainer
	values        []*valueContainer
	name          string
	err           error
	colLevelNames []string
}

// Matrix is an interface which is compatible with gonum's mat.Matrix interface
type Matrix interface {
	Dims() (r, c int)
	At(i, j int) float64
}

type floatValueContainer struct {
	slice  []float64
	isNull []bool
	index  []int
}

type stringValueContainer struct {
	slice  []string
	isNull []bool
	index  []int
}

type dateTimeValueContainer struct {
	slice  []time.Time
	isNull []bool
	index  []int
}

// Sorter stub
type Sorter struct {
	ContainerName string
	Descending    bool
	DType         DType
}

// Element stub
type Element struct {
	Val    interface{}
	IsNull bool
}

// NullFiller fills every row with a null value and changes the row status to not-null.
// If multiple fields are provided, resolves in the following order:
// 1) FillForward - fills with the last valid value,
// 2) FillBackward - fills with the next valid value,
// 3) FillZero - fills with the zero type of the slice,
// 4) FillFloat - coerces to float64 and fills with the value provided.
type NullFiller struct {
	FillForward  bool
	FillBackward bool
	FillZero     bool
	FillFloat    float64
}

// FilterFn stub
type FilterFn struct {
	F64      func(val float64) bool
	String   func(val string) bool
	DateTime func(val time.Time) bool
}

// ApplyFn stub
type ApplyFn struct {
	F64      func(val float64) float64
	String   func(val string) string
	DateTime func(val time.Time) time.Time
}

// GroupApplyFn stub
type GroupApplyFn struct {
	F64      func(vals []float64) float64
	String   func(vals []string) string
	DateTime func(vals []time.Time) time.Time
}

// ApplyFormatFn stub
type ApplyFormatFn struct {
	F64      func(val float64) string
	DateTime func(val time.Time) string
}

// GroupedSeries stub
type GroupedSeries struct {
	orderedKeys []string
	rowIndices  [][]int
	labels      []*valueContainer
	series      *Series
	aligned     bool
	err         error
}

// GroupedDataFrame stub
type GroupedDataFrame struct {
	orderedKeys []string
	rowIndices  [][]int
	labels      []*valueContainer
	df          *DataFrame
	err         error
}

// DType is a DataType that may be used in a Sorter
type DType int

const (
	// Float stub
	Float DType = iota
	// String stub
	String
	// DateTime stub
	DateTime
)

// ReadConfig stub
type ReadConfig struct {
	NumHeaderRows  int
	NumLabelCols   int
	MajorDimIsCols bool
}

// Resampler stub
type Resampler struct {
	ByYear      bool
	ByMonth     bool
	ByDay       bool
	ByWeek      bool
	StartOfWeek time.Weekday
	ByDuration  time.Duration
	Location    *time.Location
}
