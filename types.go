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
	values *valueContainer
	labels []*valueContainer
	err    error
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
	ColName    string
	Descending bool
	DType      DType
}

// Element stub
type Element struct {
	val    interface{}
	isNull bool
}

// FilterFn stub
type FilterFn struct {
	F64      func(val float64) bool
	String   func(val string) bool
	DateTime func(val time.Time) bool
	ColName  string
}

// ApplyFn stub
type ApplyFn struct {
	F64      func(val float64) float64
	String   func(val string) string
	DateTime func(val time.Time) time.Time
	ColName  string
}

// ApplyFormatFn stub
type ApplyFormatFn struct {
	F64      func(val float64) string
	DateTime func(val time.Time) string
	ColName  string
}

// GroupedSeries stub
type GroupedSeries struct {
	groups      map[string][]int
	orderedKeys []string
	series      *Series
	labelNames  []string
	aligned     bool
	err         error
}

// GroupedDataFrame stub
type GroupedDataFrame struct {
	groups      map[string][]int
	orderedKeys []string
	df          *DataFrame
	labelNames  []string
	err         error
}

// DType stub
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
