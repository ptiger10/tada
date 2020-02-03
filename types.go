package tada

import "time"

type valueContainer struct {
	slice  interface{}
	name   string
	isNull []bool
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

// DataFrame stub
type DataFrame struct {
	labels []*valueContainer
	values []*valueContainer
	name   string
	err    error
}

// FloatValueContainer stub
type FloatValueContainer struct {
	slice  []float64
	isNull []bool
}

// StringValueContainer stub
type StringValueContainer struct {
	slice  []string
	isNull []bool
}

// DateTimeValueContainer stub
type DateTimeValueContainer struct {
	slice  []time.Time
	isNull []bool
}

// Sorter stub
type Sorter struct {
	colName   string
	ascending bool
	dtype     DType
}

// Element stub
type Element struct {
	val    interface{}
	isNull bool
}

// GroupedSeries stub
type GroupedSeries struct {
	groups    map[string][]int
	reference *Series
	Err       error
}

// GroupedDataFrame stub
type GroupedDataFrame struct {
	groups    map[string][]int
	reference *DataFrame
	Err       error
}

// DType stub
type DType int

const (
	// Float stub
	Float DType = iota
	// Str stub
	Str
	// DateTime stub
	DateTime
)

// Dimension stub
type Dimension int

const (
	// Columns stub
	Columns Dimension = iota
	// Rows stub
	Rows
)

func (dim Dimension) String() string {
	return [...]string{"columns", "rows"}[dim]
}
