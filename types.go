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
}

// Elements stub
type Elements struct {
	vals   []interface{}
	isNull []bool
}

// GroupedSeries stub
type GroupedSeries struct {
	groups    map[string][]int
	reference *Series
	Err       error
}
