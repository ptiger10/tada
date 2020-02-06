package tada

import (
	"errors"
	"strings"
)

// GroupedSeries

// Err returns the underlying error, if any
func (g *GroupedSeries) Err() error {
	return g.err
}

func splitLabelIntoLevels(label string) []string {
	return strings.Split(label, "|")
}

func (g *GroupedSeries) mathFunc(fn func(val []float64, isNull []bool, index []int) (float64, bool)) *Series {
	if len(g.groups) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	vals := make([]float64, len(g.groups))
	sampleKey := g.orderedKeys[0]
	splitSampleKey := splitLabelIntoLevels(sampleKey)
	labelLevels := make([][]string, len(splitSampleKey))
	labelIsNull := make([][]bool, len(splitSampleKey))
	valueIsNull := make([]bool, len(g.groups))
	for lvl := range splitSampleKey {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
	}

	for rowNumber, key := range g.orderedKeys {
		output, isNull := fn(g.series.SliceFloat64(), g.series.values.isNull, g.groups[key])
		vals[rowNumber] = output
		valueIsNull[rowNumber] = isNull
		splitKey := splitLabelIntoLevels(key)
		for j := range splitKey {
			labelLevels[j][rowNumber] = splitKey[j]
			labelIsNull[j][rowNumber] = false
		}
	}
	labels := make([]*valueContainer, len(labelLevels))
	for j := range labelLevels {
		labels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j]}
	}
	return &Series{
		values: &valueContainer{slice: vals, isNull: valueIsNull},
		labels: labels,
	}
}

// Sum stub
func (g *GroupedSeries) Sum() *Series {

	return g.mathFunc(func(v []float64, isNull []bool, index []int) (float64, bool) {
		var sum float64
		var atLeastOneValid bool
		for _, i := range index {
			if !isNull[i] {
				sum += v[i]
				atLeastOneValid = true
			}
		}
		if !atLeastOneValid {
			return 0, true
		}
		return sum, false
	})
}
