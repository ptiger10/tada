package tada

import (
	"errors"
	"sort"
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

// func (g *GroupedSeries) math(func(val []float64, isNull bool) float64) *Series {
// if len(g.groups) == 0 {
// 	return seriesWithError(errors.New("GroupBy(): no groups"))
// }
// vals := make([]float64, len(g.groups))
// var orderedKeys []string
// for key := range g.groups {
// 	orderedKeys = append(orderedKeys, key)
// }
// sampleKey := orderedKeys[0]
// splitSampleKey := splitLabelIntoLevels(sampleKey)
// labelLevels := make([][]string, len(splitSampleKey))
// labelIsNull := make([][]bool, len(splitSampleKey))
// valueIsNull := make([]bool, len(g.groups))
// for lvl := range splitSampleKey {
// 	labelLevels[lvl] = make([]string, len(g.groups))
// 	labelIsNull[lvl] = make([]bool, len(g.groups))
// }

// floatVals := g.series.SliceFloat64()
// sort.Strings(orderedKeys)
// for rowNumber, key := range orderedKeys {
// 	var atLeastOneValid bool
// 	var output float64
// 	for _, i := range g.groups[key] {
// 		val := floatVals[i]
// 		isNull := g.series.values.isNull[i]
// 		fn(val, isNull)
// 		if !g.series.values.isNull[i] {
// 			sum += floatVals[i]
// 			atLeastOneValid = true
// 		}
// 		if atLeastOneValid {
// 			valueIsNull[rowNumber] = false
// 		} else {
// 			valueIsNull[rowNumber] = true
// 		}
// 	}
// 	vals[rowNumber] = sum
// 	splitKey := splitLabelIntoLevels(key)
// 	for j := range splitKey {
// 		labelLevels[j][rowNumber] = splitKey[j]
// 		labelIsNull[j][rowNumber] = false
// 	}
// }
// labels := make([]*valueContainer, len(labelLevels))
// for j := range labelLevels {
// 	labels[j] = &valueContainer{slice: labelLevels[j], isNull: labelIsNull[j]}
// }
// return &Series{
// 	values: &valueContainer{slice: vals, isNull: valueIsNull},
// 	labels: labels,
// }
// }

// Sum stub
func (g *GroupedSeries) Sum() *Series {
	if len(g.groups) == 0 {
		return seriesWithError(errors.New("GroupBy(): no groups"))
	}
	vals := make([]float64, len(g.groups))
	var orderedKeys []string
	for key := range g.groups {
		orderedKeys = append(orderedKeys, key)
	}
	sampleKey := orderedKeys[0]
	splitSampleKey := splitLabelIntoLevels(sampleKey)
	labelLevels := make([][]string, len(splitSampleKey))
	labelIsNull := make([][]bool, len(splitSampleKey))
	valueIsNull := make([]bool, len(g.groups))
	for lvl := range splitSampleKey {
		labelLevels[lvl] = make([]string, len(g.groups))
		labelIsNull[lvl] = make([]bool, len(g.groups))
	}

	floatVals := g.series.SliceFloat64()
	sort.Strings(orderedKeys)
	for rowNumber, key := range orderedKeys {
		var atLeastOneValid bool
		var sum float64
		for _, i := range g.groups[key] {
			if !g.series.values.isNull[i] {
				sum += floatVals[i]
				atLeastOneValid = true
			}
			if atLeastOneValid {
				valueIsNull[rowNumber] = false
			} else {
				valueIsNull[rowNumber] = true
			}
		}
		vals[rowNumber] = sum
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
