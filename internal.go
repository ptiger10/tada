package tada

import (
	"fmt"
	"math"
	"reflect"
	"sort"
	"strings"
	"time"
)

func (s *Series) resetWithError(err error) {
	s.values = nil
	s.labels = nil
	s.err = err
}

func seriesWithError(err error) *Series {
	return &Series{
		err: err,
	}
}

func isSlice(input interface{}) bool {
	return reflect.TypeOf(input).Kind() == reflect.Slice
}

// makeDefaultLabels returns a sequential series of numbers (inclusive of min, exclusive of max) and a companion isNull slice.
func makeDefaultLabels(min, max int) (labels []int, isNull []bool) {
	labels = make([]int, max-min)
	isNull = make([]bool, len(labels))
	for i := range labels {
		labels[i] = min + i
		isNull[i] = false
	}
	return
}

// makeIntRange returns a sequential series of numbers (inclusive of min, exclusive of max)
func makeIntRange(min, max int) []int {
	ret := make([]int, max-min)
	for i := range ret {
		ret[i] = min + i
	}
	return ret
}

func containsLabel(label string, labels []*valueContainer) ([]int, error) {
	toFind := strings.Split(label, "|")
	for i := range toFind {
		toFind[i] = strings.TrimSpace(toFind[i])
	}
	if len(toFind) != len(labels) {
		return nil, fmt.Errorf("label (%v) must reference same number of levels as labels (%d != %d)", label, len(toFind), len(labels))
	}
	l := reflect.ValueOf(labels[0].slice).Len()
	ret := make([]int, 0)
	for i := 0; i < l; i++ {
		match := true
		for j := range labels {
			v := reflect.ValueOf(labels[j].slice)
			if v.Index(i).String() != toFind[j] {
				match = false
				break
			}
		}
		if match {
			ret = append(ret, i)
		}
	}
	if len(ret) == 0 {
		return nil, fmt.Errorf("label (%v) does not exist", label)
	}
	return ret, nil
}

// labelWithName returns the index of the label level with the supplied name, or -1 if no level matches
func labelWithName(name string, labels []*valueContainer) int {
	for j := range labels {
		if labels[j].name == name {
			return j
		}
	}
	return -1
}

func minIntSlice(slice []int) int {
	var min int
	for _, val := range slice {
		if val < min {
			min = val
		}
	}
	return min
}

func maxIntSlice(slice []int) int {
	var max int
	for _, val := range slice {
		if val > max {
			max = val
		}
	}
	return max
}

func (vc *valueContainer) valid() []int {
	index := make([]int, 0)
	for i, isNull := range vc.isNull {
		if !isNull {
			index = append(index, i)
		}
	}
	return index
}

func (vc *valueContainer) null() []int {
	index := make([]int, 0)
	for i, isNull := range vc.isNull {
		if isNull {
			index = append(index, i)
		}
	}
	return index
}

// subsetRows returns the rows specified by index. If any position is out of range, returns an error
func (vc *valueContainer) subsetRows(index []int) error {
	v := reflect.ValueOf(vc.slice)
	l := v.Len()
	retIsNull := make([]bool, len(index))
	retVals := reflect.MakeSlice(v.Type(), len(index), len(index))
	// []int{1, 5}
	// indexPosition: [0, 1], indexValue: [1,5]
	for indexPosition, indexValue := range index {
		if indexValue >= l {
			return fmt.Errorf("index out of range (%d > %d)", indexValue, l-1)
		}
		retIsNull[indexPosition] = vc.isNull[indexValue]
		ptr := retVals.Index(indexPosition)
		ptr.Set(v.Index(indexValue))
	}

	vc.slice = retVals.Interface()
	vc.isNull = retIsNull
	return nil
}

func (vc *valueContainer) sort(dtype DType, descending bool, index []int) []int {
	var srt sort.Interface
	switch dtype {
	case Float:
		d := vc.Float()
		d.index = index
		srt = d
		if descending {
			srt = sort.Reverse(srt)
		}
		sort.Sort(srt)
		return d.index
	case Str:
		d := vc.Str()
		d.index = index
		srt = d
		if descending {
			srt = sort.Reverse(srt)
		}
		sort.Sort(srt)
		return d.index
	case DateTime:
		d := vc.DateTime()
		d.index = index
		srt = d
		if descending {
			srt = sort.Reverse(srt)
		}
		sort.Sort(srt)
		return d.index
	}

	return nil
}

func (vc *valueContainer) dropRow(index int) error {
	v := reflect.ValueOf(vc.slice)
	l := v.Len()
	if index >= l {
		return fmt.Errorf("index out of range (%d > %d)", index, l-1)
	}
	retIsNull := append(vc.isNull[:index], vc.isNull[index+1:]...)
	retVals := reflect.MakeSlice(v.Type(), 0, 0)
	retVals = reflect.AppendSlice(v.Slice(0, index), v.Slice(index+1, l))

	vc.slice = retVals.Interface()
	vc.isNull = retIsNull
	return nil
}

func (vc *valueContainer) copy() *valueContainer {
	v := reflect.ValueOf(vc.slice)
	vals := reflect.MakeSlice(v.Type(), v.Len(), v.Len())
	for i := 0; i < v.Len(); i++ {
		ptr := vals.Index(i)
		ptr.Set(v.Index(i))
	}
	isNull := make([]bool, len(vc.isNull))
	copy(isNull, vc.isNull)
	return &valueContainer{
		slice:  vals.Interface(),
		isNull: isNull,
		name:   vc.name,
	}
}

func setNullsFromInterface(input interface{}) []bool {
	var ret []bool
	if reflect.TypeOf(input).Kind() != reflect.Slice {
		return nil
	}
	switch input.(type) {
	case []float64:
		vals := input.([]float64)
		ret = make([]bool, len(vals))
		for i := range ret {
			if math.IsNaN(vals[i]) {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}

	case []string:
		vals := input.([]string)
		ret = make([]bool, len(vals))
		for i := range ret {
			if isNullString(vals[i]) {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
	case []time.Time:
		vals := input.([]time.Time)
		ret = make([]bool, len(vals))
		for i := range ret {
			if (time.Time{}) == vals[i] {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}

	case []interface{}:
		vals := input.([]interface{})
		ret = make([]bool, len(vals))
		for i := range ret {
			if isNullInterface(vals[i]) {
				ret[i] = true
			} else {
				ret[i] = false
			}
		}
		// no null value possible
	case []bool, []uint, []uint8, []uint16, []uint32, []uint64, []int, []int8, []int16, []int32, []int64, []float32:
		l := reflect.ValueOf(input).Len()
		ret = make([]bool, l)
		for i := range ret {
			ret[i] = false
		}
	case []Element:
		vals := input.([]Element)
		ret = make([]bool, len(vals))
		for i := range ret {
			ret[i] = vals[i].isNull
		}
	default:
		return nil
	}
	return ret
}

func isNullInterface(i interface{}) bool {
	switch i.(type) {
	case float64:
		f := i.(float64)
		if math.IsNaN(f) {
			return true
		}
	case string:
		s := i.(string)
		if isNullString(s) {
			return true
		}
	case time.Time:
		t := i.(time.Time)
		if (time.Time{}) == t {
			return true
		}
	}
	return false
}

func isNullString(s string) bool {
	nullStrings := []string{"NaN", "n/a", "N/A", "", "nil"}
	for _, ns := range nullStrings {
		if strings.TrimSpace(s) == ns {
			return true
		}
	}
	return false
}
