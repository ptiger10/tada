package tada

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"cloud.google.com/go/civil"
)

func (vc floatValueContainer) Less(i, j int) bool {
	if vc.slice[i] < vc.slice[j] {
		return true
	}
	return false
}

func (vc floatValueContainer) Len() int {
	return len(vc.slice)
}

func (vc floatValueContainer) Swap(i, j int) {
	vc.slice[i], vc.slice[j] = vc.slice[j], vc.slice[i]
	vc.isNull[i], vc.isNull[j] = vc.isNull[j], vc.isNull[i]
	vc.index[i], vc.index[j] = vc.index[j], vc.index[i]
}

func (vc stringValueContainer) Less(i, j int) bool {
	if vc.slice[i] < vc.slice[j] {
		return true
	}
	return false
}

func (vc stringValueContainer) Len() int {
	return len(vc.slice)
}

func (vc stringValueContainer) Swap(i, j int) {
	vc.slice[i], vc.slice[j] = vc.slice[j], vc.slice[i]
	vc.isNull[i], vc.isNull[j] = vc.isNull[j], vc.isNull[i]
	vc.index[i], vc.index[j] = vc.index[j], vc.index[i]
}

func (vc dateTimeValueContainer) Less(i, j int) bool {
	if vc.slice[i].Before(vc.slice[j]) {
		return true
	}
	return false
}

func (vc dateTimeValueContainer) Len() int {
	return len(vc.slice)
}

func (vc dateTimeValueContainer) Swap(i, j int) {
	vc.slice[i], vc.slice[j] = vc.slice[j], vc.slice[i]
	vc.isNull[i], vc.isNull[j] = vc.isNull[j], vc.isNull[i]
	vc.index[i], vc.index[j] = vc.index[j], vc.index[i]
}

// converters

func convertStringToFloat(val string, originalBool bool) (float64, bool) {
	parsedVal, err := strconv.ParseFloat(val, 64)
	if err == nil {
		return parsedVal, originalBool
	}
	return 0, true
}

func convertBoolToFloat(val bool) float64 {
	if val {
		return 1
	}
	return 0
}

func (vc *valueContainer) cast(dtype DType) {
	if vc.isString() {
		vc.setCache()
	}
	switch dtype {
	case Float64:
		_, ok := vc.slice.([]float64)
		if !ok {
			vc.slice = vc.float64().slice
		}
	case String:
		_, ok := vc.slice.([]string)
		if !ok {
			vc.slice = vc.string().slice
		}
	case DateTime:
		_, ok := vc.slice.([]time.Time)
		if !ok {
			vc.slice = vc.dateTime().slice
		}
	case Date:
		_, ok := vc.slice.([]civil.Date)
		if !ok {
			arr := vc.dateTime().slice
			ret := make([]civil.Date, len(arr))
			for i := range arr {
				ret[i] = civil.DateOf(arr[i])
			}
			vc.slice = ret
		}
	case Time:
		_, ok := vc.slice.([]civil.Time)
		if !ok {
			arr := vc.dateTime().slice
			ret := make([]civil.Time, len(arr))
			for i := range arr {
				ret[i] = civil.TimeOf(arr[i])
			}
			vc.slice = ret
		}
	}
	return
}

// if already []float64, returns shared values, not new values
func (vc *valueContainer) float64() floatValueContainer {
	newVals := make([]float64, reflect.ValueOf(vc.slice).Len())
	isNull := vc.isNull
	switch vc.slice.(type) {
	case []float64:
		newVals = vc.slice.([]float64)

	case []string:
		arr := vc.slice.([]string)
		for i := range arr {
			newVals[i], isNull[i] = convertStringToFloat(arr[i], isNull[i])
		}

	case [][]byte:
		arr := vc.slice.([][]byte)
		for i := range arr {
			newVals[i], isNull[i] = convertStringToFloat(string(arr[i]), isNull[i])
		}

	case []time.Time, []civil.DateTime, []civil.Date, []civil.Time:
		for i := range newVals {
			newVals[i], isNull[i] = 0, true
		}

	case []bool:
		arr := vc.slice.([]bool)
		for i := range arr {
			newVals[i] = convertBoolToFloat(arr[i])
		}

	case []interface{}:
		arr := vc.slice.([]interface{})
		for i := range arr {
			switch arr[i].(type) {
			case string:
				newVals[i], isNull[i] = convertStringToFloat(arr[i].(string), isNull[i])
			case float32, float64:
				newVals[i] = reflect.ValueOf(arr[i]).Float()
			case int, int8, int16, int32, int64:
				newVals[i] = float64(reflect.ValueOf(arr[i]).Int())
			case uint, uint8, uint16, uint32, uint64:
				newVals[i] = float64(reflect.ValueOf(arr[i]).Uint())
			case time.Time, civil.DateTime, civil.Date, civil.Time:
				newVals[i], isNull[i] = 0, true
			case bool:
				newVals[i] = convertBoolToFloat(arr[i].(bool))
			}
		}

	case []uint, []uint8, []uint16, []uint32, []uint64, []int, []int8, []int16, []int32, []int64, []float32:
		d := reflect.ValueOf(vc.slice)
		for i := 0; i < d.Len(); i++ {
			v := d.Index(i).Interface()
			newVals[i], isNull[i] = convertStringToFloat(fmt.Sprint(v), isNull[i])
		}
	default:
		for i := range newVals {
			newVals[i] = 0
			isNull[i] = true
		}
	}

	ret := floatValueContainer{
		isNull: isNull,
		slice:  newVals,
	}
	return ret
}

func convertDateTimeToString(v time.Time) string {
	return v.Format(time.RFC3339)
}

// if already []string, returns shared values, not new values
func (vc *valueContainer) string() stringValueContainer {
	newVals := make([]string, reflect.ValueOf(vc.slice).Len())
	isNull := vc.isNull
	switch vc.slice.(type) {
	case []string:
		newVals = vc.slice.([]string)

	case []time.Time:
		arr := vc.slice.([]time.Time)
		for i := range arr {
			newVals[i] = convertDateTimeToString(arr[i])
		}

	case []civil.Date:
		arr := vc.slice.([]civil.Date)
		for i := range arr {
			newVals[i] = arr[i].String()
		}
	case []civil.Time:
		arr := vc.slice.([]civil.Time)
		for i := range arr {
			newVals[i] = arr[i].String()
		}
	case []civil.DateTime:
		arr := vc.slice.([]civil.DateTime)
		for i := range arr {
			newVals[i] = arr[i].String()
		}

	case [][]byte:
		arr := vc.slice.([][]byte)
		for i := range arr {
			newVals[i] = string(arr[i])
		}
	case []int:
		arr := vc.slice.([]int)
		for i := range arr {
			newVals[i] = strconv.Itoa(arr[i])
		}

	case []interface{}:
		arr := vc.slice.([]interface{})
		for i := range arr {
			switch arr[i].(type) {
			case string:
				newVals[i] = arr[i].(string)
			case time.Time:
				newVals[i] = convertDateTimeToString(arr[i].(time.Time))
			default:
				d := reflect.ValueOf(vc.slice)
				newVals[i] = fmt.Sprint(d.Index(i).Interface())
			}
		}

	case []float64, []bool,
		[]uint, []uint8, []uint16, []uint32, []uint64, []int8, []int16, []int32, []int64,
		[][]string, [][]float64, [][]time.Time,
		[][]bool, [][]float32,
		[][]uint, [][]uint16, [][]uint32, [][]uint64,
		[][]int, [][]int8, [][]int16, [][]int32, [][]int64:
		d := reflect.ValueOf(vc.slice)
		for i := 0; i < d.Len(); i++ {
			newVals[i] = fmt.Sprint(d.Index(i).Interface())
		}
	}
	ret := stringValueContainer{
		slice:  newVals,
		isNull: isNull,
	}
	vc.setCacheFromString(ret.slice)
	return ret
}

// returns parsed time and whether value is null
func convertStringToDateTime(val string) (time.Time, bool) {
	for _, format := range optionDateTimeFormats {
		parsedVal, err := time.Parse(format, val)
		if err == nil {
			return parsedVal, false
		}
	}
	return time.Time{}, true
}

func (vc *valueContainer) dateTime() dateTimeValueContainer {
	newVals := make([]time.Time, reflect.ValueOf(vc.slice).Len())
	isNull := vc.isNull
	switch vc.slice.(type) {
	case []string:
		arr := vc.slice.([]string)
		for i := range arr {
			newVals[i], isNull[i] = convertStringToDateTime(arr[i])
		}

	case [][]byte:
		arr := vc.slice.([][]byte)
		for i := range arr {
			newVals[i], isNull[i] = convertStringToDateTime(string(arr[i]))
		}
	case []time.Time:
		newVals = vc.slice.([]time.Time)
	case []civil.Date:
		arr := vc.slice.([]civil.Date)
		for i := range arr {
			if isNull[i] {
				newVals[i] = time.Time{}
			} else {
				newVals[i] = arr[i].In(time.UTC)
			}
		}

	case []civil.Time:
		arr := vc.slice.([]civil.Time)
		for i := range arr {
			if isNull[i] {
				newVals[i] = time.Time{}
			} else {
				newVals[i] = time.Date(0, 0, 0, arr[i].Hour, arr[i].Minute, arr[i].Second, arr[i].Nanosecond, time.UTC)
			}
		}
	case []interface{}:
		arr := vc.slice.([]interface{})
		for i := range arr {
			switch arr[i].(type) {
			case string:
				newVals[i], isNull[i] = convertStringToDateTime(arr[i].(string))
			case time.Time:
				newVals[i] = arr[i].(time.Time)
			default:
				newVals[i] = time.Time{}
				isNull[i] = true
			}
		}
	default:
		for i := range newVals {
			newVals[i] = time.Time{}
			isNull[i] = true
		}
	}
	ret := dateTimeValueContainer{
		slice:  newVals,
		isNull: isNull,
	}
	return ret
}

// cache must be reset after any operation that modifies vc.slice
func (vc *valueContainer) resetCache() {
	vc.cache = nil
}

// conditions under which cache is set:
// - concatenating multiple container levels (groupby, lookup, promote)
// - casting from string
// - calling vc.string()
// ignores if cache is already set
func (vc *valueContainer) setCache() {
	if vc.cache != nil {
		return
	}
	switch vc.slice.(type) {
	case [][]byte:
		arr := vc.slice.([][]byte)
		vc.cache = make([]string, len(arr))
		for i := range arr {
			vc.cache[i] = string(arr[i])
		}
	case []string:
		vc.cache = vc.slice.([]string)
	case []float64:
		arr := vc.slice.([]float64)
		vc.cache = make([]string, len(arr))
		for i := range arr {
			vc.cache[i] = fmt.Sprint(arr[i])
		}
	case []int:
		arr := vc.slice.([]int)
		vc.cache = make([]string, len(arr))
		for i := range arr {
			vc.cache[i] = strconv.Itoa(arr[i])
		}
	case []time.Time:
		arr := vc.slice.([]time.Time)
		vc.cache = make([]string, len(arr))
		for i := range arr {
			vc.cache[i] = arr[i].String()
		}
	default:
		arr := reflect.ValueOf(vc.slice)
		vc.cache = make([]string, arr.Len())
		for i := 0; i < arr.Len(); i++ {
			vc.cache[i] = fmt.Sprint(arr.Index(i).Interface())
		}
	}
}

func (vc *valueContainer) isString() bool {
	_, ok := vc.slice.([]string)
	return ok
}

func (vc *valueContainer) setCacheFromString(arr []string) {
	vc.cache = arr
	return
}
