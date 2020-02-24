package tada

import (
	"fmt"
	"reflect"
	"strconv"
	"time"

	"github.com/araddon/dateparse"
)

// Less stub
func (vc floatValueContainer) Less(i, j int) bool {
	if vc.slice[i] < vc.slice[j] {
		return true
	}
	return false
}

// Len stub
func (vc floatValueContainer) Len() int {
	return len(vc.slice)
}

// Swap stub
func (vc floatValueContainer) Swap(i, j int) {
	vc.slice[i], vc.slice[j] = vc.slice[j], vc.slice[i]
	vc.isNull[i], vc.isNull[j] = vc.isNull[j], vc.isNull[i]
	vc.index[i], vc.index[j] = vc.index[j], vc.index[i]
}

// Less stub
func (vc stringValueContainer) Less(i, j int) bool {
	if vc.slice[i] < vc.slice[j] {
		return true
	}
	return false
}

// Len stub
func (vc stringValueContainer) Len() int {
	return len(vc.slice)
}

// Swap stub
func (vc stringValueContainer) Swap(i, j int) {
	vc.slice[i], vc.slice[j] = vc.slice[j], vc.slice[i]
	vc.isNull[i], vc.isNull[j] = vc.isNull[j], vc.isNull[i]
	vc.index[i], vc.index[j] = vc.index[j], vc.index[i]
}

// Less stub
func (vc dateTimeValueContainer) Less(i, j int) bool {
	if vc.slice[i].Before(vc.slice[j]) {
		return true
	}
	return false
}

// Len stub
func (vc dateTimeValueContainer) Len() int {
	return len(vc.slice)
}

// Swap stub
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

// may change in the future
func convertDateTimeToFloat(val time.Time, originalBool bool) (float64, bool) {
	return 0, true
}

func convertBoolToFloat(val bool) float64 {
	if val {
		return 1
	}
	return 0
}

func (vc *valueContainer) cast(dtype DType) {
	switch dtype {
	case Float:
		_, ok := vc.slice.([]float64)
		if !ok {
			vc.slice = vc.float().slice
		}
	case String:
		_, ok := vc.slice.([]string)
		if !ok {
			vc.slice = vc.str().slice
		}
	case DateTime:
		_, ok := vc.slice.([]time.Time)
		if !ok {
			vc.slice = vc.dateTime().slice
		}
	}
	return
}

// if already []float64, returns shared values, not new values
func (vc *valueContainer) float() floatValueContainer {
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

	case []time.Time:
		arr := vc.slice.([]time.Time)
		for i := range arr {
			// newVals[i] = float64(arr[i].UnixNano())
			newVals[i], isNull[i] = convertDateTimeToFloat(arr[i], isNull[i])
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
			case time.Time:
				newVals[i], isNull[i] = convertDateTimeToFloat(arr[i].(time.Time), isNull[i])
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
	}

	ret := floatValueContainer{
		isNull: isNull,
		slice:  newVals,
		index:  makeIntRange(0, len(newVals)),
	}
	return ret
}

// if already []string, returns shared values, not new values
func (vc *valueContainer) str() stringValueContainer {
	newVals := make([]string, reflect.ValueOf(vc.slice).Len())
	isNull := vc.isNull
	switch vc.slice.(type) {
	case []string:
		newVals = vc.slice.([]string)
	default:
		d := reflect.ValueOf(vc.slice)
		for i := 0; i < d.Len(); i++ {
			newVals[i] = fmt.Sprint(d.Index(i).Interface())
		}
	}
	ret := stringValueContainer{
		slice:  newVals,
		isNull: isNull,
		index:  makeIntRange(0, len(newVals)),
	}
	return ret
}

func convertStringToDateTime(val string) (time.Time, bool) {
	parsedVal, err := dateparse.ParseAny(val)
	if err == nil {
		return parsedVal, false
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
	case []time.Time:
		newVals = vc.slice.([]time.Time)
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
		isNull: isNull,
		slice:  newVals,
		index:  makeIntRange(0, len(newVals)),
	}
	return ret

}
