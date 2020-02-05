package tada

import "time"

// Apply

// Apply stub
func (vc *floatValueContainer) Apply(func(val float64, isNull bool) float64) *floatValueContainer {
	return nil
}

// Apply stub
func (vc *stringValueContainer) Apply(func(val string, isNull bool) string) *stringValueContainer {
	return nil
}

// Apply stub
func (vc *dateTimeValueContainer) Apply(func(val time.Time, isNull bool) time.Time) *dateTimeValueContainer {
	return nil
}
