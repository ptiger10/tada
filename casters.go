package tada

import "time"

// Filters

// Filter stub
func (vc *floatValueContainer) Filter(func(val float64) bool) []int {
	return nil
}

// GT stub
func (vc *floatValueContainer) GT(comparison float64) []int {
	return nil
}

// LT stub
func (vc *floatValueContainer) LT(comparison float64) []int {
	return nil
}

// GTE stub
func (vc *floatValueContainer) GTE(comparison float64) []int {
	return nil
}

// LTE stub
func (vc *floatValueContainer) LTE(comparison float64) []int {
	return nil
}

// EQ stub
func (vc *floatValueContainer) EQ(comparison float64) []int {
	return nil
}

// NEQ stub
func (vc *floatValueContainer) NEQ(comparison float64) []int {
	return nil
}

// Filter stub
func (vc *stringValueContainer) Filter(func(val string) bool) []int {
	return nil
}

// Contains stub
func (vc *stringValueContainer) Contains(comparison string) []int {
	return nil
}

// Filter stub
func (vc *dateTimeValueContainer) Filter(func(val time.Time) bool) []int {
	return nil
}

// Before stub
func (vc *dateTimeValueContainer) Before(comparison time.Time) []int {
	return nil
}

// After stub
func (vc *dateTimeValueContainer) After(comparison time.Time) []int {
	return nil
}

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
