package tada

import "time"

// Series

// Casters

// Float stub
func (s *Series) Float() *FloatValueContainer {
	return nil
}

// Str stub
func (s *Series) Str() *StringValueContainer {
	return nil
}

// DT stub
func (s *Series) DT() *DateTimeValueContainer {
	return nil
}

// Filters

// Filter stub
func (vc *FloatValueContainer) Filter(func(val float64) bool) []int {
	return nil
}

// GT stub
func (vc *FloatValueContainer) GT(comparison float64) []int {
	return nil
}

// LT stub
func (vc *FloatValueContainer) LT(comparison float64) []int {
	return nil
}

// GTE stub
func (vc *FloatValueContainer) GTE(comparison float64) []int {
	return nil
}

// LTE stub
func (vc *FloatValueContainer) LTE(comparison float64) []int {
	return nil
}

// EQ stub
func (vc *FloatValueContainer) EQ(comparison float64) []int {
	return nil
}

// NEQ stub
func (vc *FloatValueContainer) NEQ(comparison float64) []int {
	return nil
}

// Filter stub
func (vc *StringValueContainer) Filter(func(val string) bool) []int {
	return nil
}

// Contains stub
func (vc *StringValueContainer) Contains(comparison string) []int {
	return nil
}

// Filter stub
func (vc *DateTimeValueContainer) Filter(func(val time.Time) bool) []int {
	return nil
}

// Before stub
func (vc *DateTimeValueContainer) Before(comparison time.Time) []int {
	return nil
}

// After stub
func (vc *DateTimeValueContainer) After(comparison time.Time) []int {
	return nil
}

// Apply

// Apply stub
func (vc *FloatValueContainer) Apply(func(val float64, isNull bool) float64) *FloatValueContainer {
	return nil
}

// Apply stub
func (vc *StringValueContainer) Apply(func(val string, isNull bool) string) *StringValueContainer {
	return nil
}

// Apply stub
func (vc *DateTimeValueContainer) Apply(func(val time.Time, isNull bool) time.Time) *DateTimeValueContainer {
	return nil
}
