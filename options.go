package tada

import "time"

var optionLevelSeparator = "|"
var optionMaxRows = 50
var optionMaxColumns = 20
var optionAutoMerge = true
var optionWarnings = true
var optionNullStrings = map[string]bool{"NaN": true, "n/a": true, "N/A": true, "": true, "nil": true}
var randSeed = time.Now().Unix()

// SetOptionLevelSeparator stub
func SetOptionLevelSeparator(sep string) {
	optionLevelSeparator = sep
}

// SetOptionMaxRows stub
func SetOptionMaxRows(n int) {
	optionMaxRows = n
}

// SetOptionMaxColumns stub
func SetOptionMaxColumns(n int) {
	optionMaxColumns = n
}

// SetOptionAutoMerge stub
func SetOptionAutoMerge(set bool) {
	optionAutoMerge = set
}

// DisableWarnings stub
func DisableWarnings() {
	optionWarnings = false
}

// EnableWarnings stub
func EnableWarnings() {
	optionWarnings = true
}
