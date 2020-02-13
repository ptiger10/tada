package tada

import "time"

var optionLevelSeparator = "|"
var optionMaxRows = 50
var optionAutoMerge = true
var randSeed = time.Now().Unix()

// SetOptionLevelSeparator stub
func SetOptionLevelSeparator(sep string) {
	optionLevelSeparator = sep
}

// SetOptionMaxRows stub
func SetOptionMaxRows(n int) {
	optionMaxRows = n
}

// SetOptionAutoMerge stub
func SetOptionAutoMerge(set bool) {
	optionAutoMerge = set
}
