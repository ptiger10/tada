package tada

import "time"

var optionLevelSeparator = "|"
var optionLevelSeparatorBytes = []byte{'|'}
var optionMaxRows = 50
var optionMaxColumns = 20
var optionAutoMerge = true
var optionWarnings = true
var optionNullStrings = map[string]bool{"NaN": true, "n/a": true, "N/A": true, "": true, "nil": true}
var optionPrefix = "*"
var optionDateTimeFormats = []string{
	"2006-01-02", "01-02-2006", "01/02/2006", "1/2/2006", "2006-01-02 15:04:05 -0700 MST",
	time.RFC3339, time.RFC3339Nano, time.RFC822}
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

// SetOptionAddTimeFormat adds `format` to the list of time formats that
// can be parsed when converting values from string to time.Time
func SetOptionAddTimeFormat(format string) {
	optionDateTimeFormats = append(optionDateTimeFormats, format)
}
