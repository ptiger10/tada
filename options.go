package tada

import "time"

var optionLevelSeparator = "|"
var optionLevelSeparatorBytes = []byte{'|'}
var optionMaxRows = 50
var optionMaxColumns = 20
var optionMergeRepeats = true
var optionWarnings = true
var optionNullStrings = map[string]bool{"NaN": true, "n/a": true, "N/A": true, "": true, "nil": true}
var optionPrefix = "*"
var optionDateTimeFormats = []string{
	"2006-01-02", "01-02-2006", "01/02/2006", "1/2/2006", "2006-01-02 15:04:05 -0700 MST",
	time.RFC3339, time.RFC3339Nano, time.RFC822}
var randSeed = time.Now().Unix()

// SetOptionLevelSeparator changes the separator used in group names and multi-level column names `sep`
// (default: "|").
func SetOptionLevelSeparator(sep string) {
	optionLevelSeparator = sep
	optionLevelSeparatorBytes = []byte(sep)
}

// SetOptionMaxRows changes the max number of rows displayed when printing a Series or DataFrame to `n`
// (default: 50).
func SetOptionMaxRows(n int) {
	optionMaxRows = n
}

// SetOptionMaxColumns changes the max number of columns displayed when printing a Series or DataFrame to `n`
// (default: 20).
func SetOptionMaxColumns(n int) {
	optionMaxColumns = n
}

// SetOptionMergeRepeats sets whether or not to merge repeated values in the same container when printing a Series or Dataframe
// (default: true).
func SetOptionMergeRepeats(set bool) {
	optionMergeRepeats = set
}

// DisableWarnings prevents tada from writing warning messages to the default log writer.
func DisableWarnings() {
	optionWarnings = false
}

// EnableWarnings allows tada to write warning messages to the default log writer.
func EnableWarnings() {
	optionWarnings = true
}

// SetOptionAddTimeFormat adds `format` to the list of time formats that
// can be parsed when converting values from string to time.Time
func SetOptionAddTimeFormat(format string) {
	optionDateTimeFormats = append(optionDateTimeFormats, format)
}
