package tada

import (
	"strings"
	"time"
)

var optionLevelSeparator = "|"
var optionMaxRows = 50
var optionMaxColumns = 20
var optionsNullPrinter = "(null)"
var optionMergeRepeats = true
var optionWrapLines = false
var optionWarnings = true
var optionNullStrings = map[string]bool{"": true, optionsNullPrinter: true}
var optionNaNIsNull = true
var optionPrefix = "*"
var optionDateTimeFormats = []string{
	"2006-01-02", "01-02-2006", "01/02/2006", "1/2/06", "1/2/2006", "2006-01-02 15:04:05 -0700 MST",
	time.Kitchen, strings.ToLower(time.Kitchen),
	time.RFC3339, time.RFC3339Nano, time.RFC822}
var randSeed = time.Now().Unix()

// SetOptionDefaultSeparator changes the separator used in group names and multi-level column names to sep
// (default: "|").
func SetOptionDefaultSeparator(sep string) {
	optionLevelSeparator = sep
}

// SetOptionAddTimeFormat adds format to the list of time formats that
// can be parsed when converting values from string to time.Time.
func SetOptionAddTimeFormat(format string) {
	optionDateTimeFormats = append(optionDateTimeFormats, format)
}

// GetOptionDefaultNullStrings returns the default list of strings that tada considers null.
func GetOptionDefaultNullStrings() []string {
	return []string{"", optionsNullPrinter}
}

// SetOptionNullStrings replaces the default list of strings that tada considers null with list.
func SetOptionNullStrings(list []string) {
	newList := make(map[string]bool, 0)
	for _, s := range list {
		newList[s] = true
	}
	optionNullStrings = newList
}

// SetOptionNaNStatus sets whether math.NaN() is considered a null value or not (default: true).
func SetOptionNaNStatus(set bool) {
	optionNaNIsNull = set
}

// PrintOptionMaxRows changes the max number of rows displayed when printing a Series or DataFrame to n
// (default: 50).
func PrintOptionMaxRows(n int) {
	optionMaxRows = n
}

// PrintOptionMaxColumns changes the max number of columns displayed when printing a Series or DataFrame to n
// (default: 20).
func PrintOptionMaxColumns(n int) {
	optionMaxColumns = n
}

// PrintOptionMergeRepeats (if true) instructs the String() function to merge repeated non-header values
// when printing a Series or DataFrame (default: true).
func PrintOptionMergeRepeats(set bool) {
	optionMergeRepeats = set
}

// PrintOptionWrapLines (if true) instructs the String() function to wrap overly-wide rows onto new lines instead of truncating them
// when printing a Series or DataFrame (default: truncate).
func PrintOptionWrapLines(set bool) {
	optionWrapLines = set
}

// DisableWarnings prevents tada from writing warning messages to the default log writer.
func DisableWarnings() {
	optionWarnings = false
}

// EnableWarnings allows tada to write warning messages to the default log writer.
func EnableWarnings() {
	optionWarnings = true
}
