package tada

import (
	"fmt"

	"github.com/ptiger10/tablediff"
)

// PrettyDiff reads two slices of structs into DataFrames, prints each as a stringified csv table,
// and returns whether they are equal. If not, returns the differences between the two.
func PrettyDiff(got, want interface{}) (bool, *tablediff.Differences, error) {
	df1, err := NewStructReader(got).Read()
	if err != nil {
		return false, nil, fmt.Errorf("pretty diffing two structs: reading got: %v", err)
	}
	df2, err := NewStructReader(want).Read()
	if err != nil {
		return false, nil, fmt.Errorf("pretty diffing two structs: reading want: %v", err)
	}
	gotRecords := NewRecordWriter()
	wantRecords := NewRecordWriter()
	// ducking error because writing to [][]string known to not cause errors
	df1.WriteTo(gotRecords)
	df2.WriteTo(wantRecords)
	diffs, eq := tablediff.Diff(gotRecords.Records(), wantRecords.Records())
	return eq, diffs, nil
}
