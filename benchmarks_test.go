package tada

import "testing"

func Benchmark_ReadCSV(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ImportCSV("test_files/big.csv", nil)
	}
}
