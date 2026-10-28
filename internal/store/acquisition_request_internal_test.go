package store

import (
	"encoding/json"
	"fmt"
	"slices"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestAcquisitionReplayRequestJSON(t *testing.T) {
	must := require.New(t)
	ids := []string{"z", "quoted-\"\\", "", "z", "a", "quoted-\"\\", ""}
	original := slices.Clone(ids)
	encoded, err := acquisitionReplayRequestJSON(ids)
	must.NoError(err)
	var decoded []string
	must.NoError(json.Unmarshal(encoded, &decoded))
	must.Equal([]string{"", "a", "quoted-\"\\", "z"}, decoded)
	must.Equal(original, ids, "the caller's order and write chunks must remain intact")
}

// Isolate the extra request preparation cost from driver/server allocations.
// The complete deletion/GC fixtures separately measure the production APIs.
func BenchmarkAcquisitionReplayRequestJSON(b *testing.B) {
	for _, count := range []int{1, 500, 10000, 100000} {
		for _, repeats := range []int{1, 10} {
			for _, deduplicate := range []bool{false, true} {
				b.Run(fmt.Sprintf("ids=%d/repeats=%d/deduplicate=%t", count, repeats, deduplicate), func(b *testing.B) {
					ids := make([]string, count)
					for i := range ids {
						ids[i] = fmt.Sprintf("%016x", ((i*7919)%count)/repeats)
					}
					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						var err error
						if deduplicate {
							_, err = acquisitionReplayRequestJSON(ids)
						} else {
							_, err = json.Marshal(ids)
						}
						if err != nil {
							require.NoError(b, err)
						}
					}
				})
			}
		}
	}
}
