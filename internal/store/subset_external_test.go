package store_test

import (
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.kenn.io/msgvault/internal/store"
	"go.kenn.io/msgvault/internal/testutil"
)

func TestCopySubsetPreservesMergedSourceDraft(t *testing.T) {
	for _, provider := range []string{"imap", "gmail"} {
		t.Run(provider, func(t *testing.T) {
			require := require.New(t)
			assert := assert.New(t)
			src := testutil.NewSQLiteTestStore(t)
			from, err := src.GetOrCreateSource(provider, "source@example.test")
			require.NoError(err)
			insertMergeDraft(t, src, from.ID, provider+"_drafts", true)
			_, err = src.DB().Exec(`UPDATE messages SET sent_at = '2024-01-01 01:00:00'`)
			require.NoError(err)
			into, err := src.GetOrCreateSource(provider, "destination@example.test")
			require.NoError(err)
			_, err = src.MergeSourcesContext(t.Context(), store.MergeSourcesRequest{FromSourceID: from.ID, IntoSourceID: into.ID})
			require.NoError(err)

			dstDir := filepath.Join(t.TempDir(), "dst")
			_, err = store.CopySubset(store.DBPathForTest(src), dstDir, 1, false)
			require.NoError(err)
			dst, err := store.Open(filepath.Join(dstDir, "msgvault.db"))
			require.NoError(err)
			t.Cleanup(func() { _ = dst.Close() })
			candidates, err := dst.LoadActivityCandidatesByIDContext(t.Context(), []int64{1})
			require.NoError(err)
			require.Len(candidates, 1)
			assert.True(candidates[0].Unsent)
			assert.False(candidates[0].Eligible)
		})
	}
}
