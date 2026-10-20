package vector

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"regexp"
)

// EmbedGenBackfillMigration is the applied_migrations ledger key that guards
// the one-time embed_gen upgrade backfill. Never change it, or the backfill
// would re-run on every Open.
const EmbedGenBackfillMigration = "embed_gen_backfill_active_v1"

var endpointSegment = regexp.MustCompile(`:u[0-9a-f]{64}$`)

// HasEndpointSegment recognizes a trailing endpoint identity.
func HasEndpointSegment(fp string) bool {
	return endpointSegment.MatchString(fp)
}

// AdoptEndpoint records the configured endpoint on legacy active and building generations.
func AdoptEndpoint(ctx context.Context, db *sql.DB, rebind func(string) string, endpointFP string) error {
	if endpointFP == "" {
		return nil
	}
	rows, err := db.QueryContext(ctx, `SELECT id, fingerprint FROM index_generations WHERE state IN ('active', 'building')`)
	if err != nil {
		return fmt.Errorf("list legacy generations: %w", err)
	}
	defer func() { _ = rows.Close() }()
	type legacyGeneration struct {
		id          int64
		fingerprint string
	}
	var legacy []legacyGeneration
	for rows.Next() {
		var g legacyGeneration
		if err := rows.Scan(&g.id, &g.fingerprint); err != nil {
			return fmt.Errorf("read legacy generation: %w", err)
		}
		if !HasEndpointSegment(g.fingerprint) {
			legacy = append(legacy, g)
		}
	}
	if err := errors.Join(rows.Err(), rows.Close()); err != nil {
		return fmt.Errorf("read legacy generations: %w", err)
	}
	for _, g := range legacy {
		if _, err := db.ExecContext(ctx, rebind(`UPDATE index_generations SET fingerprint = ? WHERE id = ? AND fingerprint = ?`), g.fingerprint+":u"+endpointFP, g.id, g.fingerprint); err != nil {
			return fmt.Errorf("adopt generation endpoint: %w", err)
		}
	}
	return nil
}

// ResolveActiveForFingerprint returns the active generation if its
// fingerprint matches the supplied one. Production callers pass
// Config.GenerationFingerprint() so the preprocessing policy is part
// of the staleness check; a mismatch means an operator changed the
// embedding settings (endpoint, model, dimension, preprocessing policy, or build
// scope) without rebuilding the index, or built a one-off scoped generation
// without making that scope durable in config.
//
// Error semantics:
//   - ErrIndexStale: an active generation exists but its fingerprint
//     does not match the supplied one.
//   - ErrIndexBuilding: no active yet, but a first-ever build is in
//     progress.
//   - ErrNotEnabled: no generation exists at all (vector search not
//     initialized).
//
// Any other error from the Backend is wrapped and returned as-is.
func ResolveActiveForFingerprint(ctx context.Context, b Backend, fingerprint string) (Generation, error) {
	active, err := b.ActiveGeneration(ctx)
	if err == nil {
		if fingerprint != "" && active.Fingerprint != fingerprint {
			return Generation{}, fmt.Errorf("%w: active=%q configured=%q",
				ErrIndexStale, active.Fingerprint, fingerprint)
		}
		return active, nil
	}
	if !errors.Is(err, ErrNoActiveGeneration) {
		return Generation{}, fmt.Errorf("active generation: %w", err)
	}
	// No active generation. Check for a building one to distinguish
	// "first-time build" from "nothing configured".
	building, bErr := b.BuildingGeneration(ctx)
	if bErr != nil {
		return Generation{}, fmt.Errorf("building generation: %w", bErr)
	}
	if building != nil {
		return Generation{}, ErrIndexBuilding
	}
	return Generation{}, ErrNotEnabled
}
