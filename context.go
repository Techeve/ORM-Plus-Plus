package orm

import "context"

type ctxKey int

const (
	ctxTenant ctxKey = iota
	ctxGeo
)

type geoScope struct {
	home         string
	replicateTo  []string
	replicateAll bool
}

// WithTenant legt den Tenant-Scope für alle nachfolgenden Operationen fest.
// Ohne Tenant im Context schlagen Operationen auf tenant-gebundenen Modellen
// mit ErrNoTenant fehl (fail-closed).
func WithTenant(ctx context.Context, tenant ID) context.Context {
	return context.WithValue(ctx, ctxTenant, tenant)
}

// GeoOption konfiguriert die Replikation eines Datensatzes (nur GeoFlexible).
type GeoOption func(*geoScope)

// ReplicateTo legt lesende Kopien in weiteren Regionen an.
func ReplicateTo(regions ...string) GeoOption {
	return func(g *geoScope) { g.replicateTo = append(g.replicateTo, regions...) }
}

// ReplicateAll legt lesende Kopien in allen aktiven Regionen an und folgt
// der Topologie automatisch.
func ReplicateAll() GeoOption {
	return func(g *geoScope) { g.replicateAll = true }
}

// WithGeo legt das Daten-Geo fest: die Heimatregion, in die nachfolgend
// geschriebene Datensätze gehören. Unabhängig vom Instanz-Geo.
func WithGeo(ctx context.Context, home string, opts ...GeoOption) context.Context {
	g := geoScope{home: home}
	for _, o := range opts {
		o(&g)
	}
	return context.WithValue(ctx, ctxGeo, g)
}

func tenantFrom(ctx context.Context) (ID, bool) {
	t, ok := ctx.Value(ctxTenant).(ID)
	return t, ok && !t.IsZero()
}

func geoFrom(ctx context.Context) (geoScope, bool) {
	g, ok := ctx.Value(ctxGeo).(geoScope)
	return g, ok
}
