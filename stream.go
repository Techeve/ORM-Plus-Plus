package orm

import (
	"context"
	"fmt"
	"iter"
	"reflect"
)

// --- Live-Hub (Watch) ---

type watcher struct {
	m  *model
	ch chan CloudEvent
}

func (d *DB) subscribe(m *model) *watcher {
	w := &watcher{m: m, ch: make(chan CloudEvent, 256)}
	d.watchMu.Lock()
	d.watchers[w] = struct{}{}
	d.watchMu.Unlock()
	return w
}

func (d *DB) unsubscribe(w *watcher) {
	d.watchMu.Lock()
	delete(d.watchers, w)
	d.watchMu.Unlock()
}

// hasWatchers meldet, ob jemand auf dieses Model hört (Append spart sich
// sonst den CloudEvent-Envelope-Bau).
func (d *DB) hasWatchers(m *model) bool {
	d.watchMu.Lock()
	defer d.watchMu.Unlock()
	for w := range d.watchers {
		if w.m == m {
			return true
		}
	}
	return false
}

// publish verteilt ein committetes Event an alle Watcher des Models.
// Flüchtig: volle Empfänger verlieren Events (Verlässlichkeit kommt aus OnEvent).
func (d *DB) publish(m *model, ce CloudEvent) {
	d.watchMu.Lock()
	defer d.watchMu.Unlock()
	for w := range d.watchers {
		if w.m != m {
			continue
		}
		select {
		case w.ch <- ce:
		default:
		}
	}
}

// replayEvents liefert Events chunkweise ab den Positionen in delivered
// (je Geo) und hält delivered aktuell. emit=false bricht ab.
func (d *DB) replayEvents(ctx context.Context, q queryer, m *model, tenant ID, delivered map[string]int64, emit func(CloudEvent) bool) error {
	geos, err := d.eventGeos(ctx, m)
	if err != nil {
		return err
	}
	for _, geo := range geos {
		for {
			// Streams lesen transparent Hot + Archiv (alte Positionen).
			query := fmt.Sprintf(`SELECT %s FROM %s WHERE geo = ? AND seq > ?`, esEventSelect(m), esEventsFrom(m, true))
			args := []any{geo, delivered[geo]}
			if m.tenanted() {
				query += ` AND tenant_id = ?`
				args = append(args, tenant.String())
			}
			query += ` ORDER BY seq LIMIT 500`
			batch, err := fetchEventRows(ctx, q, m, query, args)
			if err != nil {
				return err
			}
			if len(batch) == 0 {
				break
			}
			for _, r := range batch {
				if !emit(d.cloudEvent(m, r)) {
					return nil
				}
				delivered[geo] = r.seq
			}
		}
	}
	return nil
}

// Stream liefert den Event-Strom über alle Aggregate eines Models als
// CloudEvents. Ordnung: strikt pro Aggregat, monoton pro Geo — keine
// Totalordnung über Regionen. orm.From(pos) startet nach einer Position.
func Stream[T any](ctx context.Context, h Handle, opts ...StreamOption) iter.Seq2[CloudEvent, error] {
	return func(yield func(CloudEvent, error) bool) {
		d := h.db()
		m := d.reg.models[reflect.TypeFor[T]()]
		if m == nil || m.kind != kindEventSourced {
			yield(CloudEvent{}, fmt.Errorf("orm: Stream[%T]: kein registriertes EventSourced-Model", *new(T)))
			return
		}
		tenant, err := esTenant(ctx, m)
		if err != nil {
			yield(CloudEvent{}, err)
			return
		}
		var so streamOpts
		for _, o := range opts {
			o(&so)
		}
		delivered := map[string]int64{}
		for g, s := range so.from.seqs {
			delivered[g] = s
		}
		if err := d.replayEvents(ctx, readQ(h), m, tenant, delivered, func(ce CloudEvent) bool {
			return yield(ce, nil)
		}); err != nil {
			yield(CloudEvent{}, err)
		}
	}
}

// Watch liefert einen flüchtigen Live-Strom committeter Events — für
// verbundene Clients (SSE/WebSocket). Wer nicht zuhört, verpasst nichts
// Dauerhaftes; der verlässliche Pfad ist OnEvent. Der Kanal schließt,
// wenn ctx endet. Ohne Tenant im Context (bei tenant-gebundenen Modellen)
// bleibt der Kanal leer und schließt sofort (fail-closed).
func Watch[T any](ctx context.Context, h Handle, opts ...StreamOption) <-chan CloudEvent {
	out := make(chan CloudEvent, 64)
	d := h.db()
	m := d.reg.models[reflect.TypeFor[T]()]
	if m == nil || m.kind != kindEventSourced {
		close(out)
		return out
	}
	tenant, err := esTenant(ctx, m)
	if err != nil {
		close(out)
		return out
	}
	var so streamOpts
	for _, o := range opts {
		o(&so)
	}
	w := d.subscribe(m)
	go func() {
		defer close(out)
		defer d.unsubscribe(w)
		delivered := map[string]int64{}
		for g, s := range so.from.seqs {
			delivered[g] = s
		}
		// Nachzügler ab der Position aus dem Log, dann live weiter.
		if len(so.from.seqs) > 0 {
			_ = d.replayEvents(ctx, d.qr(), m, tenant, delivered, func(ce CloudEvent) bool {
				select {
				case out <- ce:
					return true
				case <-ctx.Done():
					return false
				}
			})
		}
		for {
			select {
			case <-ctx.Done():
				return
			case ce := <-w.ch:
				if m.tenanted() && ce.Tenant != tenant {
					continue
				}
				if ce.Sequence <= delivered[ce.Geo] {
					continue
				}
				delivered[ce.Geo] = ce.Sequence
				select {
				case out <- ce:
				case <-ctx.Done():
					return
				}
			}
		}
	}()
	return out
}
