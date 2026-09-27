package admission

import (
	"errors"
	"sync/atomic"
)

type Grid struct {
	GridID string
	Lat    float64
	Lon    float64
}

var ErrUnknownGrid = errors.New("admission: unknown grid_id")
var ErrEmptySnapshot = errors.New("admission: grid snapshot is empty")

type Snapshot struct {
	grids map[string]Grid
}

func newSnapshot(grids map[string]Grid) *Snapshot {
	cp := make(map[string]Grid, len(grids))
	for k, v := range grids {
		cp[k] = v
	}
	return &Snapshot{grids: cp}
}

func (s *Snapshot) Lookup(gridID string) (Grid, bool) {
	g, ok := s.grids[gridID]
	return g, ok
}

func (s *Snapshot) Len() int {
	return len(s.grids)
}

type Registry struct {
	current atomic.Pointer[Snapshot]
}

func NewRegistry() *Registry {
	return &Registry{}
}

func (r *Registry) Lookup(gridID string) (Grid, bool) {
	snap := r.current.Load()
	if snap == nil {
		return Grid{}, false
	}
	return snap.Lookup(gridID)
}

func (r *Registry) Ready() bool {
	return r.current.Load() != nil
}

func (r *Registry) Publish(grids map[string]Grid) error {
	if len(grids) == 0 {
		return ErrEmptySnapshot
	}
	r.current.Store(newSnapshot(grids))
	return nil
}

func (r *Registry) CurrentSnapshot() *Snapshot {
	return r.current.Load()
}
