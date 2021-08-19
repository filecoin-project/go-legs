package datastore

import (
	"sync"

	"github.com/ipfs/go-datastore"
	dsq "github.com/ipfs/go-datastore/query"
)

// Here are some basic datastore implementations.

// MapDatastore uses a standard Go map for internal storage.
type MapDatastore struct {
	values map[datastore.Key][]byte
	lk     sync.RWMutex
}

// NewMapDatastore constructs a MapDatastore. It is _not_ thread-safe by
// default, wrap using sync.MutexWrap if you need thread safety (the answer here
// is usually yes).
func NewMapDatastore() (d *MapDatastore) {
	return &MapDatastore{
		values: make(map[datastore.Key][]byte),
	}
}

// Put implements Datastore.Put
func (d *MapDatastore) Put(key datastore.Key, value []byte) (err error) {
	d.lk.Lock()
	defer d.lk.Unlock()
	d.values[key] = value
	return nil
}

// Sync implements Datastore.Sync
func (d *MapDatastore) Sync(prefix datastore.Key) error {
	return nil
}

// Get implements Datastore.Get
func (d *MapDatastore) Get(key datastore.Key) (value []byte, err error) {
	d.lk.RLock()
	defer d.lk.RUnlock()
	val, found := d.values[key]
	if !found {
		return nil, datastore.ErrNotFound
	}
	return val, nil
}

// Has implements Datastore.Has
func (d *MapDatastore) Has(key datastore.Key) (exists bool, err error) {
	d.lk.RLock()
	defer d.lk.RUnlock()
	_, found := d.values[key]
	return found, nil
}

// GetSize implements Datastore.GetSize
func (d *MapDatastore) GetSize(key datastore.Key) (size int, err error) {
	if v, found := d.values[key]; found {
		return len(v), nil
	}
	return -1, datastore.ErrNotFound
}

// Delete implements Datastore.Delete
func (d *MapDatastore) Delete(key datastore.Key) (err error) {
	d.lk.Lock()
	defer d.lk.Unlock()
	delete(d.values, key)
	return nil
}

// Query implements Datastore.Query
func (d *MapDatastore) Query(q dsq.Query) (dsq.Results, error) {
	d.lk.RLock()
	defer d.lk.RUnlock()
	re := make([]dsq.Entry, 0, len(d.values))
	for k, v := range d.values {
		e := dsq.Entry{Key: k.String(), Size: len(v)}
		if !q.KeysOnly {
			e.Value = v
		}
		re = append(re, e)
	}
	r := dsq.ResultsWithEntries(q, re)
	r = dsq.NaiveQueryApply(q, r)
	return r, nil
}

func (d *MapDatastore) Batch() (datastore.Batch, error) {
	d.lk.Lock()
	defer d.lk.Unlock()
	return datastore.NewBasicBatch(d), nil
}

func (d *MapDatastore) Close() error {
	return nil
}
