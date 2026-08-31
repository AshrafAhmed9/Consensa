package storage

// memtable holds the newest unflushed records. Its skiplist order is exactly the order
// required by SSTable creation, avoiding a second sort on the write path.
type memtable struct{ list *skiplist }

func newMemtable() *memtable                { return &memtable{list: newSkiplist(1)} }
func (m *memtable) put(k, v []byte, t bool) { m.list.put(k, v, t) }
