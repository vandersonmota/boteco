package storage

const MaxPages = uint32(100)
const PageSize = uint32(4096)                  // bytes
const MaxEntriesPerPage = PageSize / uint32(8) // key + value

type Entry struct {
	key   uint32
	value uint32
}

type PageHeader {
	version uint8
	slotCount uint32
	
}

type Page struct {
	entriesOffset uint16
	entries       *[MaxEntriesPerPage]Entry
}

func (p *Page) addEntry(k, v uint32) {
	i := p.entriesOffset / uint16(8)
	p.entries[i] = Entry{key: k, value: v}
	p.entriesOffset += uint16(8)
}

type Store struct {
	pagesOffset uint32
	pages       *[MaxPages]Page
}

func NewStore() *Store {
	return &Store{
		pagesOffset: uint32(0),
	}
}

func (s *Store) addEntry(k, v uint32) {
	p := Page{
		entriesOffset: 0,
	}
	s.pages[s.pagesOffset] = p
}
