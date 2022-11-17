package tinykv

type pageKind uint8

const (
	pageSize uint32 = 4096

	pageKindUnallocated pageKind = iota
	pageKindHeader
	pageKindLeaf
	pageKindInternal
)

type page interface {
	getKind() pageKind
	getData() []byte
}

type pageBase struct {
	data []byte
}

func (p *pageBase) getKind() pageKind {
	return pageKind(p.data[0])
}

func (p *pageBase) getData() []byte {
	return p.data
}

type treePage interface {
	page
	isRoot() bool
	getParentIndex() int32
	getNumCells() uint32
	getFreeSpace() uint32
	iterCells(callback func(key, value []byte, offset uint32) bool) uint32
	addCell(key, value []byte) error
	findCell(key []byte) ([]byte, error)
}
