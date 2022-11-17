package tinykv

import "encoding/binary"

/*
Internal page layout:
| OFFSET | SIZE | DATA
|      0 |    1 | page type
|      1 |    1 | is root
|      2 |    2 | reserved
|      4 |    4 | parent index
|      8 |    4 | free space
|     12 |    4 | right child index
|     16 |    4 | cell count
|     20 |      | cells

Cell layout:
| OFFSET | SIZE | DATA
|      0 |    4 | key length
|      4 |   kl | key
|   4+kl |    4 | left child index
*/

const (
	internalPageFirstCellOffset  uint32 = 20
	internalPageDefaultFreeSpace uint32 = pageSize - internalPageFirstCellOffset
)

type internalPage struct {
	pageBase
}

func newInternalPage(index uint32, data []byte) *internalPage {
	p := &internalPage{
		pageBase{
			data: data,
		},
	}

	if p.data == nil {
		p.data = make([]uint8, pageSize)

		p.data[0] = uint8(pageKindInternal)
		p.setNumCells(0)
		p.setIsRoot(true)
		p.setParentIndex(-1)
	}

	return p
}

func (p *internalPage) isRoot() bool {
	return p.data[1] == 1
}

func (p *internalPage) setIsRoot(isRoot bool) {
	p.data[1] = 0
	if isRoot {
		p.data[1] = 1
	}
}

func (p *internalPage) getParentIndex() int32 {
	return int32(binary.LittleEndian.Uint32(p.data[2:6]))
}

func (p *internalPage) setParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(p.data[2:6], uint32(parentIndex))
}

func (p *internalPage) getNumCells() uint32 {
	return binary.LittleEndian.Uint32(p.data[10:14])
}

func (p *internalPage) setNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(p.data[10:14], numCells)
}
