package tinykv

import "encoding/binary"

/*
Internal page layout:
| OFFSET | SIZE | DATA
|      0 |    1 | page type
|      1 |    1 | is root
|      2 |    2 | reserved
|      4 |    4 | parent index
|      8 |    4 | right child index
|     12 |    4 | cell count
|     16 |      | cells

Cell layout:
| OFFSET | SIZE | DATA
|      0 |    4 | left child index
|      4 |    4 | key length
|      8 |   kl | key
*/

const (
	internalPageTypeOffset        = 0
	internalPageIsRootOffset      = 1
	internalPageParentIndexOffset = 4
	internalPageRightChildIndex   = 8
	internalPageNumCellsOffset    = 12
	internalPageFirstCellOffset   = 16
)

type internalPage struct {
	pageBase
	freeSpace uint32
}

type internalCell struct {
	key            []byte
	leftChildIndex uint32
	offset         uint32
}

type internalCellIterator struct {
	p           *internalPage
	currentCell uint32
	offset      uint32
}

func getInternalNodeCellSize(keyLen int) uint32 {
	return uint32(keyLen) + 8
}

func newInternalPage(index uint32, data []byte) *internalPage {
	p := &internalPage{
		pageBase:  pageBase{data: data},
		freeSpace: 0,
	}

	if p.data == nil {
		p.data = make([]uint8, defaultPageSize)

		p.data[0] = uint8(pageKindInternal)
		p.setNumCells(0)
		p.setIsRoot(true)
		p.setParentIndex(-1)
		p.setRightChildIndex(1)
	}

	// Calculate initial free space
	pageSizeTaken := uint32(internalPageFirstCellOffset)
	for it := p.iter(); it.hasNext(); {
		cell := it.next()
		pageSizeTaken = cell.offset + getInternalNodeCellSize(len(cell.key))
	}
	p.freeSpace = uint32(len(p.data)) - pageSizeTaken

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
	return int32(binary.LittleEndian.Uint32(p.data[internalPageParentIndexOffset : internalPageParentIndexOffset+4]))
}

func (p *internalPage) setParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(p.data[internalPageParentIndexOffset:internalPageParentIndexOffset+4], uint32(parentIndex))
}

func (p *internalPage) getNumCells() uint32 {
	return binary.LittleEndian.Uint32(p.data[internalPageNumCellsOffset : internalPageNumCellsOffset+4])
}

func (p *internalPage) setNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(p.data[internalPageNumCellsOffset:internalPageNumCellsOffset+4], numCells)
}

func (p *internalPage) setRightChildIndex(rightChildIndex uint32) {
	binary.LittleEndian.PutUint32(p.data[internalPageRightChildIndex:internalPageRightChildIndex+4], rightChildIndex)
}

func (p *internalPage) getRightChildIndex() uint32 {
	return binary.LittleEndian.Uint32(p.data[internalPageRightChildIndex : internalPageRightChildIndex+4])
}

func (p *internalPage) getFreeSpace() uint32 {
	return p.freeSpace
}

func (p *internalPage) iter() internalCellIterator {
	return internalCellIterator{p: p}
}

func (it *internalCellIterator) hasNext() bool {
	return it.currentCell < it.p.getNumCells()
}

func (it *internalCellIterator) next() internalCell {
	if it.currentCell == 0 {
		it.offset = internalPageFirstCellOffset
	}
	if !it.hasNext() {
		panic("internal cell iterator reached the end")
	}

	cellOffset := it.offset

	leftChildIndex := binary.LittleEndian.Uint32(it.p.data[it.offset : it.offset+4])
	it.offset += 4

	keyLen := binary.LittleEndian.Uint32(it.p.data[it.offset : it.offset+4])
	it.offset += 4
	key := it.p.data[it.offset : it.offset+keyLen]
	it.offset += keyLen

	it.currentCell++

	return internalCell{
		key:            key,
		leftChildIndex: leftChildIndex,
		offset:         cellOffset,
	}
}
