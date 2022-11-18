package tinykv

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

/*
Leaf page layout:
| OFFSET | SIZE | DATA
|      0 |    1 | page type
|      1 |    1 | is root
|      2 |    6 | reserved
|      8 |    4 | parent index
|     12 |    4 | num cells
|     16 |      | cells

Cell layout:
| OFFSET | SIZE | DATA
|      0 |    4 | key length
|      4 |   kl | key
|   4+kl |    4 | value length
|   8+kl |   vl | value
*/

const (
	leafPageTypeOffset        = 0
	leafPageIsRootOffset      = 1
	leafPageParentIndexOffset = 8
	leafPageNumCellsOffset    = 12
	leafPageFirstCellOffset   = 16
)

type leafPage struct {
	pageBase
	freeSpace uint32
}

type leafCell struct {
	key    []byte
	value  []byte
	offset uint32
}

type leafCellIterator struct {
	p           *leafPage
	currentCell uint32
	offset      uint32
}

func getLeafNodeCellSize(keyLen int, valueLen int) uint32 {
	return uint32(keyLen+valueLen) + 8
}

func newLeafPage(data []byte) *leafPage {
	p := &leafPage{
		pageBase:  pageBase{data: data},
		freeSpace: 0,
	}

	if p.data == nil {
		p.data = make([]byte, defaultPageSize)

		p.data[0] = byte(pageKindLeaf)
		p.setNumCells(0)
		p.setIsRoot(true)
		p.setParentIndex(-1)
	}

	// Calculate initial free space
	pageSizeTaken := uint32(leafPageFirstCellOffset)
	for it := p.iter(); it.hasNext(); {
		cell := it.next()
		pageSizeTaken = cell.offset + getLeafNodeCellSize(len(cell.key), len(cell.value))
	}
	p.freeSpace = uint32(len(p.data)) - pageSizeTaken

	return p
}

func (p *leafPage) isRoot() bool {
	return p.data[leafPageIsRootOffset] == 1
}

func (p *leafPage) setIsRoot(isRoot bool) {
	p.data[leafPageIsRootOffset] = 0
	if isRoot {
		p.data[leafPageIsRootOffset] = 1
	}
}

func (p *leafPage) getParentIndex() int32 {
	return int32(binary.LittleEndian.Uint32(p.data[leafPageParentIndexOffset : leafPageParentIndexOffset+4]))
}

func (p *leafPage) setParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(p.data[leafPageParentIndexOffset:leafPageParentIndexOffset+4], uint32(parentIndex))
}

func (p *leafPage) getNumCells() uint32 {
	return binary.LittleEndian.Uint32(p.data[leafPageNumCellsOffset : leafPageNumCellsOffset+4])
}

func (p *leafPage) setNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(p.data[leafPageNumCellsOffset:leafPageNumCellsOffset+4], numCells)
}

func (p *leafPage) getFreeSpace() uint32 {
	return p.freeSpace
}

func (p *leafPage) iter() leafCellIterator {
	return leafCellIterator{p: p}
}

func (it *leafCellIterator) hasNext() bool {
	return it.currentCell < it.p.getNumCells()
}

func (it *leafCellIterator) next() leafCell {
	if it.currentCell == 0 {
		it.offset = leafPageFirstCellOffset
	}
	if !it.hasNext() {
		panic("leaf cell iterator reached the end")
	}

	cellOffset := it.offset

	keyLen := binary.LittleEndian.Uint32(it.p.data[it.offset : it.offset+4])
	it.offset += 4
	key := it.p.data[it.offset : it.offset+keyLen]
	it.offset += keyLen

	valueLen := binary.LittleEndian.Uint32(it.p.data[it.offset : it.offset+4])
	it.offset += 4
	value := it.p.data[it.offset : it.offset+valueLen]
	it.offset += valueLen

	it.currentCell++

	return leafCell{
		key:    key,
		value:  value,
		offset: cellOffset,
	}
}

func (p *leafPage) addCell(key, value []byte) error {
	requiredSpace := getLeafNodeCellSize(len(key), len(value))
	freeSpace := p.freeSpace
	if requiredSpace > p.freeSpace {
		// TODO: split current page
		return fmt.Errorf("not enough space left in page. required: %d, free space: %d", requiredSpace, freeSpace)
	}

	// Calculate the offset of the new cell
	offset := uint32(leafPageFirstCellOffset)
	for iter := p.iter(); iter.hasNext(); {
		cell := iter.next()
		if bytes.Compare(cell.key, key) == 1 {
			// If we find a key that's greater than the one we're adding,
			// we've found our insertion point
			break
		}
		offset = cell.offset + getLeafNodeCellSize(len(cell.key), len(cell.value))
	}

	rhsSize := uint32(len(p.data)) - offset - freeSpace
	if rhsSize > 0 {
		rhsSrc := p.data[offset : offset+rhsSize]
		rhsDst := p.data[offset+requiredSpace : offset+requiredSpace+rhsSize]
		copy(rhsDst, rhsSrc)
	}

	keyLen := uint32(len(key))
	valueLen := uint32(len(value))

	binary.LittleEndian.PutUint32(p.data[offset:offset+4], keyLen)
	offset += 4
	copy(p.data[offset:offset+keyLen], key)
	offset += keyLen

	binary.LittleEndian.PutUint32(p.data[offset:offset+4], valueLen)
	offset += 4
	copy(p.data[offset:offset+valueLen], value)
	offset += valueLen

	p.freeSpace -= requiredSpace
	p.setNumCells(p.getNumCells() + 1)

	return nil
}

func (p *leafPage) findCell(key []byte) ([]byte, error) {
	var foundValue []byte = nil
	for iter := p.iter(); iter.hasNext(); {
		cell := iter.next()
		if bytes.Equal(key, cell.key) {
			foundValue = make([]byte, len(cell.value))
			copy(foundValue, cell.value)
			break
		}
	}
	return foundValue, nil
}
