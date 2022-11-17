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
|      2 |    2 | reserved
|      4 |    4 | parent index
|      8 |    4 | free space
|     12 |    4 | cell count
|     16 |      | cells

Cell layout:
| OFFSET | SIZE | DATA
|      0 |    4 | key length
|      4 |   kl | key
|   4+kl |    4 | value length
|   8+kl |   vl | value
*/

const (
	leafPageFirstCellOffset  uint32 = 16
	leafPageDefaultFreeSpace uint32 = pageSize - leafPageFirstCellOffset
)

type leafPage struct {
	pageBase
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

func newLeafPage(data []byte) *leafPage {
	p := &leafPage{
		pageBase{
			data: data,
		},
	}

	if p.data == nil {
		p.data = make([]byte, pageSize)

		p.data[0] = byte(pageKindLeaf)
		p.setNumCells(0)
		p.setIsRoot(true)
		p.setParentIndex(-1)
		p.setFreeSpace(leafPageDefaultFreeSpace)
	}

	return p
}

func (p *leafPage) isRoot() bool {
	return p.data[1] == 1
}

func (p *leafPage) setIsRoot(isRoot bool) {
	p.data[1] = 0
	if isRoot {
		p.data[1] = 1
	}
}

func (p *leafPage) getParentIndex() int32 {
	return int32(binary.LittleEndian.Uint32(p.data[4:8]))
}

func (p *leafPage) setParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(p.data[4:8], uint32(parentIndex))
}

func (p *leafPage) getNumCells() uint32 {
	return binary.LittleEndian.Uint32(p.data[12:16])
}

func (p *leafPage) setNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(p.data[12:16], numCells)
}

func (p *leafPage) setFreeSpace(freeSpace uint32) {
	binary.LittleEndian.PutUint32(p.data[8:12], freeSpace)
}

func (p *leafPage) getFreeSpace() uint32 {
	return binary.LittleEndian.Uint32(p.data[8:12])
}

// Adds a cell to the page
func (p *leafPage) addCell(key, value []byte) error {
	requiredSpace := uint32(len(key) + len(value) + 8)
	freeSpace := p.getFreeSpace()
	if requiredSpace > freeSpace {
		// TODO: split current page
		return fmt.Errorf("not enough space left in page. required: %d, free space: %d", requiredSpace, freeSpace)
	}

	// Calculate the offset of the new cell
	offset := uint32(pageSize) - freeSpace

	for iter := p.iter(); iter.hasNext(); {
		cell := iter.next()
		if bytes.Compare(cell.key, key) == 1 {
			// If we find a key that's greater than the one we're adding,
			// we've found our insertion point
			offset = cell.offset
			break
		}
	}

	rhsSize := uint32(pageSize) - offset - freeSpace
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

	p.setFreeSpace(freeSpace - requiredSpace)
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
