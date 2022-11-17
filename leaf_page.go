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
|      2 |    4 | parent index
|      6 |    4 | cell count
|     10 |      | cells

Cell layout:
| OFFSET | SIZE | DATA
|      0 |    4 | key length
|      4 |   kl | key
|   4+kl |    4 | value length
|   8+kl |   vl | value

*/

type leafPage struct {
	pageBase
}

func newLeafPage(data []byte) *leafPage {
	p := &leafPage{
		pageBase{
			data:  data,
		},
	}

	if p.data == nil {
		p.data = make([]uint8, pageSize)

		p.data[0] = uint8(pageKindLeaf)
		p.setNumCells(0)
		p.setIsRoot(true)
		p.setParentIndex(-1)
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
	return int32(binary.LittleEndian.Uint32(p.data[2:6]))
}

func (p *leafPage) setParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(p.data[2:6], uint32(parentIndex))
}

func (p *leafPage) getNumCells() uint32 {
	return binary.LittleEndian.Uint32(p.data[6:10])
}

func (p *leafPage) setNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(p.data[6:10], numCells)
}

func (p *leafPage) getFreeSpace() uint32 {
	offset := p.iterCells(func(key, value []byte, entryOffset uint32) bool {
		return true
	})
	return uint32(pageSize) - offset
}

// Iterates through all of the cells of this page in order
// and returns the final byte offset where the iteration ended.
func (p *leafPage) iterCells(callback func(key, value []byte, offset uint32) bool) uint32 {
	offset := uint32(10)
	for i := uint32(0); i < p.getNumCells(); i++ {
		entryOffset := offset

		keyLen := binary.LittleEndian.Uint32(p.data[offset : offset+4])
		offset += 4
		key := p.data[offset : offset+keyLen]
		offset += keyLen

		valueLen := binary.LittleEndian.Uint32(p.data[offset : offset+4])
		offset += 4
		value := p.data[offset : offset+valueLen]
		offset += valueLen

		if !callback(key, value, entryOffset) {
			break
		}
	}
	return offset
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
	p.iterCells(func(entryKey, entryValue []byte, entryOffset uint32) bool {
		if bytes.Compare(entryKey, key) == 1 {
			// If we find a key that's greater than the one we're adding,
			// we've found our insertion point
			offset = entryOffset
			return false
		}
		return true
	})

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

	p.setNumCells(p.getNumCells() + 1)

	return nil
}

func (p *leafPage) findCell(key []byte) ([]byte, error) {
	var foundValue []byte = nil
	p.iterCells(func(entryKey, entryValue []byte, entryOffset uint32) bool {
		if bytes.Equal(key, entryKey) {
			foundValue = make([]byte, len(entryValue))
			copy(foundValue, entryValue)
			return false
		}
		return true
	})
	return foundValue, nil
}
