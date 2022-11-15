package tinykv

import (
	"bytes"
	"encoding/binary"
	"fmt"
)

type pageKind uint8

const (
	pageSize uint32 = 4096

	pageKindUnallocated pageKind = iota
	pageKindHeader
	pageKindLeaf
	pageKindInternal
)

type page struct {
	index      uint32
	cachedData []uint8
}

func (p *page) getKind() pageKind {
	return pageKind(p.cachedData[0])
}

func (p *page) setKind(kind pageKind) {
	p.cachedData[0] = uint8(kind)
}

func (p *page) isRoot() bool {
	return p.cachedData[1] == 1
}

func (p *page) setIsRoot(isRoot bool) {
	p.cachedData[1] = 0
	if isRoot {
		p.cachedData[1] = 1
	}
}

// Returns the parent page's index
func (p *page) getParentIndex() int32 {
	return int32(binary.LittleEndian.Uint32(p.cachedData[2:6]))
}

func (p *page) setParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(p.cachedData[2:6], uint32(parentIndex))
}

// Returns the number of cells
func (p *page) getNumCells() uint32 {
	return binary.LittleEndian.Uint32(p.cachedData[6:10])
}

func (p *page) setNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(p.cachedData[6:10], numCells)
}

func (p *page) getFreeSpace() uint32 {
	offset := p.iterCells(func(key, value []byte, entryOffset uint32) bool {
		return true
	})
	return uint32(pageSize) - offset
}

// Iterates through all of the cells of this page in order
// and returns the final byte offset where the iteration ended.
func (p *page) iterCells(callback func(key, value []byte, offset uint32) bool) uint32 {
	offset := uint32(10)

	switch p.getKind() {
	case pageKindLeaf:
		for i := uint32(0); i < p.getNumCells(); i++ {
			entryOffset := offset

			keyLen := binary.LittleEndian.Uint32(p.cachedData[offset : offset+4])
			offset += 4
			key := p.cachedData[offset : offset+keyLen]
			offset += keyLen

			valueLen := binary.LittleEndian.Uint32(p.cachedData[offset : offset+4])
			offset += 4
			value := p.cachedData[offset : offset+valueLen]
			offset += valueLen

			if !callback(key, value, entryOffset) {
				break
			}
		}
	case pageKindInternal:
		panic("TODO: IterCells for internal node")
	default:
		panic("invalid page kind")
	}

	return offset
}

// Adds a cell to the page
func (p *page) addCell(key, value []byte) error {
	switch p.getKind() {
	case pageKindLeaf:
		requiredSpace := uint32(len(key) + len(value) + 8)
		freeSpace := p.getFreeSpace()
		if requiredSpace > freeSpace {
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
			rhsSrc := p.cachedData[offset : offset+rhsSize]
			rhsDst := p.cachedData[offset+requiredSpace : offset+requiredSpace+rhsSize]
			copy(rhsDst, rhsSrc)
		}

		keyLen := uint32(len(key))
		valueLen := uint32(len(value))

		binary.LittleEndian.PutUint32(p.cachedData[offset:offset+4], keyLen)
		offset += 4
		copy(p.cachedData[offset:offset+keyLen], key)
		offset += keyLen

		binary.LittleEndian.PutUint32(p.cachedData[offset:offset+4], valueLen)
		offset += 4
		copy(p.cachedData[offset:offset+valueLen], value)
		offset += valueLen

		p.setNumCells(p.getNumCells() + 1)
	case pageKindInternal:
		panic("TODO: AddCell for internal node")
	default:
		panic("invalid page kind")
	}

	return nil
}

func (p *page) findCell(key []byte) ([]byte, error) {
	var foundValue []byte = nil

	switch p.getKind() {
	case pageKindLeaf:
		p.iterCells(func(entryKey, entryValue []byte, entryOffset uint32) bool {
			if bytes.Equal(key, entryKey) {
				foundValue = make([]byte, len(entryValue))
				copy(foundValue, entryValue)
				return false
			}
			return true
		})
	case pageKindInternal:
		panic("TODO: FindCell for internal node")
	default:
		panic("invalid page kind")
	}

	return foundValue, nil
}

