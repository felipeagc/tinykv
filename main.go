package main

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
)

type PageKind uint8

const (
	PAGE_SIZE uint32 = 4096

	PageKindUnallocated PageKind = iota
	PageKindHeader
	PageKindLeaf
	PageKindInternal
)

type Page struct {
	index      uint32
	cachedData []uint8
}

func (page *Page) GetKind() PageKind {
	return PageKind(page.cachedData[0])
}

func (page *Page) SetKind(kind PageKind) {
	page.cachedData[0] = uint8(kind)
}

func (page *Page) IsRoot() bool {
	return page.cachedData[1] == 1
}

func (page *Page) SetIsRoot(isRoot bool) {
	page.cachedData[1] = 0
	if isRoot {
		page.cachedData[1] = 1
	}
}

// Returns the parent page's index
func (page *Page) GetParentIndex() int32 {
	return int32(binary.LittleEndian.Uint32(page.cachedData[2:6]))
}

func (page *Page) SetParentIndex(parentIndex int32) {
	binary.LittleEndian.PutUint32(page.cachedData[2:6], uint32(parentIndex))
}

// Returns the number of cells
func (page *Page) GetNumCells() uint32 {
	return binary.LittleEndian.Uint32(page.cachedData[6:10])
}

func (page *Page) SetNumCells(numCells uint32) {
	binary.LittleEndian.PutUint32(page.cachedData[6:10], numCells)
}

func (page *Page) GetFreeSpace() uint32 {
	offset := page.IterCells(func(key, value []byte, entryOffset uint32) bool {
		return true
	})
	return uint32(PAGE_SIZE) - offset
}

// Iterates through all of the cells of this page in order
// and returns the final byte offset where the iteration ended.
func (page *Page) IterCells(callback func(key, value []byte, offset uint32) bool) uint32 {
	offset := uint32(10)

	switch page.GetKind() {
	case PageKindLeaf:
		for i := uint32(0); i < page.GetNumCells(); i++ {
			entryOffset := offset

			keyLen := binary.LittleEndian.Uint32(page.cachedData[offset : offset+4])
			offset += 4
			key := page.cachedData[offset : offset+keyLen]
			offset += keyLen

			valueLen := binary.LittleEndian.Uint32(page.cachedData[offset : offset+4])
			offset += 4
			value := page.cachedData[offset : offset+valueLen]
			offset += valueLen

			if !callback(key, value, entryOffset) {
				break
			}
		}
	case PageKindInternal:
		panic("TODO: IterCells for internal node")
	default:
		panic("invalid page kind")
	}

	return offset
}

// Adds a cell to the page
func (page *Page) AddCell(key, value []byte) error {
	switch page.GetKind() {
	case PageKindLeaf:
		requiredSpace := uint32(len(key) + len(value) + 8)
		freeSpace := page.GetFreeSpace()
		if requiredSpace > freeSpace {
			return fmt.Errorf("not enough space left in page. required: %d, free space: %d", requiredSpace, freeSpace)
		}

		// Calculate the offset of the new cell
		offset := uint32(PAGE_SIZE) - freeSpace
		page.IterCells(func(entryKey, entryValue []byte, entryOffset uint32) bool {
			if bytes.Compare(entryKey, key) == 1 {
				// If we find a key that's greater than the one we're adding,
				// we've found our insertion point
				offset = entryOffset
				return false
			}
			return true
		})

		rhsSize := uint32(PAGE_SIZE) - offset - freeSpace
		if rhsSize > 0 {
			rhsSrc := page.cachedData[offset : offset+rhsSize]
			rhsDst := page.cachedData[offset+requiredSpace : offset+requiredSpace+rhsSize]
			copy(rhsDst, rhsSrc)
		}

		keyLen := uint32(len(key))
		valueLen := uint32(len(value))

		binary.LittleEndian.PutUint32(page.cachedData[offset:offset+4], keyLen)
		offset += 4
		copy(page.cachedData[offset:offset+keyLen], key)
		offset += keyLen

		binary.LittleEndian.PutUint32(page.cachedData[offset:offset+4], valueLen)
		offset += 4
		copy(page.cachedData[offset:offset+valueLen], value)
		offset += valueLen

		page.SetNumCells(page.GetNumCells() + 1)
	case PageKindInternal:
		panic("TODO: AddCell for internal node")
	default:
		panic("invalid page kind")
	}

	return nil
}

func (page *Page) FindCell(key []byte) ([]byte, error) {
	var foundValue []byte = nil

	switch page.GetKind() {
	case PageKindLeaf:
		page.IterCells(func(entryKey, entryValue []byte, entryOffset uint32) bool {
			if bytes.Equal(key, entryKey) {
				foundValue = make([]byte, len(entryValue))
				copy(foundValue, entryValue)
				return false
			}
			return true
		})
	case PageKindInternal:
		panic("TODO: FindCell for internal node")
	default:
		panic("invalid page kind")
	}

	return foundValue, nil
}

type BufferPool struct {
	file  *os.File
	pages []*Page
}

func NewBufferPool(path string) (*BufferPool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	bp := &BufferPool{
		file: file,
	}

	pageCount, err := bp.GetPageCount()
	if err != nil {
		bp.Close()
		return nil, err
	}

	bp.pages = make([]*Page, pageCount)

	return bp, nil
}

func (bp *BufferPool) Close() {
	for pageIndex, page := range bp.pages {
		if page != nil {
			bp.FlushPage(uint32(pageIndex))
		}
	}
	bp.file.Close()
	bp.pages = []*Page{} // Free memory
}

func (bp *BufferPool) GetPageCount() (uint32, error) {
	fileInfo, err := bp.file.Stat()
	if err != nil {
		return 0, err
	}
	pageCount := uint32(fileInfo.Size()) / PAGE_SIZE
	return pageCount, nil
}

func (bp *BufferPool) AddPage() (*Page, error) {
	pageCount, err := bp.GetPageCount()
	if err != nil {
		return nil, err
	}

	page := &Page{
		index:      uint32(pageCount),
		cachedData: make([]uint8, PAGE_SIZE),
	}

	page.SetKind(PageKindLeaf)
	page.SetNumCells(0)
	page.SetIsRoot(true)
	page.SetParentIndex(-1)

	bp.pages = append(bp.pages, page)

	bp.FlushPage(page.index)

	return page, nil
}

func (bp *BufferPool) GetPage(pageIndex uint32) (*Page, error) {
	if len(bp.pages) <= int(pageIndex) {
		// This page is not created yet!
		return nil, fmt.Errorf("Invalid page index: %d\n", pageIndex)
	}

	if bp.pages[pageIndex] == nil {
		// Page is not cached in memory, so let's allocate space for it
		page := &Page{
			index:      pageIndex,
			cachedData: make([]uint8, PAGE_SIZE),
		}

		pageOffset := pageIndex * PAGE_SIZE
		_, err := bp.file.ReadAt(page.cachedData, int64(pageOffset))
		if err != nil {
			return nil, err
		}

		bp.pages[pageIndex] = page
	}

	return bp.pages[pageIndex], nil
}

func (bp *BufferPool) FlushPage(pageIndex uint32) error {
	page := bp.pages[pageIndex]
	if page == nil {
		return errors.New("tried to flush unloaded page")
	}

	_, err := bp.file.WriteAt(page.cachedData, int64(pageIndex*PAGE_SIZE))
	return err
}

type DB struct {
	bufferPool *BufferPool
}

func OpenDB(path string) (*DB, error) {
	bp, err := NewBufferPool(path)
	if err != nil {
		return nil, err
	}

	_, err = bp.AddPage()
	if err != nil {
		bp.Close()
		return nil, err
	}

	return &DB{
		bufferPool: bp,
	}, nil
}

func (db *DB) Close() {
	db.bufferPool.Close()
}

func (db *DB) Set(key, value []byte) error {
	page, err := db.bufferPool.GetPage(0)
	if err != nil {
		return err
	}

	if foundValue, _ := page.FindCell(key); foundValue != nil {
		panic("TODO: can't replace cells yet")
	}

	err = page.AddCell(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	page, err := db.bufferPool.GetPage(0)
	if err != nil {
		return nil, err
	}

	return page.FindCell(key)
}

func main() {
	bp, err := NewBufferPool("./test.db")
	if err != nil {
		panic(err)
	}
	defer bp.Close()

	_ = bp
}
