package tinykv

import (
	"errors"
	"fmt"
	"os"
)

type bufferPool struct {
	file  *os.File
	pages []page
}

func newBufferPool(path string) (*bufferPool, error) {
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, err
	}

	bp := &bufferPool{
		file: file,
	}

	pageCount, err := bp.getPageCount()
	if err != nil {
		bp.close()
		return nil, err
	}

	bp.pages = make([]page, pageCount)

	return bp, nil
}

func (bp *bufferPool) close() {
	for pageIndex, page := range bp.pages {
		if page != nil {
			bp.flushPage(uint32(pageIndex))
		}
	}
	bp.file.Close()
	bp.pages = []page{} // Free memory
}

func (bp *bufferPool) getPageCount() (uint32, error) {
	fileInfo, err := bp.file.Stat()
	if err != nil {
		return 0, err
	}
	pageCount := uint32(fileInfo.Size()) / defaultPageSize
	return pageCount, nil
}

func (bp *bufferPool) addPage(page page) error {
	pageIndex, err := bp.getPageCount()
	if err != nil {
		return err
	}

	bp.pages = append(bp.pages, page)
	bp.flushPage(pageIndex)

	return nil
}

func (bp *bufferPool) getPage(pageIndex uint32) (page, error) {
	if len(bp.pages) <= int(pageIndex) {
		// This page is not created yet!
		return nil, fmt.Errorf("Invalid page index: %d\n", pageIndex)
	}

	if bp.pages[pageIndex] == nil {
		// Page is not cached in memory, so let's allocate space for it
		pageData := make([]uint8, defaultPageSize)

		pageOffset := pageIndex * defaultPageSize
		_, err := bp.file.ReadAt(pageData, int64(pageOffset))
		if err != nil {
			return nil, err
		}

		var page page
		switch pageKind(pageData[0]) {
		case pageKindHeader:
			panic("TODO: import header page")
		case pageKindUnallocated:
			panic("TODO: import unallocated page")
		case pageKindLeaf:
			page = newLeafPage(pageData)
		case pageKindInternal:
			panic("TODO: import internal page")
		default:
			panic("invalid page kind")
		}

		bp.pages[pageIndex] = page
	}

	return bp.pages[pageIndex], nil
}

func (bp *bufferPool) flushPage(pageIndex uint32) error {
	page := bp.pages[pageIndex]
	if page == nil {
		return errors.New("tried to flush unloaded page")
	}

	_, err := bp.file.WriteAt(page.getData(), int64(pageIndex*defaultPageSize))
	return err
}
