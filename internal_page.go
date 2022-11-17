package tinykv

type internalPage struct {
	index uint32
	cachedData []byte
}

func newInternalPage(index uint32, data []byte) *internalPage {
	p := &internalPage{
		index:      index,
		cachedData: data,
	}

	if p.cachedData == nil {
		p.cachedData = make([]uint8, pageSize)

		// p.cachedData[0] = uint8(pageKindLeaf)
		// p.setNumCells(0)
		// p.setIsRoot(true)
		// p.setParentIndex(-1)
	}

	return p
}
