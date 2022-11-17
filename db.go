package tinykv

type DB struct {
	bufferPool *bufferPool
}

func OpenDB(path string) (*DB, error) {
	bp, err := newBufferPool(path)
	if err != nil {
		return nil, err
	}

	err = bp.addPage(newLeafPage(nil))
	if err != nil {
		bp.close()
		return nil, err
	}

	return &DB{
		bufferPool: bp,
	}, nil
}

func (db *DB) Close() {
	db.bufferPool.close()
}

func (db *DB) Set(key, value []byte) error {
	page, err := db.bufferPool.getPage(0)
	if err != nil {
		return err
	}

	tPage := page.(treePage)

	if foundValue, _ := tPage.findCell(key); foundValue != nil {
		panic("TODO: can't replace cells yet")
	}

	err = tPage.addCell(key, value)
	if err != nil {
		return err
	}

	return nil
}

func (db *DB) Get(key []byte) ([]byte, error) {
	page, err := db.bufferPool.getPage(0)
	if err != nil {
		return nil, err
	}

	tPage := page.(treePage)

	return tPage.findCell(key)
}
