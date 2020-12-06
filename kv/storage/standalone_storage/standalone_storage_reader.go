package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
)

type StandAloneStorageReader struct {
	storage *StandAloneStorage
	txn     *badger.Txn
}

func NewStandAloneStorageReader(storage *StandAloneStorage) *StandAloneStorageReader {
	ssr := new(StandAloneStorageReader)
	ssr.storage = storage
	ssr.txn = nil

	return ssr
}

func (ssr *StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	txn := ssr.storage.db.NewTransaction(false)
	defer txn.Discard()

	val, err := engine_util.GetCFFromTxn(txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}

	return val, err
}

func (ssr *StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	txn := ssr.storage.db.NewTransaction(false)
	ssr.txn = txn

	return engine_util.NewCFIterator(cf, txn)
}

func (ssr *StandAloneStorageReader) Close() {
	ssr.txn.Discard()
}
