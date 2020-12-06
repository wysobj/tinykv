package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	conf *config.Config
	db   *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	standAloneStore := new(StandAloneStorage)
	standAloneStore.conf = conf

	return standAloneStore
}

func (s *StandAloneStorage) Start() error {
	s.db = engine_util.CreateDB(s.conf.DBPath, false)

	return nil
}

func (s *StandAloneStorage) Stop() error {
	err := s.db.Close()

	return err
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	ssr := NewStandAloneStorageReader(s)
	return ssr, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	var err error
	for _, v := range batch {
		switch v.Data.(type) {
		case storage.Put:
			put := v.Data.(storage.Put)
			err = engine_util.PutCF(s.db, put.Cf, put.Key, put.Value)
		case storage.Delete:
			delete := v.Data.(storage.Delete)
			err = engine_util.DeleteCF(s.db, delete.Cf, delete.Key)
		}

		if err != nil {
			break
		}
	}

	return err
}
