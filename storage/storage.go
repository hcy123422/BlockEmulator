// storage is a key-value database and its interfaces indeed
// the information of block will be saved in storage

package storage

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"errors"
	"fmt"
	"log"
	"os"
	"strconv"

	"github.com/boltdb/bolt"
)

type Storage struct {
	num                   uint64
	blockchainid          uint64
	dbFilePath            string // path to the database
	blockBucket           string // bucket in bolt database
	blockHeaderBucket     string // bucket in bolt database
	newestBlockHashBucket string // bucket in bolt database
	DataBase              *bolt.DB
}

// new a storage, build a bolt datase
func NewStorage(cc *params.ChainConfig) *Storage {
	_, errStat := os.Stat("./record")
	if os.IsNotExist(errStat) {
		errMkdir := os.Mkdir("./record", os.ModePerm)
		if errMkdir != nil {
			log.Panic(errMkdir)
		}
	} else if errStat != nil {
		log.Panic(errStat)
	}

	s := &Storage{
		num:                   cc.ShardNums,
		blockchainid:          cc.ShardID,
		dbFilePath:            "./record/" + strconv.FormatUint(cc.ShardID, 10) + "_" + strconv.FormatUint(cc.NodeID, 10) + "_database",
		blockBucket:           "block",
		blockHeaderBucket:     "blockHeader",
		newestBlockHashBucket: "newestBlockHash",
	}

	db, err := bolt.Open(s.dbFilePath, 0600, nil)
	if err != nil {
		log.Panic(err)
	}

	// create buckets
	db.Update(func(tx *bolt.Tx) error {
		_, err := tx.CreateBucketIfNotExists([]byte(s.blockBucket))
		if err != nil {
			log.Panic("create blocksBucket failed")
		}
		for i := uint64(0); i < cc.ShardNums; i++ {
			_, err = tx.CreateBucketIfNotExists([]byte(s.blockBucket + string(i)))
			if err != nil {
				log.Panic("create blockBucket" + string(i) + " failed")
			}
			_, err = tx.CreateBucketIfNotExists([]byte(s.blockHeaderBucket + string(i)))
			if err != nil {
				log.Panic("create blockHeaderBucket" + string(i) + " failed")
			}

			_, err = tx.CreateBucketIfNotExists([]byte(s.newestBlockHashBucket + string(i)))
			if err != nil {
				log.Panic("create newestBlockHashBucket" + string(i) + " failed")
			}
		}
		_, err = tx.CreateBucketIfNotExists([]byte(s.blockHeaderBucket))
		if err != nil {
			log.Panic("create blockHeaderBucket failed")
		}

		_, err = tx.CreateBucketIfNotExists([]byte(s.newestBlockHashBucket))
		if err != nil {
			log.Panic("create newestBlockHashBucket failed")
		}

		return nil
	})
	s.DataBase = db
	return s
}

// update the newest block in the database
func (s *Storage) UpdateNewestBlock(newestbhash []byte) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		nbhBucket := tx.Bucket([]byte(s.newestBlockHashBucket))
		// the bucket has the only key "OnlyNewestBlock"
		err := nbhBucket.Put([]byte("OnlyNewestBlock"), newestbhash)
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
	fmt.Println("The newest block is updated")
}
func (s *Storage) UpdateNewestBlocks(newestbhash []byte, id uint64) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		nbhBucket := tx.Bucket([]byte(s.newestBlockHashBucket + string(id)))
		// the bucket has the only key "OnlyNewestBlock"
		err := nbhBucket.Put([]byte("OnlyNewestBlock"), newestbhash)
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
	fmt.Println("The newest block is updated")
}

// add a blockheader into the database
func (s *Storage) AddBlockHeader(blockhash []byte, bh *core.BlockHeader) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.blockHeaderBucket))
		err := bhbucket.Put(blockhash, bh.Encode())
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
}
func (s *Storage) AddBlockHeaders(blockhash []byte, bh *core.BlockHeader, id uint64) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.blockHeaderBucket + string(id)))
		err := bhbucket.Put(blockhash, bh.Encode())
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
}

// add a block into the database
func (s *Storage) AddBlock(b *core.Block) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		bbucket := tx.Bucket([]byte(s.blockBucket))
		err := bbucket.Put(b.Hash, b.Encode())
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
	s.AddBlockHeader(b.Hash, b.Header)
	s.UpdateNewestBlock(b.Hash)
	fmt.Println("Block is added")
}
func (s *Storage) AddBlocks(b *core.Block, id uint64) {
	err := s.DataBase.Update(func(tx *bolt.Tx) error {
		bbucket := tx.Bucket([]byte(s.blockBucket + string(id)))
		err := bbucket.Put(b.Hash, b.Encode())
		if err != nil {
			log.Panic()
		}
		return nil
	})
	if err != nil {
		log.Panic()
	}
	s.AddBlockHeaders(b.Hash, b.Header, id)
	s.UpdateNewestBlocks(b.Hash, id)
	fmt.Println("Block is added")
}

// read  blockheaders from the database
func (s *Storage) GetBlockHeaders(bhash []byte) ([]*core.BlockHeader, error) {
	var res *core.BlockHeader
	var headers []*core.BlockHeader
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		for i := uint64(0); i < s.num; i++ {
			bhbucket := tx.Bucket([]byte(s.blockHeaderBucket + string(i)))
			bh_encoded := bhbucket.Get(bhash)
			if bh_encoded == nil {
				return errors.New("the block is not existed")
			}
			res = core.DecodeBH(bh_encoded)
			headers = append(headers, res)
		}
		return nil
	})
	return headers, err
}

// read a blockheader from the database
func (s *Storage) GetBlockHeader(bhash []byte) (*core.BlockHeader, error) {
	var res *core.BlockHeader
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.blockHeaderBucket))
		bh_encoded := bhbucket.Get(bhash)
		if bh_encoded == nil {
			return errors.New("the block is not existed")
		}
		res = core.DecodeBH(bh_encoded)
		return nil
	})
	fmt.Println(err)
	return res, err
}

// read a block from the database
func (s *Storage) GetBlock(bhash []byte) (*core.Block, error) {
	var res *core.Block
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bbucket := tx.Bucket([]byte(s.blockBucket))
		b_encoded := bbucket.Get(bhash)
		if b_encoded == nil {
			return errors.New("the block is not existed")
		}
		res = core.DecodeB(b_encoded)
		return nil
	})
	return res, err
}
func (s *Storage) GetBlocks(bhash []byte, id uint64) (*core.Block, error) {
	var res *core.Block
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bbucket := tx.Bucket([]byte(s.blockBucket + string(id)))
		b_encoded := bbucket.Get(bhash)
		if b_encoded == nil {
			return errors.New("the block is not existed")
		}
		res = core.DecodeB(b_encoded)
		return nil
	})
	return res, err
}

// read the Newest block hash
func (s *Storage) GetNewestBlockHash() ([]byte, error) {
	var nhb []byte
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.newestBlockHashBucket))
		// the bucket has the only key "OnlyNewestBlock"
		nhb = bhbucket.Get([]byte("OnlyNewestBlock"))
		if nhb == nil {
			return errors.New("cannot find the newest block hash")
		}
		return nil
	})
	return nhb, err
}

// read the Newest block hash
func (s *Storage) GetNewestBlockHashs(id uint64) ([]byte, error) {
	var nhb []byte
	err := s.DataBase.View(func(tx *bolt.Tx) error {
		bhbucket := tx.Bucket([]byte(s.newestBlockHashBucket + string(id)))
		// the bucket has the only key "OnlyNewestBlock"
		nhb = bhbucket.Get([]byte("OnlyNewestBlock"))
		if nhb == nil {
			return errors.New("cannot find the newest block hash")
		}
		return nil
	})
	return nhb, err
}
