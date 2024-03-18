// Here the blockchain structrue is defined
// each node in this system will maintain a blockchain object.

package chain

import (
	"blockEmulator/core"
	"blockEmulator/params"
	"blockEmulator/storage"
	"blockEmulator/utils"
	"errors"
	"fmt"
	"log"
	"math/big"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/trie"
)

type BlockChain struct {
	db           ethdb.Database      // the leveldb database to store in the disk, for status trie
	triedb       *trie.Database      // the trie database which helps to store the status trie
	ChainConfig  *params.ChainConfig // the chain configuration, which can help to identify the chain
	CurrentBlock *core.Block         // the top block in this blockchain
	CurBlocks    map[uint64]*core.Block
	Storage      *storage.Storage  // Storage is the bolt-db to store the blocks
	Txpool       *core.TxPool      // the transaction pool
	PartitionMap map[string]uint64 // the partition map which is defined by some algorithm can help account parition
	pmlock       sync.RWMutex
}

// Get the transaction root, this root can be used to check the transactions
func GetTxTreeRoot(txs []*core.Transaction) []byte {
	// use a memory trie database to do this, instead of disk database
	triedb := trie.NewDatabase(rawdb.NewMemoryDatabase())
	transactionTree := trie.NewEmpty(triedb)
	for _, tx := range txs {
		transactionTree.Update(tx.TxHash, tx.Encode())
	}
	return transactionTree.Hash().Bytes()
}

// Write Partition Map
// 写入分区映射
func (bc *BlockChain) Update_PartitionMap(key string, val uint64) {
	bc.pmlock.Lock()
	defer bc.pmlock.Unlock()
	bc.PartitionMap[key] = val
}

// Get parition (if not exist, return default)
// 获取partion（如果不存在，返回默认值）
func (bc *BlockChain) Get_PartitionMap(key string) uint64 {
	bc.pmlock.RLock()
	defer bc.pmlock.RUnlock()
	if _, ok := bc.PartitionMap[key]; !ok {
		return uint64(utils.Addr2Shard(key))
	}
	return bc.PartitionMap[key]
}

// Send a transaction to the pool (need to decide which pool should be sended)
// 将事务发送到池（需要决定应发送哪个池）   bc决定？
func (bc *BlockChain) SendTx2Pool(txs []*core.Transaction) {
	bc.Txpool.AddTxs2Pool(txs)
}

// handle transactions and modify the status trie
// 处理事务并修改状态trie
func (bc *BlockChain) GetUpdateStatusTrie(txs []*core.Transaction) common.Hash {
	fmt.Printf("The len of txs is %d\n", len(txs))
	// the empty block (length of txs is 0) condition
	if len(txs) == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	// build trie from the triedb (in disk)
	//从triedb（在磁盘中）构建trie
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	cnt := 0
	// handle transactions, the signature check is ignored here
	//处理事务，此处忽略签名检查
	for i, tx := range txs {
		// fmt.Printf("tx %d: %s, %s\n", i, tx.Sender, tx.Recipient)
		// senderIn := false
		if !tx.Relayed && (bc.Get_PartitionMap(tx.Sender) == bc.ChainConfig.ShardID || tx.HasBroker) {
			// senderIn = true
			// fmt.Printf("the sender %s is in this shard %d, \n", tx.Sender, bc.ChainConfig.ShardID)
			// modify local accountstate
			//修改本地帐户状态
			s_state_enc, _ := st.Get([]byte(tx.Sender))
			var s_state *core.AccountState
			if s_state_enc == nil {
				// fmt.Println("missing account SENDER, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				s_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				s_state = core.DecodeAS(s_state_enc)
			}
			s_balance := s_state.Balance
			if s_balance.Cmp(tx.Value) == -1 {
				fmt.Printf("the balance is less than the transfer amount\n")
				continue
			}
			s_state.Deduct(tx.Value)
			st.Update([]byte(tx.Sender), s_state.Encode())
			cnt++
		}
		// recipientIn := false
		if bc.Get_PartitionMap(tx.Recipient) == bc.ChainConfig.ShardID || tx.HasBroker {
			// fmt.Printf("the recipient %s is in this shard %d, \n", tx.Recipient, bc.ChainConfig.ShardID)
			// recipientIn = true
			// modify local state
			r_state_enc, _ := st.Get([]byte(tx.Recipient))
			var r_state *core.AccountState
			if r_state_enc == nil {
				// fmt.Println("missing account RECIPIENT, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				r_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				r_state = core.DecodeAS(r_state_enc)
			}
			r_state.Deposit(tx.Value)
			st.Update([]byte(tx.Recipient), r_state.Encode())
			cnt++
		}

		// if senderIn && !recipientIn {
		// 	// change this part to the pbft stage
		// 	fmt.Printf("this transaciton is cross-shard txs, will be sent to relaypool later\n")
		// }
	}
	// commit the memory trie to the database in the disk
	//将内存trie提交到磁盘中的数据库
	if cnt == 0 {
		return common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	}
	rt, ns := st.Commit(false)
	err = bc.triedb.Update(trie.NewWithNodeSet(ns))
	if err != nil {
		log.Panic()
	}
	err = bc.triedb.Commit(rt, false)
	if err != nil {
		log.Panic(err)
	}
	fmt.Println("modified account number is ", cnt)
	return rt
}
func (bc *BlockChain) GetUpdateStatusTries(txs []*core.Transaction, id uint64) common.Hash {
	fmt.Printf("The len of txs is %d\n", len(txs))
	// the empty block (length of txs is 0) condition
	if len(txs) == 0 {
		return common.BytesToHash(bc.CurBlocks[id].Header.StateRoot)
	}
	// build trie from the triedb (in disk)
	//从triedb（在磁盘中）构建trie
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurBlocks[id].Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	cnt := 0
	// handle transactions, the signature check is ignored here
	//处理事务，此处忽略签名检查
	for i, tx := range txs {
		// fmt.Printf("tx %d: %s, %s\n", i, tx.Sender, tx.Recipient)
		// senderIn := false
		if !tx.Relayed && (bc.Get_PartitionMap(tx.Sender) == bc.ChainConfig.ShardID || tx.HasBroker) {
			// senderIn = true
			// fmt.Printf("the sender %s is in this shard %d, \n", tx.Sender, bc.ChainConfig.ShardID)
			// modify local accountstate
			//修改本地帐户状态
			s_state_enc, _ := st.Get([]byte(tx.Sender))
			var s_state *core.AccountState
			if s_state_enc == nil {
				// fmt.Println("missing account SENDER, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				s_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				s_state = core.DecodeAS(s_state_enc)
			}
			s_balance := s_state.Balance
			if s_balance.Cmp(tx.Value) == -1 {
				fmt.Printf("the balance is less than the transfer amount\n")
				continue
			}
			s_state.Deduct(tx.Value)
			st.Update([]byte(tx.Sender), s_state.Encode())
			cnt++
		}
		// recipientIn := false
		if bc.Get_PartitionMap(tx.Recipient) == bc.ChainConfig.ShardID || tx.HasBroker {
			// fmt.Printf("the recipient %s is in this shard %d, \n", tx.Recipient, bc.ChainConfig.ShardID)
			// recipientIn = true
			// modify local state
			r_state_enc, _ := st.Get([]byte(tx.Recipient))
			var r_state *core.AccountState
			if r_state_enc == nil {
				// fmt.Println("missing account RECIPIENT, now adding account")
				ib := new(big.Int)
				ib.Add(ib, params.Init_Balance)
				r_state = &core.AccountState{
					Nonce:   uint64(i),
					Balance: ib,
				}
			} else {
				r_state = core.DecodeAS(r_state_enc)
			}
			r_state.Deposit(tx.Value)
			st.Update([]byte(tx.Recipient), r_state.Encode())
			cnt++
		}

		// if senderIn && !recipientIn {
		// 	// change this part to the pbft stage
		// 	fmt.Printf("this transaciton is cross-shard txs, will be sent to relaypool later\n")
		// }
	}
	// commit the memory trie to the database in the disk
	//将内存trie提交到磁盘中的数据库
	if cnt == 0 {
		return common.BytesToHash(bc.CurBlocks[id].Header.StateRoot)
	}
	rt, ns := st.Commit(false)
	err = bc.triedb.Update(trie.NewWithNodeSet(ns))
	if err != nil {
		log.Panic()
	}
	err = bc.triedb.Commit(rt, false)
	if err != nil {
		log.Panic(err)
	}
	fmt.Println("modified account number is ", cnt)
	return rt
}

// generate (mine) a block, this function return a block
func (bc *BlockChain) GenerateBlock() *core.Block {
	// pack the transactions from the txpool
	txs := bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)
	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Now(),
	}
	// handle transactions to build root
	rt := bc.GetUpdateStatusTrie(txs)

	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	b := core.NewBlock(bh, txs)
	b.Header.Miner = 0
	b.Hash = b.Header.Hash()
	return b
}
func (bc *BlockChain) GenerateBlocks(id uint64) *core.Block {
	// pack the transactions from the txpool
	txs := bc.Txpool.PackTxs(bc.ChainConfig.BlockSize)
	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurBlocks[id].Hash,
		Number:          bc.CurBlocks[id].Header.Number + 1,
		Time:            time.Now(),
	}
	// handle transactions to build root
	rt := bc.GetUpdateStatusTries(txs, id)

	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(txs)
	b := core.NewBlock(bh, txs)
	b.Header.Miner = 0
	b.Hash = b.Header.Hash()
	return b
}

// new a genisis block, this func will be invoked only once for a blockchain object
// 新的一个genisis块，这个函数将只为区块链对象调用一次
func (bc *BlockChain) NewGenisisBlock() *core.Block {
	body := make([]*core.Transaction, 0)
	bh := &core.BlockHeader{
		Number: 0,
	}
	// build a new trie database by db
	triedb := trie.NewDatabaseWithConfig(bc.db, &trie.Config{
		Cache:     0,
		Preimages: true,
	})
	bc.triedb = triedb
	statusTrie := trie.NewEmpty(triedb)
	bh.StateRoot = statusTrie.Hash().Bytes()
	bh.TxRoot = GetTxTreeRoot(body)
	b := core.NewBlock(bh, body)
	b.Hash = b.Header.Hash()
	return b
}

// add the genisis block in a blockchain
func (bc *BlockChain) AddGenisisBlock(gb *core.Block) {
	bc.Storage.AddBlock(gb)
	newestHash, err := bc.Storage.GetNewestBlockHash()
	if err != nil {
		log.Panic()
	}
	curb, err := bc.Storage.GetBlock(newestHash)
	if err != nil {
		log.Panic()
	}
	bc.CurrentBlock = curb
}
func (bc *BlockChain) AddGenisisBlocks(gb *core.Block, id uint64) {
	bc.Storage.AddBlocks(gb, id)
	println("AddGenisisBlocks")
	newestHash, err := bc.Storage.GetNewestBlockHashs(id)
	if err != nil {
		println("bc.Storage.GetNewestBlockHashs")
		log.Panic()
	}
	curb, err := bc.Storage.GetBlocks(newestHash, id)
	if err != nil {
		println("bc.Storage.GetBlocks(newestHash, id)")
		log.Panic()
	}
	bc.CurBlocks[id] = curb
}

// add a block
func (bc *BlockChain) AddBlock(b *core.Block) {
	if b.Header.Number != bc.CurrentBlock.Header.Number+1 {
		fmt.Println("the block height is not correct")
		return
	}
	// if this block is mined by the node, the transactions is no need to be handled again
	//如果该块由节点挖掘，则不需要再次处理事务
	if b.Header.Miner != bc.ChainConfig.NodeID {
		rt := bc.GetUpdateStatusTrie(b.Body)
		fmt.Println(bc.CurrentBlock.Header.Number+1, "the root = ", rt.Bytes())
	}
	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}
func (bc *BlockChain) AddBlocks(b *core.Block, id uint64) {
	if b.Header.Number != bc.CurBlocks[id].Header.Number+1 {
		fmt.Println("the block height is not correct")
		return
	}
	// if this block is mined by the node, the transactions is no need to be handled again
	//如果该块由节点挖掘，则不需要再次处理事务
	if b.Header.Miner != bc.ChainConfig.NodeID {
		rt := bc.GetUpdateStatusTries(b.Body, id)
		fmt.Println(bc.CurBlocks[id].Header.Number+1, "the root = ", rt.Bytes())
	}
	bc.CurBlocks[id] = b
	bc.Storage.AddBlocks(b, id)
}

// new a blockchain.
// the ChainConfig is pre-defined to identify the blockchain; the db is the status trie database in disk
// ChainConfiguration是预定义的，用于识别区块链；数据库是磁盘中的状态trie数据库
//
//	func NewBlockChain(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
//		fmt.Println("Generating a new blockchain", db)
//		//var bcs=make(map[uint64]*BlockChain)
//		bc := &BlockChain{
//			db:           db,
//			ChainConfig:  cc,
//			Txpool:       core.NewTxPool(),
//			Storage:      storage.NewStorage(cc),
//			PartitionMap: make(map[string]uint64),
//		}
//
//		curHash, err := bc.Storage.GetNewestBlockHash()
//
//		if err != nil {
//			fmt.Println("Get newest block hash err")
//			// if the Storage bolt database cannot find the newest blockhash,
//			// it means the blockchain should be built in height = 0
//			if err.Error() == "cannot find the newest block hash" {
//				genisisBlock := bc.NewGenisisBlock()
//				bc.AddGenisisBlock(genisisBlock)
//				fmt.Println("New genisis block")
//
//				return bc, nil
//			}
//			log.Panic()
//		}
//
//		// there is a blockchain in the storage
//		fmt.Println("Existing blockchain found")
//		curb, err := bc.Storage.GetBlock(curHash)
//		if err != nil {
//			log.Panic()
//		}
//
//		bc.CurrentBlock = curb
//		triedb := trie.NewDatabaseWithConfig(db, &trie.Config{
//			Cache:     0,
//			Preimages: true,
//		})
//		bc.triedb = triedb
//		// check the existence of the trie database
//		_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
//		if err != nil {
//			log.Panic()
//		}
//		fmt.Println("The status trie can be built")
//		fmt.Println("Generated a new blockchain successfully")
//		return bc, nil
//
// }
func NewBlockChains(cc *params.ChainConfig, db ethdb.Database) (*BlockChain, error) {
	fmt.Println("Generating a new blockchain", db)
	//var bcs=make(map[uint64]*BlockChain)
	bc := &BlockChain{
		db:           db,
		ChainConfig:  cc,
		Txpool:       core.NewTxPool(),
		Storage:      storage.NewStorage(cc),
		PartitionMap: make(map[string]uint64),
		CurBlocks:    make(map[uint64]*core.Block),
	}

	for i := uint64(0); i < cc.ShardNums; i++ {
		curHash, err := bc.Storage.GetNewestBlockHashs(i)
		if err != nil {
			fmt.Println("Get newest block hash err")
			// if the Storage bolt database cannot find the newest blockhash,
			// it means the blockchain should be built in height = 0
			if err.Error() == "cannot find the newest block hash" {
				genisisBlock := bc.NewGenisisBlock()
				bc.AddGenisisBlocks(genisisBlock, i)
				fmt.Println("New genisis block")
				continue
			}
			log.Panic()
		}

		// there is a blockchain in the storage
		fmt.Println("Existing blockchain found")
		curb, err := bc.Storage.GetBlocks(curHash, i)
		if err != nil {
			log.Panic()
		}

		bc.CurBlocks[i] = curb
		triedb := trie.NewDatabaseWithConfig(db, &trie.Config{
			Cache:     0,
			Preimages: true,
		})
		bc.triedb = triedb
		// check the existence of the trie database
		_, err = trie.New(trie.TrieID(common.BytesToHash(curb.Header.StateRoot)), triedb)
		if err != nil {
			log.Panic()
		}
		fmt.Println("The status trie can be built")
		fmt.Println("Generated a new blockchain successfully")

	}
	return bc, nil
}

// check a block is valid or not in this blockchain config\
// 检查区块链配置中的区块是否有效
func (bc *BlockChain) IsValidBlock(b *core.Block) error {
	if string(b.Header.ParentBlockHash) != string(bc.CurrentBlock.Hash) {
		fmt.Println("the parentblock hash is not equal to the current block hash")
		return errors.New("the parentblock hash is not equal to the current block hash")
	} else if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) {
		fmt.Println("the transaction root is wrong")
		return errors.New("the transaction root is wrong")
	}
	return nil
}
func (bc *BlockChain) IsValidBlocks(b *core.Block, id uint64) error {
	if string(b.Header.ParentBlockHash) != string(bc.CurBlocks[id].Hash) {
		fmt.Println("the parentblock hash is not equal to the current block hash")
		return errors.New("the parentblock hash is not equal to the current block hash")
	} else if string(GetTxTreeRoot(b.Body)) != string(b.Header.TxRoot) {
		fmt.Println("the transaction root is wrong")
		return errors.New("the transaction root is wrong")
	}
	return nil
}

// add accounts
func (bc *BlockChain) AddAccounts(ac []string, as []*core.AccountState) {
	fmt.Printf("The len of accounts is %d, now adding the accounts\n", len(ac))

	bh := &core.BlockHeader{
		ParentBlockHash: bc.CurrentBlock.Hash,
		Number:          bc.CurrentBlock.Header.Number + 1,
		Time:            time.Time{},
	}
	// handle transactions to build root
	rt := common.BytesToHash(bc.CurrentBlock.Header.StateRoot)
	if len(ac) != 0 {
		st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
		if err != nil {
			log.Panic(err)
		}
		for i, addr := range ac {
			if bc.Get_PartitionMap(addr) == bc.ChainConfig.ShardID {
				ib := new(big.Int)
				ib.Add(ib, as[i].Balance)
				new_state := &core.AccountState{
					Balance: ib,
					Nonce:   as[i].Nonce,
				}
				st.Update([]byte(addr), new_state.Encode())
			}
		}
		rrt, ns := st.Commit(false)
		err = bc.triedb.Update(trie.NewWithNodeSet(ns))
		if err != nil {
			log.Panic(err)
		}
		err = bc.triedb.Commit(rt, false)
		if err != nil {
			log.Panic(err)
		}
		rt = rrt
	}

	emptyTxs := make([]*core.Transaction, 0)
	bh.StateRoot = rt.Bytes()
	bh.TxRoot = GetTxTreeRoot(emptyTxs)
	b := core.NewBlock(bh, emptyTxs)
	b.Header.Miner = 0
	b.Hash = b.Header.Hash()

	bc.CurrentBlock = b
	bc.Storage.AddBlock(b)
}

// fetch accounts
func (bc *BlockChain) FetchAccounts(addrs []string) []*core.AccountState {
	res := make([]*core.AccountState, 0)
	st, err := trie.New(trie.TrieID(common.BytesToHash(bc.CurrentBlock.Header.StateRoot)), bc.triedb)
	if err != nil {
		log.Panic(err)
	}
	for _, addr := range addrs {
		asenc, _ := st.Get([]byte(addr))
		var state_a *core.AccountState
		if asenc == nil {
			ib := new(big.Int)
			ib.Add(ib, params.Init_Balance)
			state_a = &core.AccountState{
				Nonce:   uint64(0),
				Balance: ib,
			}
		} else {
			state_a = core.DecodeAS(asenc)
		}
		res = append(res, state_a)
	}
	return res
}

// close a blockChain, close the database inferfaces
func (bc *BlockChain) CloseBlockChain() {
	bc.Storage.DataBase.Close()
	bc.triedb.CommitPreimages()
}

// print the details of a blockchain
func (bc *BlockChain) PrintBlockChain() string {
	vals := []interface{}{
		bc.CurrentBlock.Header.Number,
		bc.CurrentBlock.Hash,
		bc.CurrentBlock.Header.StateRoot,
		bc.CurrentBlock.Header.Time,
		bc.triedb,
		// len(bc.Txpool.RelayPool[1]),
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}
func (bc *BlockChain) PrintBlockChains(id uint64) string {
	vals := []interface{}{
		bc.CurBlocks[id].Header.Number,
		bc.CurBlocks[id].Hash,
		bc.CurBlocks[id].Header.StateRoot,
		bc.CurBlocks[id].Header.Time,
		bc.triedb,
		// len(bc.Txpool.RelayPool[1]),
	}
	res := fmt.Sprintf("%v\n", vals)
	fmt.Println(res)
	return res
}
