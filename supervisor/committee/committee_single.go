package committee

import (
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/core"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/signal"

	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"math/big"
	"os"
	"strconv"
	"time"
)

type Single_CommitteeModule struct {
	csvPath      string
	dataTotalNum int
	nowDataNum   int
	batchDataNum int
	IpNodeTable  map[uint64]map[uint64]string
	pl           *pbft_log.PbftLog
	Ss           *signal.StopSignal // to control the stop message sending
}

func NewSingle_CommitteeModule(Ip_nodeTable map[uint64]map[uint64]string, Ss *signal.StopSignal, plog *pbft_log.PbftLog, csvFilePath string, dataNum, batchNum int) *Single_CommitteeModule {
	return &Single_CommitteeModule{
		csvPath:      csvFilePath,
		dataTotalNum: dataNum,
		batchDataNum: batchNum,
		nowDataNum:   0,
		IpNodeTable:  Ip_nodeTable,
		Ss:           Ss,
		pl:           plog,
	}
}

// transfrom, data to transaction
// check whether it is a legal txs meesage. if so, read txs and put it into the txlist
func data5tx(data []string, nonce uint64) (*core.Transaction, bool) {
	//if data[6] == "0" && data[7] == "0" && len(data[3]) > 16 && len(data[4]) > 16 && data[3] != data[4] {
	//	val, ok := new(big.Int).SetString(data[8], 10)
	//	if !ok {
	//		log.Panic("new int failed\n")
	//	}
	//	tx := core.NewTransaction(data[3][2:], data[4][2:], val, nonce)
	//	return tx, true
	//}
	if len(data[6]) < 3 {
		val, ok := new(big.Int).SetString("10", 10)
		if !ok {
			log.Panic("new int failed\n")
		}
		hcp_code, _ := strconv.ParseUint(data[7], 10, 64)
		tx := core.NewTreeTransaction("0x32be343b94f860124dc4fee278fdcbd38c102d88", "0x104994f45d9d697ca104e5704a7b77d7fec3537c", val, nonce, data[5], data[6], hcp_code)
		return tx, true
	}
	return &core.Transaction{}, false
}

func (rthm *Single_CommitteeModule) HandleOtherMessage([]byte) {
	rthm.pl.Plog.Printf("hhhhhhhhhhhhhhhhhhhh\n")
}

func (rthm *Single_CommitteeModule) txSending(txlist []*core.Transaction) {
	// the txs will be sent
	sendToShard := make(map[uint64][]*core.Transaction)

	for idx := 0; idx <= len(txlist); idx++ {
		if idx > 0 && (idx%params.InjectSpeed == 0 || idx == len(txlist)) {
			// send to shard
			for sid := uint64(0); sid < uint64(1); sid++ {
				it := message.InjectTxs{
					Txs:       sendToShard[sid],
					ToShardID: sid,
				}
				itByte, err := json.Marshal(it)
				if err != nil {
					log.Println("commit3")
					log.Panic(err)
				}
				send_msg := message.MergeMessage(message.CInject, itByte)
				log.Println("CInject")
				go networks.TcpDial(send_msg, rthm.IpNodeTable[sid][0])
			}
			sendToShard = make(map[uint64][]*core.Transaction)
			time.Sleep(time.Second)
		}
		if idx == len(txlist) {
			break
		}
		tx := txlist[idx]
		//单区块链
		sendToShard[uint64(0)] = append(sendToShard[uint64(0)], tx)
		//在这里可以写根据tx的属性分到不同的子树
		//if tx.Medicaldata.State == "IL" {
		//	sendToShard[uint64(0)] = append(sendToShard[uint64(0)], tx)
		//}
		//if tx.Medicaldata.State == "OH" {
		//	sendToShard[uint64(1)] = append(sendToShard[uint64(1)], tx)
		//}
		//if tx.Medicaldata.State == "OK" {
		//	sendToShard[uint64(2)] = append(sendToShard[uint64(2)], tx)
		//}
		//if tx.Medicaldata.State == "PA" {
		//	sendToShard[uint64(3)] = append(sendToShard[uint64(3)], tx)
		//}
		//sendersid := uint64(utils.Addr2Shard(tx.Sender))
		//sendToShard[sendersid] = append(sendToShard[sendersid], tx)
	}
}

// read transactions, the Number of the transactions is - batchDataNum
func (rthm *Single_CommitteeModule) MsgSendingControl() {
	txfile, err := os.Open(rthm.csvPath)
	if err != nil {
		log.Println("commit1")
		log.Panic(err)
	}
	defer txfile.Close()
	reader := csv.NewReader(txfile)
	txlist := make([]*core.Transaction, 0) // save the txs in this epoch (round)

	for {
		data, err := reader.Read()
		if err == io.EOF {
			log.Println("commit12")
			break
		}
		if err != nil {
			log.Println("commit2")
			log.Panic(err)
		}
		if tx, ok := data4tx(data, uint64(rthm.nowDataNum)); ok {
			txlist = append(txlist, tx)
			rthm.nowDataNum++
		}

		// re-shard condition, enough edges
		if len(txlist) == int(rthm.batchDataNum) || rthm.nowDataNum == rthm.dataTotalNum {
			log.Println("commit42")
			rthm.txSending(txlist)
			// reset the variants about tx sending
			txlist = make([]*core.Transaction, 0)
			rthm.Ss.StopGap_Reset()
		}

		if rthm.nowDataNum == rthm.dataTotalNum {
			log.Println("commit22")
			break
		}
	}
}

// no operation here
func (rthm *Single_CommitteeModule) HandleBlockInfo(b *message.BlockInfoMsg) {
	rthm.pl.Plog.Printf("received from shard %d in epoch %d.\n", b.SenderShardID, b.Epoch)
}
