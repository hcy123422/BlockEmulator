// The pbft consensus process

package pbft_all

import (
	"blockEmulator/chain"
	"blockEmulator/consensus_shard/pbft_all/dataSupport"
	"blockEmulator/consensus_shard/pbft_all/pbft_log"
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/shard"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"bufio"
	"encoding/csv"
	"encoding/json"
	"io"
	"log"
	"net"
	"os"
	"strconv"
	"sync"
	"time"

	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/ethdb"
)

type PbftConsensusNode struct {
	// the local config about pbft
	RunningNode *shard.Node // the node information
	ShardID     uint64      // denote the ID of the shard (or pbft), only one pbft consensus in a shard
	NodeID      uint64      // denote the ID of the node in the pbft (shard)

	// the data structure for blockchain
	CurChain  *chain.BlockChain // all node in the shard maintain the same blockchain ##分片中的所有节点都维护相同的区块链
	db        ethdb.Database    // to save the mpt
	CurChains []*chain.BlockChain
	Chains    map[uint64]*chain.BlockChain
	// the global config about pbft
	//pbft的全局配置
	pbftChainConfig *params.ChainConfig          // the chain config in this pbft
	ip_nodeTable    map[uint64]map[uint64]string // 表示特定节点的ip
	node_nums       uint64                       // the number of nodes in this pfbt, denoted by N
	malicious_nums  uint64                       // f, 3f + 1 = N
	view            uint64                       // 表示这个pbft的视图，主节点可以从这个变体中推断出来

	// the control message and message checking utils in pbft
	//pbft中的控制消息和消息检查实用程序
	seqids            map[uint64]uint64
	sequenceID        uint64                          // pbft的消息序列id
	stop              bool                            // send stop signal
	pStop             chan uint64                     // channle for stopping consensus
	requestPool       map[string]*message.Request     // RequestHash to Request
	cntPrepareConfirm map[string]map[*shard.Node]bool // count the prepare confirm message, [messageHash][Node]bool
	cntCommitConfirm  map[string]map[*shard.Node]bool // count the commit confirm message, [messageHash][Node]bool
	isCommitBordcast  map[string]bool                 // denote whether the commit is broadcast
	isReply           map[string]bool                 // denote whether the message is reply
	height2Digest     map[uint64]string               // sequence (block height) -> request, fast read

	// locks about pbft
	sequenceLock sync.Mutex // the lock of sequence
	lock         sync.Mutex // lock the stage
	askForLock   sync.Mutex // lock for asking for a serise of requests
	stopLock     sync.Mutex // lock the stop varient

	// seqID of other Shards, to synchronize
	//其他碎片的seqID，以进行同步
	seqIDMap   map[uint64]uint64
	seqMapLock sync.Mutex

	// logger
	pl *pbft_log.PbftLog
	// tcp control
	tcpln       net.Listener
	tcpPoolLock sync.Mutex

	// to handle the message in the pbft
	ihm ExtraOpInConsensus

	// to handle the message outside of pbft
	ohm OpInterShards
	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	//监事和委员会组成部分
	ComMod committee.CommitteeModule

	// measure components
	//测量组件
	testMeasureMods []measure.MeasureModule
}

// generate a pbft consensus for a node
func NewPbftNode(shardID, nodeID uint64, pcc *params.ChainConfig, messageHandleType string) *PbftConsensusNode {
	p := new(PbftConsensusNode)
	p.seqids = make(map[uint64]uint64)
	p.ip_nodeTable = params.IPmap_nodeTable
	p.node_nums = pcc.Nodes_perShard
	p.ShardID = shardID
	p.NodeID = nodeID
	p.pbftChainConfig = pcc
	fp := "./record/ldb/s" + strconv.FormatUint(shardID, 10) + "/n" + strconv.FormatUint(nodeID, 10)
	var err error
	p.db, err = rawdb.NewLevelDBDatabase(fp, 0, 1, "accountState", false)
	if err != nil {
		log.Panic(err)
	}
	//
	p.CurChain, err = chain.NewBlockChains(pcc, p.db)

	if err != nil {
		log.Panic("cannot new a blockchain")
	}
	//p.CurChains = append(p.CurChains, p.CurChain)
	if err != nil {
		log.Panic("cannot new a blockchain")
	}
	p.RunningNode = &shard.Node{
		NodeID:  nodeID,
		ShardID: shardID,
		IPaddr:  p.ip_nodeTable[shardID][nodeID],
	}

	p.stop = false
	p.sequenceID = p.CurChain.CurBlocks[shardID].Header.Number + 1
	p.pStop = make(chan uint64)
	p.requestPool = make(map[string]*message.Request)
	p.cntPrepareConfirm = make(map[string]map[*shard.Node]bool)
	p.cntCommitConfirm = make(map[string]map[*shard.Node]bool)
	p.isCommitBordcast = make(map[string]bool)
	p.isReply = make(map[string]bool)
	p.height2Digest = make(map[uint64]string)
	p.malicious_nums = (p.node_nums - 1) / 3
	p.view = 0
	for i := uint64(0); i < pcc.ShardNums; i++ {
		p.seqids[i] = p.CurChain.CurBlocks[i].Header.Number + 1
	}
	p.seqIDMap = make(map[uint64]uint64)

	p.pl = pbft_log.NewPbftLog(shardID, nodeID)

	// choose how to handle the messages in pbft or beyond pbft
	switch string(messageHandleType) {
	case "CLPA_Broker":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod_forBroker{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPABrokerOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "CLPA":
		ncdm := dataSupport.NewCLPADataSupport()
		p.ihm = &CLPAPbftInsideExtraHandleMod{
			pbftNode: p,
			cdm:      ncdm,
		}
		p.ohm = &CLPARelayOutsideModule{
			pbftNode: p,
			cdm:      ncdm,
		}
	case "Broker":
		p.ihm = &RawBrokerPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawBrokerOutsideModule{
			pbftNode: p,
		}
	case "Single":
		p.ihm = &RawPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawOutsideModule{
			pbftNode: p,
		}
	default:
		p.ihm = &RawRelayPbftExtraHandleMod{
			pbftNode: p,
		}
		p.ohm = &RawRelayOutsideModule{
			pbftNode: p,
		}
	}

	return p
}
func (p *PbftConsensusNode) NewSendPbftNode(pcc *params.ChainConfig) {
	p.Ss = signal.NewStopSignal(2 * int(pcc.ShardNums))
	p.ComMod = committee.NewSingle_CommitteeModule(p.ip_nodeTable, p.Ss, p.pl, params.FileInput2, params.TotalDataSize, params.BatchSize)
	p.testMeasureMods = make([]measure.MeasureModule, 0)
	p.testMeasureMods = append(p.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
}
func (p *PbftConsensusNode) SupervisorTxHandling() {
	p.ComMod.MsgSendingControl()
	log.Println("pbftuuuuuuuuuuuuu")
	// TxHandling is end
	for !p.Ss.GapEnough() { //
		time.Sleep(time.Second)
	}
	// send stop message
	log.Println("pbftstoppppp")
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	p.pl.Plog.Println("main node: now sending cstop message to all nodes")
	for sid := uint64(0); sid < p.pbftChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < p.pbftChainConfig.Nodes_perShard && nid != p.NodeID; nid++ {
			networks.TcpDial(stopmsg, p.ip_nodeTable[sid][nid])
		}
	}
	p.pl.Plog.Println("Send node: now Closing")

	p.CloseSendpbft()

}
func (p *PbftConsensusNode) handleBlockInfos(content []byte) {
	log.Println("handleBlockInfos")
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		log.Println("StopGap_Inc")
		p.Ss.StopGap_Inc()

	} else {
		log.Println("StopGap_Reset")
		p.Ss.StopGap_Reset()
	}

	//p.ComMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range p.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	// add codes here ...
}
func (p *PbftConsensusNode) CloseSendpbft() {
	p.pl.Plog.Println("Closing...")
	for _, measureMod := range p.testMeasureMods {
		p.pl.Plog.Println(measureMod.OutputMetricName())
		p.pl.Plog.Println(measureMod.OutputRecord())
		println()
	}

	p.pl.Plog.Println("Trying to input .csv")
	// write to .csv file
	dirpath := params.DataWrite_path + "supervisor_measureOutput/"
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	for _, measureMod := range p.testMeasureMods {
		targetPath := dirpath + measureMod.OutputMetricName() + ".csv"
		f, err := os.Open(targetPath)
		resultPerEpoch, totResult := measureMod.OutputRecord()
		resultStr := make([]string, 0)
		for _, result := range resultPerEpoch {
			resultStr = append(resultStr, strconv.FormatFloat(result, 'f', 8, 64))
		}
		resultStr = append(resultStr, strconv.FormatFloat(totResult, 'f', 8, 64))
		if err != nil && os.IsNotExist(err) {
			file, er := os.Create(targetPath)
			if er != nil {
				panic(er)
			}
			defer file.Close()

			w := csv.NewWriter(file)
			title := []string{measureMod.OutputMetricName()}
			w.Write(title)
			w.Flush()
			w.Write(resultStr)
			w.Flush()
		} else {
			file, err := os.OpenFile(targetPath, os.O_APPEND|os.O_RDWR, 0666)

			if err != nil {
				log.Panic(err)
			}
			defer file.Close()
			writer := csv.NewWriter(file)
			err = writer.Write(resultStr)
			if err != nil {
				log.Panic()
			}
			writer.Flush()
		}
		f.Close()
		p.pl.Plog.Println(measureMod.OutputRecord())
	}
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	networks.TcpDial(stopmsg, p.ip_nodeTable[p.NodeID][0])
}

// handle the raw message, send it to corresponded interfaces
func (p *PbftConsensusNode) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	// pbft inside message type
	case message.CPrePrepare:
		p.handlePrePrepare(content)
	case message.CPrepare:
		p.handlePrepare(content)
	case message.CCommit:
		p.handleCommit(content)
	case message.CBlockSingle:
		p.handleBlockInfos(content)
	case message.CRequestOldrequest:
		p.handleRequestOldSeq(content)
	case message.CSendOldrequest:
		p.handleSendOldSeq(content)
	case message.CStop:
		p.WaitToStop()

	// handle the message from outside
	default:
		log.Println("gggggggggggg")
		p.ohm.HandleMessageOutsidePBFT(msgType, content)
	}
}

func (p *PbftConsensusNode) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		if p.getStopSignal() {
			return
		}
		switch err {
		case nil:
			log.Println("ptfb_handleClientRequest")
			p.tcpPoolLock.Lock()
			p.handleMessage(clientRequest)
			p.tcpPoolLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Println("333333333")
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (p *PbftConsensusNode) TcpListen() {
	ln, err := net.Listen("tcp", p.RunningNode.IPaddr)
	p.tcpln = ln
	if err != nil {
		log.Println("11111111111")
		log.Panic(err)
	}
	for {
		conn, err := p.tcpln.Accept()
		if err != nil {
			log.Println("22222222222222")
			return
		}
		log.Println("ptfb")
		go p.handleClientRequest(conn)
	}
}

// listen to the request
func (p *PbftConsensusNode) OldTcpListen() {
	ipaddr, err := net.ResolveTCPAddr("tcp", p.RunningNode.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	ln, err := net.ListenTCP("tcp", ipaddr)
	p.tcpln = ln
	if err != nil {
		log.Panic(err)
	}
	p.pl.Plog.Printf("S%dN%d begins listening：%s\n", p.ShardID, p.NodeID, p.RunningNode.IPaddr)

	for {
		if p.getStopSignal() {
			p.closePbft()
			return
		}
		conn, err := p.tcpln.Accept()
		if err != nil {
			log.Panic(err)
		}
		b, err := io.ReadAll(conn)
		if err != nil {
			log.Panic(err)
		}
		p.handleMessage(b)
		conn.(*net.TCPConn).SetLinger(0)
		defer conn.Close()
	}
}

// when received stop
func (p *PbftConsensusNode) WaitToStop() {
	p.pl.Plog.Println("handling stop message")
	p.stopLock.Lock()
	p.stop = true
	p.stopLock.Unlock()
	if p.NodeID == p.view {
		p.pStop <- 1
	}
	networks.CloseAllConnInPool()
	p.tcpln.Close()
	p.closePbft()
	p.pl.Plog.Println("handled stop message")
}

func (p *PbftConsensusNode) getStopSignal() bool {
	p.stopLock.Lock()
	defer p.stopLock.Unlock()
	return p.stop
}

// close the pbft
func (p *PbftConsensusNode) closePbft() {
	p.CurChain.CloseBlockChain()
}
