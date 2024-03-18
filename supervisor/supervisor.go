// Supervisor is an abstract role in this simulator that may read txs, generate partition infos,
// and handle history data.

package supervisor

import (
	"blockEmulator/message"
	"blockEmulator/networks"
	"blockEmulator/params"
	"blockEmulator/supervisor/committee"
	"blockEmulator/supervisor/measure"
	"blockEmulator/supervisor/signal"
	"blockEmulator/supervisor/supervisor_log"
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
)

type Supervisor struct {
	// basic infos
	IPaddr       string // ip address of this Supervisor
	ChainConfig  *params.ChainConfig
	Ip_nodeTable map[uint64]map[uint64]string

	// tcp controll
	listenStop bool
	tcpLn      net.Listener
	tcpLock    sync.Mutex
	// logger module
	sl *supervisor_log.SupervisorLog

	// control components
	Ss *signal.StopSignal // to control the stop message sending

	// supervisor and committee components
	//监事和委员会组成部分
	ComMod committee.CommitteeModule

	// measure components
	//测量组件
	testMeasureMods []measure.MeasureModule

	// diy, add more structures or classes here ...
	//在此处添加更多结构或类
}

func (d *Supervisor) NewSupervisor(ip string, pcc *params.ChainConfig, committeeMethod string, mearsureModNames ...string) {
	d.IPaddr = ip
	d.ChainConfig = pcc
	d.Ip_nodeTable = params.IPmap_nodeTable

	d.sl = supervisor_log.NewSupervisorLog()

	d.Ss = signal.NewStopSignal(2 * int(pcc.ShardNums))

	switch committeeMethod {
	case "CLPA_Broker":
		d.ComMod = committee.NewCLPACommitteeMod_Broker(d.Ip_nodeTable, d.Ss, d.sl, params.FileInput, params.TotalDataSize, params.BatchSize, 80)
	case "CLPA":
		d.ComMod = committee.NewCLPACommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.FileInput, params.TotalDataSize, params.BatchSize, 80)
	case "Broker":
		d.ComMod = committee.NewBrokerCommitteeMod(d.Ip_nodeTable, d.Ss, d.sl, params.FileInput, params.TotalDataSize, params.BatchSize)
	case "Tree":
		d.ComMod = committee.NewTree_CommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.FileInput2, params.TotalDataSize, params.BatchSize)
	case "Single":
		d.ComMod = committee.NewOnly_CommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.FileInput2, params.TotalDataSize, params.BatchSize)
	default:
		d.ComMod = committee.NewRelayCommitteeModule(d.Ip_nodeTable, d.Ss, d.sl, params.FileInput, params.TotalDataSize, params.BatchSize)
	}

	d.testMeasureMods = make([]measure.MeasureModule, 0)
	for _, mModName := range mearsureModNames {
		switch mModName {
		case "TPS_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Relay())
		case "TPS_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_avgTPS_Broker())
		case "TCL_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Relay())
		case "TCL_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestModule_TCL_Broker())
		case "CrossTxRate_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Relay())
		case "CrossTxRate_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestCrossTxRate_Broker())
		case "TxNumberCount_Relay":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Relay())
		case "TxNumberCount_Broker":
			d.testMeasureMods = append(d.testMeasureMods, measure.NewTestTxNumCount_Broker())
		default:
		}
	}
}

// Supervisor received the block informatino from the leaders, and handle these
// message to measure the performances.消息来衡量性能。
func (d *Supervisor) handleBlockInfos(content []byte) {
	log.Println("SupervisorhandleBlockInfos")
	bim := new(message.BlockInfoMsg)
	err := json.Unmarshal(content, bim)
	if err != nil {
		log.Panic()
	}
	// StopSignal check
	if bim.BlockBodyLength == 0 {
		d.Ss.StopGap_Inc()
	} else {
		d.Ss.StopGap_Reset()
	}

	d.ComMod.HandleBlockInfo(bim)

	// measure update
	for _, measureMod := range d.testMeasureMods {
		measureMod.UpdateMeasureRecord(bim)
	}
	// add codes here ...
}

// read transactions from dataFile. When the number of data is enough,
// the Supervisor will do re-partition and send partitionMSG and txs to leaders.
func (d *Supervisor) SupervisorTxHandling() {
	d.ComMod.MsgSendingControl()
	log.Println("uuuuuuuuuuuuu")
	// TxHandling is end
	for !d.Ss.GapEnough() { //
		time.Sleep(time.Second)
	}
	// send stop message
	log.Println("stoppppp")
	stopmsg := message.MergeMessage(message.CStop, []byte("this is a stop message~"))
	d.sl.Slog.Println("Supervisor: now sending cstop message to all nodes")
	for sid := uint64(0); sid < d.ChainConfig.ShardNums; sid++ {
		for nid := uint64(0); nid < d.ChainConfig.Nodes_perShard; nid++ {
			networks.TcpDial(stopmsg, d.Ip_nodeTable[sid][nid])
		}
	}
	d.sl.Slog.Println("Supervisor: now Closing")
	d.listenStop = true
	d.CloseSupervisor()
}

// handle message. only one message to be handled now
func (d *Supervisor) handleMessage(msg []byte) {
	msgType, content := message.SplitMessage(msg)
	switch msgType {
	case message.CBlockInfo:
		d.handleBlockInfos(content)
		// add codes for more functionality
	default:
		d.ComMod.HandleOtherMessage(msg)
		for _, mm := range d.testMeasureMods {
			mm.HandleExtraMessage(msg)
		}
	}
}

func (d *Supervisor) handleClientRequest(con net.Conn) {
	defer con.Close()
	clientReader := bufio.NewReader(con)
	for {
		clientRequest, err := clientReader.ReadBytes('\n')
		switch err {
		case nil:
			d.tcpLock.Lock()
			d.handleMessage(clientRequest)
			d.tcpLock.Unlock()
		case io.EOF:
			log.Println("client closed the connection by terminating the process")
			return
		default:
			log.Printf("error: %v\n", err)
			return
		}
	}
}

func (d *Supervisor) TcpListen() {
	ln, err := net.Listen("tcp", d.IPaddr)
	if err != nil {
		log.Println("444444444444")
		log.Panic(err)
	}
	d.tcpLn = ln
	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			return
		}
		go d.handleClientRequest(conn)
	}
}

// tcp listen for Supervisor
func (d *Supervisor) OldTcpListen() {
	ipaddr, err := net.ResolveTCPAddr("tcp", d.IPaddr)
	if err != nil {
		log.Panic(err)
	}
	ln, err := net.ListenTCP("tcp", ipaddr)
	d.tcpLn = ln
	if err != nil {
		log.Panic(err)
	}
	d.sl.Slog.Printf("Supervisor begins listening：%s\n", d.IPaddr)

	for {
		conn, err := d.tcpLn.Accept()
		if err != nil {
			if d.listenStop {
				return
			}
			log.Panic(err)
		}
		b, err := io.ReadAll(conn)
		if err != nil {
			log.Panic(err)
		}
		d.handleMessage(b)
		conn.(*net.TCPConn).SetLinger(0)
		defer conn.Close()
	}
}

// close Supervisor, and record the data in .csv file
func (d *Supervisor) CloseSupervisor() {
	d.sl.Slog.Println("Closing...")
	for _, measureMod := range d.testMeasureMods {
		d.sl.Slog.Println(measureMod.OutputMetricName())
		d.sl.Slog.Println(measureMod.OutputRecord())
		println()
	}

	d.sl.Slog.Println("Trying to input .csv")
	// write to .csv file
	dirpath := params.DataWrite_path + "supervisor_measureOutput/"
	err := os.MkdirAll(dirpath, os.ModePerm)
	if err != nil {
		log.Panic(err)
	}
	for _, measureMod := range d.testMeasureMods {
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
		d.sl.Slog.Println(measureMod.OutputRecord())
	}
	networks.CloseAllConnInPool()
	d.tcpLn.Close()
}
