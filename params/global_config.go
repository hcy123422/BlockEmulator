package params

var (
	Block_Interval      = 5000   // generate new block interval
	MaxBlockSize_global = 2000   // the block contains the maximum number of transactions
	InjectSpeed         = 2000   // the transaction inject speed
	TotalDataSize       = 100000 // the total number of txs  tx的总数
	BatchSize           = 16000  // supervisor read a batch of txs then send them, it should be larger than inject speed
	BrokerNum           = 10
	NodesInShard        = 4
	ShardNum            = 4
	DataWrite_path      = "./result/"                                              // 测量数据结果输出路径
	LogWrite_path       = "./log"                                                  // log output path
	SupervisorAddr      = "127.0.0.1:18800"                                        //supervisor ip address
	FileInput           = `D:\blockEmulator\2000000to2999999_BlockTransaction.csv` //the raw BlockTransaction data path
	FileInput2          = `D:\blockEmulator\select_abcd.csv`
)
