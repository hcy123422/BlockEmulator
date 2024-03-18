package main

import (
	"blockEmulator/build"
	"github.com/spf13/pflag"
)

var (
	shardNum    int
	nodeNum     int
	shardID     int
	nodeID      int
	modID       int
	isClient    bool
	isGen       bool
	isSiglebock bool
)

func main() {
	//test.TestBlockChain(4)
	pflag.IntVarP(&shardNum, "shardNum", "S", 2, "indicate that how many shards are deployed")
	pflag.IntVarP(&nodeNum, "nodeNum", "N", 4, "indicate how many nodes of each shard are deployed")
	pflag.IntVarP(&shardID, "shardID", "s", 0, "id of the shard to which this node belongs, for example, 0")
	pflag.IntVarP(&nodeID, "nodeID", "n", 0, "id of this node, for example, 0")
	pflag.IntVarP(&modID, "modID", "m", 3, "choice Committee Method,for example, 0, [CLPA_Broker,CLPA,Broker,Relay] ")
	pflag.BoolVarP(&isClient, "client", "c", false, "whether this node is a client")
	pflag.BoolVarP(&isGen, "gen", "g", false, "generation bat")
	pflag.BoolVarP(&isSiglebock, "Sigleblock", "b", false, "generation bat")
	pflag.Parse()

	if isGen {
		build.GenerateBatFile(nodeNum, shardNum, modID)
		build.GenerateShellFile(nodeNum, shardNum, modID)
		return
	}
	if isSiglebock {
		build.BuildNewNode(uint64(nodeID), uint64(nodeNum), uint64(shardID), uint64(shardNum), uint64(modID))
		return
	}

	if isClient {
		build.BuildSupervisor(uint64(nodeNum), uint64(shardNum), uint64(modID))
	} else {
		build.BuildNewPbftNode(uint64(nodeID), uint64(nodeNum), uint64(shardID), uint64(shardNum), uint64(modID))
	}
}
