package main

import (
	"fmt"
	"flag"
	"log"
	"strconv"
	"sync"
	"time"
	"net/http"
	"io/ioutil"
	"os"
	"encoding/json"
	"syscall"
	"os/signal"
	"context"
)

var shardID int
var crossProtocal string
var nodeNumPerShard int // 子节点数量
var submitTxRate int     // 每秒生成的交易数量 submitTxRate * nodeNumPerShard
var shardsUrls = []string{"172.17.0.2", "172.17.0.3"}
// var shardsUrls = []string{"172.17.0.2", "172.17.0.3", "172.17.0.4", "172.17.0.5", "172.17.0.6"}
// , "192.168.1.112", "192.168.1.104"
const (
	slavePortBase    = 8081 // 子节点起始端口
	duration         = 5    // 出块周期
)

type Transaction struct {
	ID          string    `json:"id"`
	FromID      string    `json:"fromid"`
	ToID        string    `json:"toid"`
	SendShard   string    `json:"sendshard"`
	ReceivShard string    `json:"receivshard"`
	Amount      string    `json:"amount"`
	Status      string    `json:"status"`
	SubmitTime  string    `json:"submit_time"`
	ConsensTime string    `json:"consens_time"`
}

type ShardLedger struct {
	ShardID int
	Blocks  []Block
}

type Block struct {
	BlockID    string
	Timestamp  time.Time
	Transactions []Transaction
}

// loadTransactionsFromFile loads transactions from a JSON file
func loadTransactionsFromFile(filename string) []Transaction {
	// Open the JSON file
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("Failed to open file: %s", err)
	}
	defer file.Close()

	// Read the file content
	bytes, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("Failed to read file: %s", err)
	}

	// Parse the JSON data
	var transactions []Transaction
	err = json.Unmarshal(bytes, &transactions)
	if err != nil {
		log.Fatalf("Failed to parse JSON: %s", err)
	}

	return transactions
}

func filterTransactionsBySendShard(transactions []Transaction, shard string) []Transaction {
	var filtered []Transaction
	crossCount := 0
	for _, transaction := range transactions {
		if transaction.SendShard == shard && transaction.Amount != "0.0" {
			filtered = append(filtered, transaction)
			if transaction.ReceivShard != shard {
				crossCount += 1
			}
		}
	}
	percentage := float64(crossCount) / float64(len(transactions)) * 100
	log.Printf("当前分片跨分片交易占比: %.2f%%\n", percentage)
	return filtered
}


func main() {
	ctx, cancel := context.WithCancel(context.Background()) // 创建上下文和取消函数
	defer cancel()
	// 设置信号监听
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	flag.IntVar(&shardID, "shard", 1, "The shard number")
	flag.IntVar(&submitTxRate, "rate", 5, "The rate of submit TX")
	flag.IntVar(&nodeNumPerShard, "node", 5, "The SubNode number")
	flag.StringVar(&crossProtocal, "crp", "BrokerChain", "The Cross Chain Protocal") 
	// BrokerChain Monoxide PledgeACS Presto
	flag.Parse()
	log.Printf("Current Shard ID: %d\n", shardID)

	transactions := loadTransactionsFromFile(fmt.Sprintf("transactions%d.json", len(shardsUrls)))
	
	filteredTransactions := filterTransactionsBySendShard(transactions, strconv.Itoa(shardID))

	subNodes := make([]*SubNode, nodeNumPerShard)
	peerAddresses := make([]string, nodeNumPerShard)
	for i := 0; i < nodeNumPerShard; i++ {
		peerAddresses[i] = fmt.Sprintf("localhost:%d", slavePortBase+i)
	}

	// 创建主节点（此时不启动）
	mainNode := NewMainNode(nodeNumPerShard, shardsUrls, shardID, peerAddresses, crossProtocal, make(chan struct{}))

	// 启动子节点
	var wg sync.WaitGroup
	var startWg sync.WaitGroup // 用于等待所有子节点启动

	for i := 0; i < nodeNumPerShard; i++ {
		peers := make([]string, 0)
		for j := 0; j < nodeNumPerShard; j++ {
			if i != j {
				peers = append(peers, fmt.Sprintf("localhost:%d", slavePortBase+j))
			}
		}
		subNode := NewSubNode(
			fmt.Sprintf("Node%d", i+1),
			fmt.Sprintf("%d", slavePortBase+i),
			"localhost:8080",
			peers,
			shardID,
			crossProtocal,
			make(chan struct{}),
			shardsUrls,
		)
		subNodes[i] = subNode
		wg.Add(1)
		startWg.Add(1)
		go func(i int) {
			defer wg.Done()
			subNode.Start(ctx, filteredTransactions[i*len(filteredTransactions)/nodeNumPerShard:(i+1)*len((filteredTransactions))/nodeNumPerShard], &startWg, submitTxRate)
		}(i)
	}

	startWg.Wait() // 等待所有子节点启动完成
	log.Println("所有子节点已启动，开始处理交易")

	// 在所有子节点启动后启动主节点
	go mainNode.Start(ctx, duration)

	for {
    resp, err := http.Get(fmt.Sprintf("http://%s:8080/ready", shardsUrls[0]))
    if err == nil && resp.StatusCode == http.StatusOK {
        resp.Body.Close()
        break
    }
    time.Sleep(100 * time.Millisecond)
	}
	// 通知所有子节点主节点已就绪
	mainNode.readyChan <- struct{}{}
	for _, subNode := range subNodes {
		subNode.readyChan <- struct{}{}
	}

	// 监听信号
	go func() {
		sig := <-sigs
		log.Printf("Received signal: %s, terminating...", sig)
		cancel() // 触发所有节点的取消信号
		wg.Wait() // 等待所有子节点退出
		// mainNode.saveLedgerToFile()
		// mainNode.saveRetriesToFile()
		// for _, subNode := range subNodes {
		// 	subNode.saveTransactionToFile()
		// 	subNode.saveRetriesToFile()
		// }
		// 在这里可以添加任何清理操作，例如关闭连接、释放资源等
		os.Exit(0)
	}()

	wg.Wait() // 等待所有子节点完成
}