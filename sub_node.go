package main

import (
	"bytes"
	"encoding/json"
	"crypto/sha256"
	"math/rand"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
	"os"
	"context"
)

type Message struct {
	TxID        string `json:"txid"`
	FromID      string `json:"fromid"`
	ToID        string `json:"toid"`
	SendShard   string `json:"sendshard"`
	ReceivShard string `json:"receivshard"`
	Amount      string `json:"amount"`
	Status      string `json:"status"`
	MsgFrom     string `json:"msgfrom"`
	SubmitTime  string `json:"submit_time"`
}

type SubNode struct {
	ID               string
	port             string
	mainNodeAddr     string
	shardID          int
	peerAddresses    []string
	transactionQueue []Transaction
	mutex            sync.Mutex
	prePrepareACK    map[string]map[string]bool
	prepareACK       map[string]map[string]bool
	commitACK        map[string]map[string]bool
	ledgerData       []Transaction
	crossProtocal    string
	readyChan        chan struct{}
	ledgerLoadTime   time.Duration
	retryCounts      map[string]map[string]int
  retryMutex       sync.Mutex
	txLogs           []Message
	shardsUrls       []string
}

func NewSubNode(id, port, mainAddr string, peers []string, shardID int, crossProtocal string, readyChan chan struct{}, shardsUrls []string) *SubNode {
	return &SubNode{
		ID:               id,
		port:             port,
		mainNodeAddr:     mainAddr,
		peerAddresses:    peers,
		shardID:          shardID,
		transactionQueue: make([]Transaction, 0),
		prePrepareACK:    make(map[string]map[string]bool),
		prepareACK:       make(map[string]map[string]bool),
		commitACK:        make(map[string]map[string]bool),
		ledgerData:       []Transaction{},
		crossProtocal:    crossProtocal,
		readyChan:        readyChan,
		ledgerLoadTime:   0,
		retryCounts:      make(map[string]map[string]int),
		txLogs:           make([]Message, 0),
		shardsUrls:       shardsUrls,
	}
}

func (s *SubNode) Start(ctx context.Context, txs []Transaction, startWg *sync.WaitGroup, submitTxRate int) {
	mux := http.NewServeMux()
	mux.HandleFunc("/message", s.handleMessage)
	go func() {
		log.Printf("子节点 %s 启动，监听端口 %s\n", s.ID, s.port)
		if err := http.ListenAndServe(":"+s.port, mux); err != nil {
			log.Fatalf("子节点 %s 启动失败: %v", s.ID, err)
		}
	}()

	s.loadLedgerData()
	s.loadTransactions(txs)

	time.Sleep(1000 * time.Millisecond) // 等待 HTTP 服务启动
	startWg.Done() // 通知主线程，当前子节点已启动

	<-s.readyChan // 阻塞，直到主节点启动完成

	// 主节点完成后再开始 NewTicker
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			s.processTransactions(submitTxRate)
		case <-ctx.Done():
			log.Printf("%s ctx.Done()", s.ID)
			s.saveTransactionToFile()
			s.saveRetriesToFile()
			return
		}
	}
}

func (s *SubNode) loadTransactions(txs []Transaction) {
	s.mutex.Lock()
	s.transactionQueue = txs
	s.mutex.Unlock()
	log.Printf("子节点 %s 加载了 %d 笔交易\n", s.ID, len(txs))
	time.Sleep(100 * time.Millisecond)
}

func (s *SubNode) processTransactions(submitTxRate int) {

	count := submitTxRate
	if len(s.transactionQueue) < count {
		count = len(s.transactionQueue)
	}
	if count == 0 {
		return
	}

	txs := s.transactionQueue[:count]
	s.transactionQueue = s.transactionQueue[count:]

	// log.Printf("子节点 %s 广播了 %d 笔交易\n", s.ID, len(txs))
	for _, tx := range txs {
		tx.SubmitTime = fmt.Sprintf("%d", time.Now().UnixMilli())
		msg := Message{
			TxID:        tx.ID,
			FromID:      tx.FromID,
			ToID:        tx.ToID,
			SendShard:   tx.SendShard,
			ReceivShard: tx.ReceivShard,
			Amount:      tx.Amount,
			Status:      tx.Status,
			MsgFrom:     s.ID,
			SubmitTime:  tx.SubmitTime,
		}
		// if tx.SendShard != tx.ReceivShard {
		// 	log.Printf("TX %s Send at %s\n", s.ID, tx.SubmitTime)
		// }
		
		s.broadcast(msg)
		s.txLogs = append(s.txLogs, msg)
	}
}

func (s *SubNode) saveTransactionToFile() {
	file, err := os.OpenFile(fmt.Sprintf("outputs/%d/%s_Shard%d_%s_transactions.json", len(s.shardsUrls), s.crossProtocal, s.shardID, s.ID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Printf("保存交易到文件失败: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, tx := range s.txLogs {
		if err := encoder.Encode(tx); err != nil {
			log.Printf("编码交易失败: %v", err)
		}
	}
}

func (s *SubNode) broadcast(msg Message) {
	go func() {
		if s.crossProtocal == "Presto" && msg.Status == "Prepare" && msg.SendShard == strconv.Itoa(s.shardID) && msg.SendShard != msg.ReceivShard {
			// delay := time.Duration(rand.Intn(500)) * time.Millisecond
			// time.AfterFunc(delay, func() {
			body, _ := json.Marshal(msg)
			maxRetries := 5 // 最大重试次数
			for i := 0; i < maxRetries; i++ {
				resp, err := http.Post("http://localhost:8080/prestocount", "application/json", bytes.NewBuffer(body))
				if err == nil {
					defer resp.Body.Close()
					break
				}
				s.logRetryCount(msg.TxID, "Presto")
				time.Sleep(time.Second * time.Duration(i+1))
			}
			// })
		}
	}()
	for _, addr := range s.peerAddresses {
		go func(addr string) {
			delay := time.Duration(rand.Intn(500)) * time.Millisecond
			time.AfterFunc(delay, func() {
				url := fmt.Sprintf("http://%s/message", addr)
				// 重传机制
				maxRetries := 5 // 最大重试次数
				for i := 0; i < maxRetries; i++ {
					body, _ := json.Marshal(msg)
					resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
					
					if err == nil {
						// log.Printf("子节点 %s 广播消息 %s 到 %s 状态 %s\n", s.ID, msg.TxID, addr, msg.Status)
						defer resp.Body.Close()
						return // 成功，退出重试
					}
					s.logRetryCount(msg.TxID, msg.Status)
					time.Sleep(time.Second * time.Duration(i+1))
				}
				// 重试失败
				// log.Printf("子节点 %s 广播消息到 %s 失败，已达到最大重试次数\n", s.ID, addr)
			})
		}(addr)
	}
}

func (s *SubNode) handleMessage(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		log.Printf("子节点 %s 解析消息失败: %v\n", s.ID, err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	// log.Printf("子节点 %s 接收到 S%s 消息 %s%s\n", s.ID, msg.SendShard, msg.TxID, msg.Status)
	switch msg.Status {
	case "PrePrepare":
		s.handlePrePrepare(msg)
	case "Prepare":
		s.handlePrepare(msg)
	case "Commit":
		s.handleCommit(msg)
	}
}

func (s *SubNode) logRetryCount(txID, txStatus string) {
		log.Printf("logRetryCount + 1")
    s.retryMutex.Lock()
    if s.retryCounts[txID] == nil {
        s.retryCounts[txID] = make(map[string]int)
    }
    s.retryCounts[txID][txStatus]++
    s.retryMutex.Unlock()
}

func (s *SubNode) handlePrePrepare(msg Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// log.Printf("============ 测试拥塞 %s \n", "handlePrePrepare")
	if s.prePrepareACK[msg.SendShard + msg.TxID] == nil {
		s.prePrepareACK[msg.SendShard + msg.TxID] = make(map[string]bool)
	}
	if s.prePrepareACK[msg.SendShard + msg.TxID][msg.MsgFrom] {
		return
	}
	s.prePrepareACK[msg.SendShard + msg.TxID][msg.MsgFrom] = true
	
	newMsg := Message{
		TxID:        msg.TxID,
		FromID:      msg.FromID,
		ToID:        msg.ToID,
		SendShard:   msg.SendShard,
		ReceivShard: msg.ReceivShard,
		Amount:      msg.Amount,
		Status:      "Prepare",
		MsgFrom:     s.ID,
		SubmitTime:  msg.SubmitTime,
	}
	s.broadcast(newMsg)
	s.prePrepareACK[msg.SendShard + msg.TxID] = make(map[string]bool)

}

func (s *SubNode) handlePrepare(msg Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// log.Printf("============ 测试拥塞 %s \n", "handlePrepare")
	if s.prepareACK[msg.SendShard + msg.TxID] == nil {
		s.prepareACK[msg.SendShard + msg.TxID] = make(map[string]bool)
	}
	if s.prepareACK[msg.SendShard + msg.TxID][msg.MsgFrom] {
		return
	}
	s.prepareACK[msg.SendShard + msg.TxID][msg.MsgFrom] = true

	// log.Printf("子节点 %s 接收到 S%s 消息 %s 状态 %s 第%d次\n", s.ID, msg.SendShard, msg.TxID, msg.Status, len(s.prepareACK[msg.SendShard + msg.TxID]))
	// log.Printf("selectedTXs0\n")
	if s.crossProtocal == "Monoxide" && msg.SendShard != strconv.Itoa(s.shardID) {
		s.CheckLedgerHeavy()
	}else{
		s.CheckLedger()
	}
	

	// log.Printf("prepareACK %d %d\n", len(s.commitACK[msg.SendShard + msg.TxID]), (len(s.peerAddresses)+1)/2)
	if len(s.prepareACK[msg.SendShard + msg.TxID]) > (len(s.peerAddresses)+1)/2 {
		newMsg := Message{
			TxID:        msg.TxID,
			FromID:      msg.FromID,
			ToID:        msg.ToID,
			SendShard:   msg.SendShard,
			ReceivShard: msg.ReceivShard,
			Amount:      msg.Amount,
			Status:      "Commit",
			MsgFrom:     s.ID,
			SubmitTime:  msg.SubmitTime,
		}
		s.broadcast(newMsg)
		s.prepareACK[msg.SendShard + msg.TxID] = make(map[string]bool)
	}
}

func (s *SubNode) handleCommit(msg Message) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	// log.Printf("============ 测试拥塞 %s \n", "handleCommit")
	if s.commitACK[msg.SendShard + msg.TxID] == nil {
		s.commitACK[msg.SendShard + msg.TxID] = make(map[string]bool)
	}
	if s.commitACK[msg.SendShard + msg.TxID][msg.MsgFrom] {
		return
	}
	s.commitACK[msg.SendShard + msg.TxID][msg.MsgFrom] = true

	if s.crossProtocal == "Monoxide" && msg.SendShard != strconv.Itoa(s.shardID) {
		s.CheckLedgerHeavy()
	}else{
		s.CheckLedger()
	}
	// log.Printf("commitACK %d %d\n", len(s.commitACK[msg.SendShard + msg.TxID]), (len(s.peerAddresses)+1)/2)

	if len(s.commitACK[msg.SendShard + msg.TxID]) > (len(s.peerAddresses)+1)/2 {
		newMsg := Message{
			TxID:        msg.TxID,
			FromID:      msg.FromID,
			ToID:        msg.ToID,
			SendShard:   msg.SendShard,
			ReceivShard: msg.ReceivShard,
			Amount:      msg.Amount,
			Status:      "Commit",
			MsgFrom:     s.ID,
			SubmitTime:  msg.SubmitTime,
		}
		reqBody, _ := json.Marshal(newMsg)

		// 重传机制
		maxRetries := 5 // 最大重试次数
		for i := 0; i < maxRetries; i++ {
			resp, err := http.Post(fmt.Sprintf("http://%s/confirm", s.mainNodeAddr), "application/json", bytes.NewBuffer(reqBody))
			if err == nil {
				// log.Printf("子节点 %s 提交交易 %s\n", s.ID, msg.TxID)
				s.commitACK[msg.SendShard + msg.TxID] = make(map[string]bool)
				defer resp.Body.Close()
				return // 成功，退出重试
			}
			s.logRetryCount(msg.TxID, newMsg.Status)
			// 指数退避
			time.Sleep(time.Second * time.Duration(i+1))
		}

		// 重试失败
		log.Printf("子节点 %s 提交交易 %s 失败，已达到最大重试次数\n", s.ID, msg.TxID)
	}
}

// 模拟加载账本验证过程（Monoxide中跨分片交易证明的读取与验证）
func (s *SubNode) CheckLedgerHeavy() {
	time.Sleep(s.ledgerLoadTime)

	// 提取所有交易
	var selectedTXs []Transaction = s.ledgerData
	// 随机选择 50 笔交易
	rand.Seed(time.Now().UnixNano())
	if len(selectedTXs) > 50 {
			selectedTXs = selectedTXs[:50]
	}

	// log.Printf("selectedTXs len %d\n", len(selectedTXs))
	// 生成 hash 树的根节点
	var hashes [][]byte
	for _, tx := range selectedTXs {
		txData, _ := json.Marshal(tx)
		hash := sha256.Sum256(txData)
		hashes = append(hashes, hash[:])
	}
	computeMerkleRoot(hashes)
}

// 模拟验证过程
func (s *SubNode) CheckLedger() {
	// 随机选择 10 笔交易
	rand.Seed(time.Now().UnixNano())
	selectedTXs := s.ledgerData
	if len(selectedTXs) > 10 {
		selectedTXs = selectedTXs[:10]
	}
	// log.Printf("selectedTXs len %d\n", len(selectedTXs))
	// 生成 hash 树的根节点
	var hashes [][]byte
	for _, tx := range selectedTXs {
		txData, _ := json.Marshal(tx)
		hash := sha256.Sum256(txData)
		hashes = append(hashes, hash[:])
	}
	computeMerkleRoot(hashes)
}


// 模拟查询过程
func (s *SubNode) QueryLedger() {

	// 查找第 10 笔交易
	if len(s.ledgerData) < 10 {
		return
	}
}

func computeMerkleRoot(hashes [][]byte) []byte {
	if len(hashes) == 0 {
		return nil
	}
	for len(hashes) > 1 {
		var newHashes [][]byte
		for i := 0; i < len(hashes); i += 2 {
			if i+1 < len(hashes) {
				hash := sha256.Sum256(append(hashes[i], hashes[i+1]...))
				newHashes = append(newHashes, hash[:])
			} else {
				newHashes = append(newHashes, hashes[i])
			}
		}
		hashes = newHashes
	}
	return hashes[0]
}

// 加载账本数据
func (s *SubNode) loadLedgerData() {
	startTime := time.Now() // 开始时间
	file, err := os.Open("shard_1_ledger_back.json")
	if err != nil {
		log.Printf("加载账本数据失败: %v", err)
		return
	}
	defer file.Close()

	var ledger ShardLedger
	decoder := json.NewDecoder(file)
	if err := decoder.Decode(&ledger); err != nil {
		log.Printf("解析账本数据失败: %v", err)
		return
	}

	// 提取所有交易
	for _, block := range ledger.Blocks {
		s.ledgerData = append(s.ledgerData, block.Transactions...)
	}
	endTime := time.Now() // 结束时间
  s.ledgerLoadTime = endTime.Sub(startTime) // 计算耗时

	log.Printf("解析账本数据: %d", len(s.ledgerData))
}


func (s *SubNode) saveRetriesToFile() {
		counts := s.retryCounts
		var logEntries []string
    for txID, statuses := range counts {
        for status, count := range statuses {
            logEntry := fmt.Sprintf("%s_%s_%d", txID, status, count)
            logEntries = append(logEntries, logEntry)
        }
    }
		log.Printf("Retries Count %d", len(logEntries))

    if len(logEntries) == 0 {
        return
    }



		file, err := os.Create(fmt.Sprintf("outputs/%d/retry_counts_%s_%s.log", len(s.shardsUrls), s.ID, s.crossProtocal))
		if err != nil {
        log.Printf("打开文件失败: %v", err)
        return
    }
    defer file.Close()
		log.Printf("Sub_node %s更新通信超时日志，共%d行", s.ID ,len(logEntries))
    for _, entry := range logEntries {
        _, err = file.WriteString(entry + "\n")
        if err != nil {
            log.Printf("写入文件失败: %v", err)
        }
    }
}