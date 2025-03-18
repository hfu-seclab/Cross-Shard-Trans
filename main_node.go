package main

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	// "io/ioutil"
	"fmt"
	"log"
	"net/http"
	"strconv"
	"sync"
	"time"
	"math/rand"
	"os"
	"context"
)

type MainNode struct {
	asyncFlag     bool
	confirmedPool map[string]Transaction
	confirmCounts map[string]map[string]bool
	PrestoCounts  map[string]map[string]bool
	mutex         sync.Mutex
	n             int // 子节点总数
	shardsUrls    []string
	shardID       int
	peerAddresses []string
	ledger        ShardLedger
	ledgerData    []Transaction // 新增字段，用于存储加载的账本数据
	crossProtocal string
	readyChan     chan struct{}
	retryCounts   map[string]map[string]int
  retryMutex    sync.Mutex
}

func NewMainNode(n int, shardsUrls []string, shardID int, peerAddresses []string, crossProtocal string, readyChan chan struct{}) *MainNode {
	return &MainNode{
		asyncFlag:     false,
		confirmedPool: make(map[string]Transaction),
		confirmCounts: make(map[string]map[string]bool),
		PrestoCounts:  make(map[string]map[string]bool),
		n:             n,
		shardsUrls:    shardsUrls,
		shardID:       shardID,
		peerAddresses: peerAddresses,
		ledger:        ShardLedger{ShardID: shardID},
		ledgerData:    []Transaction{},// 初始化 ledgerData
		crossProtocal: crossProtocal,
		readyChan:     readyChan,
		retryCounts:   make(map[string]map[string]int),
	}
}

func (m *MainNode) Start(ctx context.Context, duration int) {
	// 加载账本数据
	m.loadLedgerData()

	http.HandleFunc("/confirm", m.handleConfirm)
	http.HandleFunc("/handlectx", m.handleCrossTrans)
	http.HandleFunc("/oncheck", m.CheckLedger)
	http.HandleFunc("/onquery", m.QueryLedger)
	http.HandleFunc("/prestocount", m.PrestoCount)
	http.HandleFunc("/ready", func(w http.ResponseWriter, r *http.Request) {
		if m.asyncFlag {
			w.WriteHeader(http.StatusOK)
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	})
	go func() {
		if err := http.ListenAndServe(fmt.Sprintf(":%d", 8080), nil); err != nil {
			log.Fatalf("主节点启动失败: %v", err)
		}
	}()
	if m.shardID == 0 {
			time.Sleep(3 * time.Second)
			m.asyncFlag = true
	}
	<-m.readyChan
	
	go func() {
		tempPool := []Transaction{}
		// 定时打包区块
		ticker := time.NewTicker(time.Duration(duration) * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				tempConfirmedPool := m.confirmedPool
				log.Printf("待上链交易池高度 %d\n", len(tempConfirmedPool))
				m.confirmedPool = make(map[string]Transaction)
				if len(tempPool) > 0 {
					for _, _tx := range tempPool {
						tempConfirmedPool[_tx.SendShard + _tx.ID] = _tx
					}
					tempPool = []Transaction{}
				}
				m.mutex.Lock()
				if len(tempConfirmedPool) > 0 {
					log.Printf("[主节点] 打包区块，包含 %d 笔交易\n", len(tempConfirmedPool))
					block := Block{
						BlockID:    "",
						Timestamp:  time.Now(),
						Transactions: make([]Transaction, 0),
					}
					for _, tx := range tempConfirmedPool {
						if m.crossProtocal == "Presto" {
							if tx.SendShard != strconv.Itoa(m.shardID) && tx.SendShard != tx.ReceivShard {
								// log.Println("Presto_local %s", tx.ID)
								// 验证交易是否存在，不存在则移出成块list
								shardIndex, err := strconv.ParseInt(tx.SendShard, 10, 0)
								if err != nil {
									fmt.Println("Error converting string to int:", err)
								}
								url := fmt.Sprintf("http://%s:8080/onquery", m.shardsUrls[shardIndex])
								reqBody, _ := json.Marshal(map[string]string{"txid": tx.ID})
								maxRetries := 5
								success := false
								for i := 0; i < maxRetries; i++ {
									resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBody))
									if err == nil {
										defer resp.Body.Close()
										if resp.StatusCode == http.StatusOK {
											// body, _ := ioutil.ReadAll(resp.Body)
											// log.Printf(string(body))
											success = true
											break
										}
									}
									m.logRetryCount(tx.ID, "PrestoCheck")
									time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
								}
								if success {
									// log.Println("+++Presto提交接收分片交易%s", tx.ID)
									block.Transactions = append(block.Transactions, tx)
								} else {
									log.Println("Presto接收分片交易%s需Reload", tx.ID)
									tempPool = append(tempPool, tx)
								}
							} else {
								// log.Println("Presto_cross %s", tx.ID)
								block.Transactions = append(block.Transactions, tx)
							}
						} else {
							// 此处提交PledgeACS等方案跨分片TX_B
							if tx.ReceivShard != strconv.Itoa(m.shardID) {
								// log.Printf("[主节点] 跨分片向 %s 提交 1 笔交易 %s\n", tx.ReceivShard, tx.ID)
								shardIndex, err := strconv.Atoi(tx.ReceivShard)
								if err != nil {
									return
								}
								go func() {
									url := fmt.Sprintf("%s:%d", m.shardsUrls[shardIndex], 8080)
									block.Transactions = append(block.Transactions, tx)
									m.submitCrossTrans(url, tx)
								}()
							} else {
								block.Transactions = append(block.Transactions, tx)
							}
						}
				
						
					}
					
					block.BlockID = fmt.Sprintf("%d-%d", m.shardID, time.Now().UnixMilli())
					m.ledger.Blocks = append(m.ledger.Blocks, block)
					
					// m.saveLedgerToFile()
				}
				m.mutex.Unlock()
			case <-ctx.Done():
				log.Println("Main ctx.Done()")
				m.saveLedgerToFile()
				m.saveRetriesToFile()
				return
			}
		}

	}()
}

// 加载账本数据
func (m *MainNode) loadLedgerData() {
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
		m.ledgerData = append(m.ledgerData, block.Transactions...)
	}
}

func (m *MainNode) saveLedgerToFile() {
	file, err := os.Create(fmt.Sprintf("outputs/%d/%s_shard_%d_ledger.json", len(m.shardsUrls), m.crossProtocal, m.shardID))
	if err != nil {
		log.Printf("保存账本到文件失败: %v", err)
		return
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	if err := encoder.Encode(m.ledger); err != nil {
		log.Printf("编码账本失败: %v", err)
	}
}

func (m *MainNode) handleCrossTrans(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	
	// 向主分片查询TX_A是否存在（替代查询质押事务是否在主链上）
	if m.crossProtocal == "PledgeACS" {
	  shardIndex, err := strconv.ParseInt(msg.SendShard, 10, 0)
		if err != nil {
			fmt.Println("Error converting string to int:", err)
		}
		url := fmt.Sprintf("http://%s:8080/onquery", m.shardsUrls[shardIndex])
		reqBody, _ := json.Marshal(map[string]string{"txid": msg.TxID})
		maxRetries := 5
		for i := 0; i < maxRetries; i++ {
			resp, err := http.Post(url, "application/json", bytes.NewBuffer(reqBody))
			if err == nil {
				defer resp.Body.Close()
				if resp.StatusCode == http.StatusOK {
					// body, _ := ioutil.ReadAll(resp.Body)
					// log.Printf(string(body))
					// success = true
          break
				}
			}
			m.logRetryCount(msg.TxID, "handleCrossTrans")
			time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
		}
	}
	// 开始提交TX_B
	for _, addr := range m.peerAddresses {
		go func(addr string) {
			url := fmt.Sprintf("http://%s/message", addr)
			body, _ := json.Marshal(msg)

			maxRetries := 5
			for i := 0; i < maxRetries; i++ {
				resp, err := http.Post(url, "application/json", bytes.NewBuffer(body))
				if err == nil {
					defer resp.Body.Close()
					return
				}
				m.logRetryCount(msg.TxID, msg.Status)
				time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
			}
		}(addr)
	}
}

func (m *MainNode) submitCrossTrans(url string, tx Transaction) {
	m.mutex.Lock()
	defer m.mutex.Unlock()
	newMsg := Message{
		TxID:        tx.ID,
		FromID:      tx.FromID,
		ToID:        tx.ToID,
		SendShard:   tx.SendShard,
		ReceivShard: tx.ReceivShard,
		Amount:      tx.Amount,
		Status:      "PrePrepare",
		MsgFrom:     "",
		SubmitTime:  tx.SubmitTime,
	}
	reqBody, _ := json.Marshal(newMsg)

	maxRetries := 5 // 最大重试次数
	for i := 0; i < maxRetries; i++ {
		resp, err := http.Post(fmt.Sprintf("http://%s/handlectx", url), "application/json", bytes.NewBuffer(reqBody))
		if err == nil {
			defer resp.Body.Close()
			return // 成功，退出重试
		}
		m.logRetryCount(newMsg.TxID, "submitCrossTrans")
		time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
	}
}

func (m *MainNode) handleConfirm(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		log.Printf("解析消息失败: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.confirmCounts[msg.SendShard+msg.TxID] == nil {
		m.confirmCounts[msg.SendShard+msg.TxID] = make(map[string]bool)
	}

	if m.confirmCounts[msg.SendShard+msg.TxID][msg.MsgFrom] {
		return
	}
	m.confirmCounts[msg.SendShard+msg.TxID][msg.MsgFrom] = true
	if len(m.confirmCounts[msg.SendShard+msg.TxID]) > (m.n+1)/2 {
		conTX := Transaction{
			ID:          msg.TxID,
			FromID:      msg.FromID,
			ToID:        msg.ToID,
			SendShard:   msg.SendShard,
			ReceivShard: msg.ReceivShard,
			Amount:      msg.Amount,
			SubmitTime:  msg.SubmitTime,
			Status:      "Consensused",
			ConsensTime: fmt.Sprintf("%d", time.Now().UnixMilli()),
		}
		m.confirmedPool[msg.SendShard+msg.TxID] = conTX
		// log.Printf("消息 %s 添加到待选区块\n", msg.TxID)
		delete(m.confirmCounts, msg.SendShard+msg.TxID)
	}
	w.WriteHeader(http.StatusOK)
}

// 模拟验证过程
func (m *MainNode) CheckLedger(w http.ResponseWriter, r *http.Request) {

	// 随机选择 10 笔交易
	rand.Seed(time.Now().UnixNano())
	selectedTXs := m.ledgerData
	if len(selectedTXs) > 10 {
		selectedTXs = selectedTXs[:10]
	}

	// 生成 hash 树的根节点
	var hashes [][]byte
	for _, tx := range selectedTXs {
		txData, _ := json.Marshal(tx)
		hash := sha256.Sum256(txData)
		hashes = append(hashes, hash[:])
	}

	rootHash := computeMerkleRoot(hashes)
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fmt.Sprintf("Merkle Root: %x", rootHash)))
}


// 模拟查询过程
func (m *MainNode) QueryLedger(w http.ResponseWriter, r *http.Request) {
	var reqData map[string]string
	err := json.NewDecoder(r.Body).Decode(&reqData)
	if err != nil {
		http.Error(w, "Invalid request body", http.StatusBadRequest)
		return
	}

	txID, exists := reqData["txid"]
	if !exists || txID == "" {
		http.Error(w, "Missing txid parameter", http.StatusBadRequest)
		return
	}

	for _, block := range m.ledger.Blocks {
		for _, tx := range block.Transactions {
			if tx.ID == txID {
				w.WriteHeader(http.StatusOK)
				w.Write([]byte("Transaction is on-chain"))
				return
			}
		}
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte("Transaction is not on-chain"))
	// m.mutex.Lock()
	// defer m.mutex.Unlock()
	// // 查找第 10 笔交易
	// if len(m.ledgerData) < 10 {
	// 	http.Error(w, "Transaction not found", http.StatusNotFound)
	// 	return
	// }
	// tx := m.ledgerData[9]

	// w.WriteHeader(http.StatusOK)
	// w.Write([]byte(fmt.Sprintf("Transaction Time: %s", tx.SubmitTime)))
}


// Presto - 查询指定交易是否已经在 m.ledger.Blocks 中



// Presto - 计数，超过一半通知接收分片
func (m *MainNode) PrestoCount(w http.ResponseWriter, r *http.Request) {
	var msg Message
	if err := json.NewDecoder(r.Body).Decode(&msg); err != nil {
		log.Printf("解析消息失败: %v", err)
		http.Error(w, "Bad Request", http.StatusBadRequest)
		return
	}
	m.mutex.Lock()
	defer m.mutex.Unlock()

	if m.PrestoCounts[msg.TxID] == nil {
		m.PrestoCounts[msg.TxID] = make(map[string]bool)
	}

	if m.PrestoCounts[msg.TxID][msg.MsgFrom] {
		return
	}
	m.PrestoCounts[msg.TxID][msg.MsgFrom] = true

	if len(m.PrestoCounts[msg.TxID]) > (m.n+1)/2 {
		delete(m.PrestoCounts, msg.TxID)
		go func () {
			newMsg := Message{
				TxID:        msg.TxID,
				FromID:      msg.FromID,
				ToID:        msg.ToID,
				SendShard:   msg.SendShard,
				ReceivShard: msg.ReceivShard,
				Amount:      msg.Amount,
				Status:      "PrePrepare",
				MsgFrom:     "",
				SubmitTime:  msg.SubmitTime,
			}
			reqBody, _ := json.Marshal(newMsg)

			shardIndex, err := strconv.ParseInt(msg.ReceivShard, 10, 0)
			if err != nil {
				fmt.Println("Error converting string to int:", err)
			}
			url := fmt.Sprintf("%s:8080", m.shardsUrls[shardIndex])
			maxRetries := 5 // 最大重试次数
			for i := 0; i < maxRetries; i++ {
				resp, err := http.Post(fmt.Sprintf("http://%s/handlectx", url), "application/json", bytes.NewBuffer(reqBody))
				if err == nil {
					// log.Printf("Presto发送%s到%s", newMsg.TxID ,newMsg.ReceivShard)
					defer resp.Body.Close()
					return // 成功，退出重试
				}
				m.logRetryCount(newMsg.TxID, "submitCrossTrans")
				time.Sleep(time.Millisecond * 100 * time.Duration(i+1))
			}
		}()
	}
	w.WriteHeader(http.StatusOK)
}

func (m *MainNode) logRetryCount(txID, txStatus string) {
    m.retryMutex.Lock()
    if m.retryCounts[txID] == nil {
        m.retryCounts[txID] = make(map[string]int)
    }
    m.retryCounts[txID][txStatus]++
    m.retryMutex.Unlock()
}

func (m *MainNode) saveRetriesToFile() {
		counts := m.retryCounts
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

		file, err := os.Create(fmt.Sprintf("outputs/%d/retry_counts_main_%s.log", len(m.shardsUrls), m.crossProtocal))
		if err != nil {
        log.Printf("打开文件失败: %v", err)
        return
    }
    defer file.Close()
		log.Printf("Main_node更新通信超时日志，共%d行", len(logEntries))
    for _, entry := range logEntries {
        _, err = file.WriteString(entry + "\n")
        if err != nil {
            log.Printf("写入文件失败: %v", err)
        }
    }
}