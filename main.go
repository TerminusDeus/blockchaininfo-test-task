package main

import (
	"fmt"
	"log"
	"encoding/json"
	"net/http"
	"io/ioutil"
	"strconv"
	"sync"

	"blockchaininfo-test-task/config"
	"blockchaininfo-test-task/db"
	"github.com/alfg/blockchain"
	"github.com/gin-gonic/gin"
	"github.com/coreos/bbolt"
	"blockchaininfo-test-task/tools"
)

type GetTransactionsReq struct {
	Address string `json:"address" form:"address"`
}

type GetTransactionsResp struct {
	Transactions []Transaction `json:"transactions"`
}

type Transaction struct {
	Raw   string `json:"raw"`
	Block *Block `json:"block"`
}

type Block struct {
	Hash   string `json:"hash"`
	Height int    `json:"height"`
	Time   int    `json:"time"`
}

var (
	CashDB *bolt.DB
	Config config.Config
)

func main() {
	Config = config.InitConfig()
	CashDB = db.InitDB(Config.BoltDBName)
	defer CashDB.Close()
	// this creates bucket entity (if bucket not exists) for boltdb - file for our data:
	if err := CashDB.Update(func(tx *bolt.Tx) error {
		// bucket name can be customized in conf.json
		_, err := tx.CreateBucketIfNotExists([]byte(Config.BucketName))
		if err != nil {
			return fmt.Errorf("create %s: %s", Config.BucketName, err.Error())
		}
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	router := gin.Default()
	router.GET("gettransactions", GetTransactions)
	// port can be customized in conf.json
	router.Run(":" + Config.Port)
}

func GetTransactions(context *gin.Context) {
	var (
		getTransactionsReq                                  GetTransactionsReq
		oldTransactions, newTransactions, totalTransactions []Transaction
		address, rawTx                                      string
		b                                                   []byte
		block                                               *Block
		wg                                                  sync.WaitGroup
		mu                                                  sync.RWMutex
	)
	blocksHeightsMap := map[int]bool{}
	transactionsHashMap := map[string]string{}

	if tools.Check("Invalid resp data", context.Bind(&getTransactionsReq)) {
		address = getTransactionsReq.Address
	}

	c, err := blockchain.New()
	tools.Check("Error while create new blockchain.info client", err)

	getAddressResp, err := c.GetAddress(address)
	tools.Check("Error while get address for blockchain.info client", err)

	for i := range getAddressResp.Txs {
		var exist bool
		CashDB.View(func(tx *bolt.Tx) error {
			b := tx.Bucket([]byte(Config.BucketName))
			v := b.Get([]byte(getAddressResp.Txs[i].Hash))
			if v != nil {
				exist = true
				transaction := Transaction{}
				tools.Check("Error while unmarshal transaction data", json.Unmarshal(v, &transaction))
				oldTransactions = append(oldTransactions, transaction)
			}
			return nil
		})
		if exist {
			continue
		}

		blocksHeightsMap[getAddressResp.Txs[i].BlockHeight] = true

		getRawTxHexResp, err := http.Get("http://blockchain.info/rawtx/" + getAddressResp.Txs[i].Hash + "?format=hex")
		tools.Check("Error while get rawtx in hex response", err)
		defer getRawTxHexResp.Body.Close()

		b, err = ioutil.ReadAll(getRawTxHexResp.Body)
		tools.Check("Error while read get rawtx in hex response body", err)
		rawTx = string(b)

		if getAddressResp.Txs[i].BlockHeight == 0 {
			block = nil
		} else {
			block = &Block{Time: getAddressResp.Txs[i].Time, Height: getAddressResp.Txs[i].BlockHeight}
		}

		transaction := Transaction{Raw: rawTx, Block: block}
		newTransactions = append(newTransactions, transaction)
		transactionsHashMap[rawTx] = getAddressResp.Txs[i].Hash
	}

	if err := CashDB.Update(func(tx *bolt.Tx) error {
		b := tx.Bucket([]byte(Config.BucketName))
		// only for new transaction
		wg.Add(len(blocksHeightsMap))
		var err error
		for blockHeight := range blocksHeightsMap {
			go func(blockHeight int, err error) {
				blocks, _ := c.GetBlockHeight(strconv.Itoa(blockHeight))
				for _, block := range blocks.Blocks {
					for i, transaction := range newTransactions {
						if block.Height == transaction.Block.Height {
							transaction.Block.Hash = block.Hash
							newTransactions[i] = transaction
							transactionByte, err := json.Marshal(newTransactions[i])
							if err != nil {
								return
							}
							mu.Lock()
							err = b.Put([]byte(transactionsHashMap[newTransactions[i].Raw]), transactionByte)
							mu.Unlock()
							if err != nil {
								return
							}
						}
					}
				}
				wg.Done()
			}(blockHeight, err)
		}
		wg.Wait()
		return nil
	}); err != nil {
		log.Fatal(err)
	}

	totalTransactions = append(oldTransactions, newTransactions...)
	GetTransactionsResp := GetTransactionsResp{Transactions: totalTransactions}
	context.JSON(200, GetTransactionsResp)
}
