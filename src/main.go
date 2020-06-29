package main

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"
	"time"

	"github.com/gin-gonic/gin"
	"github.com/jmoiron/sqlx"
	_ "github.com/lib/pq"
	"github.com/sirupsen/logrus"
	"github.com/streadway/amqp"
)

// Message is a base struct for messages
type Message struct {
	ID       string `json:"id"`
	ClientID string `json:"clientId"`
	Text     string `json:"text"`
	Operator string `json:"operator"`
	Cost     float64
}

// Tariff is a base struct for tariffs
type Tariff struct {
	ID   string  `db:"id" json:"id"`
	Name string  `db:"name" json:"name"`
	Cost float64 `db:"cost" json:"cost"`
}

const oneMsgSymbolsCount float64 = 160

var db *sqlx.DB
var tariffsMap sync.Map
var handledMessages sync.Map

func main() {
	initDB()
	isTariffsLoaded := false
	for !isTariffsLoaded {
		err := loadTariffs()
		if err != nil {
			logrus.Error("err load tariffs: ", err, " try again after 20 seconds")
			time.Sleep(time.Second * 20)
		} else {
			isTariffsLoaded = true
		}
	}

	go InitMsgConsumer()
	r := gin.New()
	r.GET("/stat", func(c *gin.Context) {
		c.JSON(200, getStat())
	})
	host := ":14501"
	logrus.Info("running http server on host: ", host)
	r.Run(host)
}

func initDB() {
	var err error
	connected := false
	defaultURI := "user=postgres password=VjMaexz$rF dbname=billing sslmode=disable"
	envURI := os.Getenv("SQL_URI")
	if envURI == "" {
		envURI = defaultURI
	}
	for !connected {
		db, err = sqlx.Connect("postgres", envURI)
		if err != nil {
			logrus.Error("failed connect to database:", envURI, " ", err, " try reconnect after 20 seconds")
			time.Sleep(20 * time.Second)
			continue
		} else {
			connected = true
		}
	}
	logrus.Info("success connect to database: ", envURI)
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		delivery := d
		go handleMessage(&delivery)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}

func handleMessage(d *amqp.Delivery) {
	now := time.Now().UTC()
	msg := &Message{}
	if err := json.Unmarshal(d.Body, msg); err != nil {
		logrus.Error("err parse message: ", err)
		return
	}
	msgLen := float64(len(msg.Text))
	numMessages := math.Trunc(msgLen/oneMsgSymbolsCount) + 1
	tariff := getTariff(msg.Operator)
	if tariff == nil {
		logrus.Error("tariff not found")
		return
	}
	d.Ack(false)
	msg.Cost = tariff.Cost * numMessages
	after := time.Since(now).Seconds()
	handledMessages.Store(msg.ID, after)
	logrus.Info("Message: ", msg.ID, " operator: ", tariff.Name, " client: ", msg.ClientID, " cost: ", msg.Cost)
}

func loadTariffs() error {
	data := []Tariff{}
	err := db.Select(&data, `SELECT * FROM tariff`)
	if err != nil {
		logrus.Error("err get tariffs data: ", err)
		return err
	}
	for i := 0; i < len(data); i++ {
		tariff := data[i]
		tariffsMap.Store(tariff.ID, &tariff)
	}
	return nil
}

func getTariff(id string) *Tariff {
	tariffInt, ok := tariffsMap.Load(id)
	if !ok {
		logrus.Error("tariff not found: ", id)
		return nil
	}
	tariff, ok := tariffInt.(*Tariff)
	if !ok {
		logrus.Error("err conv tariff interface")
		return nil
	}
	return tariff
}

func getStat() map[string]interface{} {
	lenHandled := 0
	var sumTime float64
	handledMessages.Range(func(key, value interface{}) bool {
		lenHandled++
		t := value.(float64)
		sumTime += t
		return true
	})

	avgTimeOneMsg := sumTime / float64(lenHandled)
	rps := 1 / avgTimeOneMsg
	return map[string]interface{}{
		"handledMessages":     lenHandled,
		"sumTime":             sumTime,
		"avgTimeHandleOneMsg": avgTimeOneMsg,
		"rps":                 rps,
	}
}
