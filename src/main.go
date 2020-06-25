package main

import (
	"encoding/json"
	"log"
	"math"
	"os"
	"sync"
	"time"

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

func main() {
	initDB()
	loadTariffs()
	go InitMsgConsumer()
	for {

	}
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
	logrus.Info("Message: ", msg.ID, " operator: ", tariff.Name, " client: ", msg.ClientID, " cost: ", msg.Cost)
}

func loadTariffs() {
	data := []Tariff{}
	err := db.Select(&data, `SELECT * FROM tariff`)
	if err != nil {
		logrus.Error("err get tariffs data: ", err)
		return
	}
	for i := 0; i < len(data); i++ {
		tariff := data[i]
		tariffsMap.Store(tariff.ID, &tariff)
	}
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
