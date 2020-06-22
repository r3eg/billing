package main

import (
	"log"

	"github.com/streadway/amqp"
)

// Message is a base struct for messages
type Message struct {
	ID       string `json:"id"`
	ClientID string `json:"clientId"`
	Text     string `json:"text"`
	Operator string `json:"operator"`
}

var count = 0

func main() {
	go InitMsgConsumer()
	// gin.SetMode("release")
	// r := gin.New()
	// r.POST("/handle", func(c *gin.Context) {
	// 	msg := Message{}
	// 	if err := c.ShouldBindJSON(&msg); err != nil {
	// 		logrus.Error("[handle] ", "Error parse req body: ", err)
	// 		return
	// 	}
	// 	logrus.Info(msg.ID)
	// })
	// r.Run(":14500") // listen and serve on 0.0.0.0:8080 (for windows "localhost:8080")
	for {

	}
}

func handle(deliveries <-chan amqp.Delivery, done chan error) {
	for d := range deliveries {
		log.Printf(
			"got %dB delivery: [%v] %q",
			len(d.Body),
			d.DeliveryTag,
			d.Body,
		)
		d.Ack(false)
	}
	log.Printf("handle: deliveries channel closed")
	done <- nil
}
