package rabbit_helper

import (
	"github.com/rabbitmq/amqp091-go"
	"log"
	"math"
	"time"
)

type Rabbit struct {
	RabbitUrl  string
	Connection *amqp091.Connection
	Channel    *amqp091.Channel
}

func (rabbit *Rabbit) Connect(rabbitUrl string) error {
	var connection *amqp091.Connection
	var channel *amqp091.Channel
	var counts int64
	var backOff = 1 * time.Second

	for {
		c, err := amqp091.Dial(rabbitUrl)
		if err != nil {
			log.Println("rabbitmq not yet ready...!")
			counts++
		} else {
			log.Println("connected to RabbitMQ")
			connection = c
			break
		}

		if counts > 5 {
			log.Println(err)
			return err
		}

		backOff = time.Duration(math.Pow(float64(counts), 2)) * time.Second
		log.Printf("backing off for %v seconds\n", backOff)
		time.Sleep(backOff)

	}

	ch, err := connection.Channel()
	if err != nil {
		log.Println(err)
		return err
	}
	channel = ch

	rabbit.Connection = connection
	rabbit.Channel = channel
	return nil
}

func (rabbit *Rabbit) Consume(routingKey string) {

}

func (rabbit *Rabbit) Publish(routingKey string, data map[string]any) {

}
