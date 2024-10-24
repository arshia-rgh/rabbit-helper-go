package rabbit_helper

import (
	"encoding/json"
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

func New(rabbitUrl string) *Rabbit {
	var rabbit = &Rabbit{RabbitUrl: rabbitUrl}
	err := rabbit.connect()

	if err != nil {
		log.Panic(err)
	}

	return rabbit

}

func (rabbit *Rabbit) connect() error {
	var connection *amqp091.Connection
	var channel *amqp091.Channel
	var counts int64
	var backOff = 1 * time.Second

	for {
		c, err := amqp091.Dial(rabbit.RabbitUrl)
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

func (rabbit *Rabbit) queueDeclare(routingKey string) error {
	_, err := rabbit.Channel.QueueDeclare(
		routingKey,
		false,
		false,
		false,
		false,
		nil,
	)
	return err
}

func (rabbit *Rabbit) Consume(routingKey string, callback func(data any)) error {
	err := rabbit.queueDeclare(routingKey)
	if err != nil {
		return err
	}

	for {
		msgs, err := rabbit.Channel.Consume(
			routingKey,
			"",
			true,
			false,
			false,
			false,
			nil,
		)

		if err != nil {
			log.Printf("error consuming message: %v", err)
			continue
		}
		log.Printf("waiting for messages in queue: %v", routingKey)

		for msg := range msgs {
			log.Printf("recieved a message: %v", msg.Body)

			var data any
			err := json.Unmarshal(msg.Body, &data)
			if err != nil {
				log.Println("error unmarshalling message:", err)
				continue
			}

			go callback(data)
		}

	}
}

func (rabbit *Rabbit) Publish(routingKey string, data any) error {
	err := rabbit.queueDeclare(routingKey)
	if err != nil {
		return err
	}

	jsonData, err := json.Marshal(data)
	if err != nil {
		return err
	}

	err = rabbit.Channel.Publish(
		"",
		routingKey,
		false,
		false,
		amqp091.Publishing{
			ContentType: "application/json",
			Body:        jsonData,
		},
	)
	if err != nil {
		log.Println("error publishing message: ", err)
		return err
	}

	log.Printf("published message from queue: %v msg: %v \n", routingKey, jsonData)

	return nil

}

func (rabbit *Rabbit) Close() {
	if rabbit.Channel != nil {
		rabbit.Channel.Close()
	}

	if rabbit.Connection != nil {
		rabbit.Connection.Close()
	}
}
