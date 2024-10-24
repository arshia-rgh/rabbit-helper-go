package rabbit_helper

import "github.com/rabbitmq/amqp091-go"

type Rabbit struct {
	RabbitUrl  string
	Connection *amqp091.Connection
	Channel    *amqp091.Channel
}

func (rabbit *Rabbit) Connect(rabbitUrl string) (Rabbit, error) {

}
