// ----------------------------------------------------------------------------
// Pulsar Functions
//
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------

package main

import (
	"context"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
)

// Global varibales
const keepalive = 2
const pingTimeout = 1

func connectPulsar(configuration Configuration) pulsar.Client {
	log.Info("Connecting to Pulsar: ", configuration.PulsarURL)

	auth := pulsar.NewAuthenticationToken(configuration.PulsarAuthenticationKey)

	client, err := pulsar.NewClient(pulsar.ClientOptions{URL: configuration.PulsarURL, Authentication: auth})

	if err != nil {
		log.Fatal(err)
	}

	return client
}

func createProducer(client pulsar.Client, configuration Configuration) pulsar.Producer {
	log.Info("Creating Pulsar Producer: ")

	producer, err := client.CreateProducer(pulsar.ProducerOptions{
		Topic: configuration.PulsarTopic,
	})
	if err != nil {
		log.Fatal(err)
	}

	return producer
}

func sendPulsar(configuration Configuration, producer pulsar.Producer, message []byte) pulsar.MessageID {

	id, err := producer.Send(context.Background(), &pulsar.ProducerMessage{
		Payload: message,
		Properties: map[string]string{
			"id":  configuration.RadarID,
			"key": configuration.RadarKey,
		},
	})

	//defer producer.Close()

	if err != nil {
		log.Warn("Failed to publish message", err)
	}
	return id
}

func disconnectPulsar(c pulsar.Client) {
	c.Close()

}
