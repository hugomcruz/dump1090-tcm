// ----------------------------------------------------------------------------
// Dump1090 MQTT subscriber
// Subscribed from MQTT and prints to STDOUT
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------
package main

import (
	"bytes"
	"compress/gzip"
	"crypto/tls"
	"fmt"
	"io/ioutil"
	"log"

	"os"
	"os/signal"
	"syscall"

	"github.com/apache/pulsar-client-go/pulsar"
	MQTT "github.com/eclipse/paho.mqtt.golang"
	"github.com/tkanos/gonfig"
)

//Configuration Data
type Configuration struct {
	Protocol                string
	PulsarTopic             string
	PulsarAuthenticationKey string
	PulsarURL               string
	PulsarSubscriber        string

	MQTTServerURL string
	MQTTClientID  string
	MQTTTopic     string
	MQTTQos       int
	MQTTUsername  string
	MQTTPassword  string
}

func onMessageReceived(client MQTT.Client, message MQTT.Message) {

	byteData := message.Payload()

	//Decompress the payload message
	r, _ := gzip.NewReader(bytes.NewReader(byteData))
	result, _ := ioutil.ReadAll(r)

	data := string(result)

	fmt.Print(data)

}

func main() {

	// Read the configuration file using gonfig package
	configuration := Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		fmt.Println("Error reading configuration file: " + err.Error())
		fmt.Println("Exiting now.")
		os.Exit(1)

	}

	if configuration.Protocol == "MQTT" {

		// Create channel for subscription
		c := make(chan os.Signal, 1)
		signal.Notify(c, os.Interrupt, syscall.SIGTERM)

		connOpts := MQTT.NewClientOptions().AddBroker(configuration.MQTTServerURL).SetClientID(configuration.MQTTClientID).SetCleanSession(true)

		if configuration.MQTTUsername != "" {
			connOpts.SetUsername(configuration.MQTTUsername)
			if configuration.MQTTPassword != "" {
				connOpts.SetPassword(configuration.MQTTPassword)
			}
		}
		tlsConfig := &tls.Config{InsecureSkipVerify: true, ClientAuth: tls.NoClientCert}
		connOpts.SetTLSConfig(tlsConfig)

		connOpts.OnConnect = func(c MQTT.Client) {
			if token := c.Subscribe(configuration.MQTTTopic, byte(configuration.MQTTQos), onMessageReceived); token.Wait() && token.Error() != nil {
				panic(token.Error())
			}
		}

		client := MQTT.NewClient(connOpts)
		if token := client.Connect(); token.Wait() && token.Error() != nil {
			panic(token.Error())
		}

		<-c

	} else if configuration.Protocol == "Pulsar" {

		auth := pulsar.NewAuthenticationToken(configuration.PulsarAuthenticationKey)

		client, err := pulsar.NewClient(pulsar.ClientOptions{URL: configuration.PulsarURL, Authentication: auth})

		if err != nil {
			log.Fatal(err)
		}

		defer client.Close()

		channel := make(chan pulsar.ConsumerMessage, 100)

		options := pulsar.ConsumerOptions{
			Topic:            configuration.PulsarTopic,
			SubscriptionName: configuration.PulsarSubscriber,
			Type:             pulsar.Shared,
		}

		options.MessageChannel = channel

		consumer, err := client.Subscribe(options)
		if err != nil {
			log.Fatal(err)
		}

		defer consumer.Close()

		// Receive messages from channel. The channel returns a struct which contains message and the consumer from where
		// the message was received. It's not necessary here since we have 1 single consumer, but the channel could be
		// shared across multiple consumers as well
		for cm := range channel {
			msg := cm.Message

			byteData := msg.Payload()
			props := msg.Properties()

			//Decompress the payload message
			r, _ := gzip.NewReader(bytes.NewReader(byteData))
			result, _ := ioutil.ReadAll(r)

			data := string(result)

			fmt.Print(props)
			fmt.Print("\n")
			fmt.Print(data)

			consumer.Ack(msg)
		}

	}

}
