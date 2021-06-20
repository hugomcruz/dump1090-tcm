// ----------------------------------------------------------------------------
// Dump1090 MQTT subscriber
// Subscribed from MQTT and save in a file on a hourly basis rotation
// Contact: Hugo Cruz - hugo.m.cruz@gmail.com
// ----------------------------------------------------------------------------
package main

import (
	"bytes"
	"compress/gzip"
	"io/ioutil"
	"path/filepath"
	"strconv"
	"strings"
	"time"

	"os"

	"github.com/apache/pulsar-client-go/pulsar"
	log "github.com/sirupsen/logrus"
	"github.com/tkanos/gonfig"
)

// Channel variables
var done = make(chan bool)
var tasks = make(chan string)

//Configuration global varaible
var configuration Configuration

type Configuration struct {
	PulsarTopic             string
	PulsarAuthenticationKey string
	PulsarURL               string
	PulsarSubscriber        string
	FilesPath               string
	LogLevel                string
	RadarID                 string
}

func genFileName(startTime time.Time) string {

	hour, _, _ := startTime.Clock()
	year := startTime.Year()
	month := int(startTime.Month())
	day := startTime.Day()

	//Change later put trailing zero - Use the time formating
	monthStr := strconv.Itoa(month)
	if month < 10 {
		monthStr = "0" + monthStr
	}

	//Change later put trailing zero
	dayStr := strconv.Itoa(day)
	if day < 10 {
		dayStr = "0" + dayStr
	}

	//Change later put trailing zero
	hourStr := strconv.Itoa(hour)
	if hour < 10 {
		hourStr = "0" + hourStr
	}

	filename := "fr-" + strconv.Itoa(year) + monthStr + dayStr + "_" + hourStr + "00.csv"
	return filename

}

// Calculate next roll over - return next rollover timestamp
// and initial time of start (used for filename)
func nextRollOver() (int64, time.Time) {

	loc, _ := time.LoadLocation("UTC")
	currentTime := time.Now().In(loc)
	hour, _, _ := currentTime.Clock()
	year := currentTime.Year()
	month := currentTime.Month()
	day := currentTime.Day()

	windowInit := time.Date(year, month, day, hour, 0, 0, 0, time.UTC)
	windowClose := windowInit.Add(time.Hour)

	windowCloseTS := windowClose.UnixNano() / 1000000
	return windowCloseTS, windowInit
}

func consume(data string) {
	nextRoll, startTime := nextRollOver()
	filename := genFileName(startTime)

	fullpath := filepath.Join(configuration.FilesPath, filename)
	file, err := os.OpenFile(fullpath+".tmp", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	loc, _ := time.LoadLocation("UTC")

	//Debug
	tm := time.Unix(nextRoll/1000, 0).In(loc)
	timeNow := time.Now().In(loc)
	timeNowMillis := timeNow.UnixNano() / 1000000
	log.Debug("Current time       : ", timeNow)
	log.Debug("Current timestamp  : ", timeNowMillis)
	log.Debug("Start Time         : ", startTime)
	log.Debug("Next roll time     : ", tm)
	log.Debug("Next Roll timestamp: ", nextRoll)
	log.Debug("New Filename       : ", filename)

	msg := data

	//Split into Individual messages
	dataArray := strings.Split(msg, "\n")

	for _, radarLine := range dataArray {
		lineSplit := strings.Split(radarLine, ",")

		// Last line is empry after split
		if len(lineSplit) == 1 {
			continue
		}

		timestampStr := lineSplit[1]

		timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
		if err != nil {
			log.Error("Error converting time")
		}

		//fmt.Printf("CurrentTimestamp: %d", timestamp)

		if timestamp >= nextRoll {
			log.Info("Rolling the storage file now.")
			file.Close()
			e := os.Rename(fullpath+".tmp", fullpath)
			if e != nil {
				log.Fatal(e)
			}

			nextRoll, startTime = nextRollOver()
			filename = genFileName(startTime)

			//Debug
			tm := time.Unix(nextRoll/1000, 0).In(loc)
			timeNow := time.Now().In(loc)
			timeNowMillis := timeNow.UnixNano() / 1000000
			log.Debug("Current time       : ", timeNow)
			log.Debug("Current timestamp  : ", timeNowMillis)
			log.Debug("Start Time         : ", startTime)
			log.Debug("Next roll time     : ", tm)
			log.Debug("Next Roll timestamp: ", nextRoll)
			log.Debug("New Filename       : ", filename)

			fullpath := filepath.Join(configuration.FilesPath, filename)
			file, err = os.OpenFile(fullpath+".tmp", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
			if err != nil {
				panic(err)
			}
		}

		//Write to the file
		file.WriteString(radarLine + "\n")

	}

}

func main() {

	// Setup the logger
	log.SetFormatter(&log.TextFormatter{
		DisableColors: false,
		FullTimestamp: true,
	})

	log.Info(">>>>>>>>>> STARTING the Dump1090 Store Subscriber <<<<<<<<<<<<<")

	// Read the configuration
	configuration = Configuration{}
	err := gonfig.GetConf("config.json", &configuration)

	if err != nil {
		log.Error("Error reading configuration: ", err.Error())
		os.Exit(1)
	}

	// Setup the Log Level
	if configuration.LogLevel == "DEBUG" {
		log.SetLevel(log.DebugLevel)
	} else if configuration.LogLevel == "INFO" {
		log.SetLevel(log.InfoLevel)
	} else if configuration.LogLevel == "ERROR" {
		log.SetLevel(log.ErrorLevel)
	} else if configuration.LogLevel == "WARN" {
		log.SetLevel(log.WarnLevel)
	} else {
		log.SetLevel(log.InfoLevel) // Make info default in case of missing config
	}

	// Pulsar stuff
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

	// Prepare the rile name
	nextRoll, startTime := nextRollOver()
	filename := genFileName(startTime)

	fullpath := filepath.Join(configuration.FilesPath, filename)
	file, err := os.OpenFile(fullpath+".tmp", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
	if err != nil {
		panic(err)
	}

	loc, _ := time.LoadLocation("UTC")

	//Debug
	tm := time.Unix(nextRoll/1000, 0).In(loc)
	timeNow := time.Now().In(loc)
	timeNowMillis := timeNow.UnixNano() / 1000000
	log.Debug("Current time       : ", timeNow)
	log.Debug("Current timestamp  : ", timeNowMillis)
	log.Debug("Start Time         : ", startTime)
	log.Debug("Next roll time     : ", tm)
	log.Debug("Next Roll timestamp: ", nextRoll)
	log.Debug("New Filename       : ", filename)

	// Receive individual messages here
	for cm := range channel {
		msg := cm.Message

		byteData := msg.Payload()
		props := msg.Properties()

		//Only process messages for a particular radar ID
		if props["id"] == configuration.RadarID {
			log.Debug("Processing message for Radar: ", props["id"])
			//Decompress the payload message
			r, _ := gzip.NewReader(bytes.NewReader(byteData))
			result, _ := ioutil.ReadAll(r)

			data := string(result)

			msg := data

			//Split into Individual messages
			dataArray := strings.Split(msg, "\n")

			for _, radarLine := range dataArray {
				lineSplit := strings.Split(radarLine, ",")

				// Last line is empry after split
				if len(lineSplit) == 1 {
					continue
				}

				timestampStr := lineSplit[1]

				timestamp, err := strconv.ParseInt(timestampStr, 10, 64)
				if err != nil {
					log.Error("Error converting time")
				}

				//fmt.Printf("CurrentTimestamp: %d", timestamp)

				if timestamp >= nextRoll {
					log.Info("Rolling the storage file now.")
					file.Close()
					e := os.Rename(fullpath+".tmp", fullpath)
					if e != nil {
						log.Fatal(e)
					}

					nextRoll, startTime = nextRollOver()
					filename = genFileName(startTime)

					//Debug
					tm := time.Unix(nextRoll/1000, 0).In(loc)
					timeNow := time.Now().In(loc)
					timeNowMillis := timeNow.UnixNano() / 1000000
					log.Debug("Current time       : ", timeNow)
					log.Debug("Current timestamp  : ", timeNowMillis)
					log.Debug("Start Time         : ", startTime)
					log.Debug("Next roll time     : ", tm)
					log.Debug("Next Roll timestamp: ", nextRoll)
					log.Debug("New Filename       : ", filename)

					fullpath := filepath.Join(configuration.FilesPath, filename)
					file, err = os.OpenFile(fullpath+".tmp", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
					if err != nil {
						panic(err)
					}
				}

				//Write to the file
				file.WriteString(radarLine + "\n")

			}

		}

		consumer.Ack(msg)
	}
}
