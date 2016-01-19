package main

import (
	"flag"
	"net/http"
	"os"

	log "github.com/Sirupsen/logrus"
	"github.com/gorilla/mux"
	"github.com/influxdb/influxdb/client/v2"
)

// define global vars
var (
	influx_user      string
	influx_pass      string
	InfluxClient     client.Client
	InfluxPointbatch client.BatchPoints
)

func init() {
	// Log as JSON instead of the default ASCII formatter.
	log.SetFormatter(&log.JSONFormatter{})

	// Output to stderr instead of stdout, could also be a file.
	log.SetOutput(os.Stdout)

	// Only log the warning severity or above.
	log.SetLevel(log.InfoLevel)
}

func Handlers() *mux.Router {
	r := mux.NewRouter()
	r.HandleFunc("/db/graphite/series", HandleInflux).Methods("POST")
	return r
}

func main() {
	// local vars
	var err error

	addr := flag.String("bind", ":8089", "on which address should the proxy listen")
	dest := flag.String("dest", "http://localhost:8086", "url to send the Influxdb 0.9 metrics")
	db := flag.String("db", "graphite", "Influxdb 0.9 database to use")
	user := flag.String("user", "influx", "Influxdb 0.9 user")
	pass := flag.String("password", "secret", "Influxdb 0.9 password")
	verbose := flag.Bool("verbose", false, "set verbose messages ON")
	debug := flag.Bool("debug", false, "set debug ON")
	flag.Parse()

	// set the right logging level
	if *debug == true {
		log.SetLevel(log.DebugLevel)
	} else if *verbose == true {
		log.SetLevel(log.InfoLevel)
	} else {
		log.SetLevel(log.WarnLevel)
	}

	log.WithFields(log.Fields{
		"bind": addr,
	}).Info("Influxdb 0.8 0.9 proxy started")

	influx_user = *user
	influx_pass = *pass

	// connect to the destination influxdb 0.9 server
	conf := client.HTTPConfig{
		Addr:     *dest,
		Username: *user,
		Password: *pass,
	}

	InfluxClient, err = client.NewHTTPClient(conf)
	if err != nil {
		log.WithFields(log.Fields{
			"address": *dest,
			"user":    *user,
			"error":   err.Error(),
		}).Fatal("Can't connect to Influxdb 0.9")
	}

	// Create a new global batch point handler
	InfluxPointbatch, _ = client.NewBatchPoints(client.BatchPointsConfig{
		Database:  *db,
		Precision: "us",
	})

	http.ListenAndServe(*addr, Handlers())
}
