package main

import (
	"encoding/json"
	"io"
	"io/ioutil"
	"math"
	"net/http"
	"strconv"
	"strings"
	"time"

	log "github.com/Sirupsen/logrus"
	"github.com/influxdb/influxdb/client/v2"
)

// this structure defines the JSON message format of Influxdb 0.8
type influxStruct struct {
	Name    string      `json:"name"`
	Columns []string    `json:"columns"`
	Points  [][]float64 `json:"points"`
}

type influxMultiStruct []influxStruct

// this is the main function to process an Influxdb 0.8 request
func HandleInflux(w http.ResponseWriter, r *http.Request) {

	// instanciate an Influxdb data structure
	t := influxMultiStruct{}

	jsonDataFromHttp, err := ioutil.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Body parsing Error, dropping request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	err = json.Unmarshal(jsonDataFromHttp, &t)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("Body JSON parsing Error, dropping request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	// check the params
	err = r.ParseForm()
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("URL parsing Error, dropping request")
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	log.WithFields(log.Fields{
		"RemoteAddr":      r.RemoteAddr,
		"X-Forwarded-For": r.Header["X-Forwarded-For"],
		"RequestURI":      r.RequestURI,
	}).Info("Ready to parse request")

	// we process only if login/pass is good
	// for the moment the login/pass to remote influx is fixed, but we should
	// send the data with the incoming login/pass

	if r.Form["u"][0] == influx_user && r.Form["p"][0] == influx_pass {
		// parse the metrics and add to the queue
		io.WriteString(w, "request accepted for user "+influx_user+"\n")
	} else {
		io.WriteString(w, "Username/Password missmatch\n")
		log.WithFields(log.Fields{
			"RemoteAddr":      r.RemoteAddr,
			"X-Forwarded-For": r.Header["X-Forwarded-For"],
			"RequestURI":      r.RequestURI,
		}).Warn("Username/Password missmatch")
		return
	}

	// parse the json
	var pointTime time.Time
	for index, element := range t {
		log.WithFields(log.Fields{
			"id":      index,
			"name":    element.Name,
			"columns": element.Columns,
			"points":  element.Points,
		}).Debug("Adding a new point")

		// split the serie name in master name and tags
		serieSplit := strings.Split(element.Name, ".")

		// generate tags from the doted name
		tags := make(map[string]string)
		tags["name"] = element.Name
		for index, tag := range serieSplit {
			tags["tag"+strconv.Itoa(index)] = tag
		}

		// add each points of the same serie
		for _, point := range element.Points {
			// splitting fields
			fields := make(map[string]interface{})
			for index, field := range element.Columns {
				fields[field] = point[index]
			}

			if field, ok := fields["time"]; ok {
				// parse the time
				switch {
				case r.Form["time_precision"][0] == "s":
					pointTime = time.Unix(int64(field.(float64)), 0)

				case r.Form["time_precision"][0] == "ms":
					timeSec := math.Abs(field.(float64) / 1000)
					timeNs := math.Mod(field.(float64), 1000)
					pointTime = time.Unix(int64(timeSec), int64(timeNs))

				case r.Form["time_precision"][0] == "us":
					timeSec := math.Abs(field.(float64) / 1000000)
					timeNs := math.Mod(field.(float64), 1000000)
					pointTime = time.Unix(int64(timeSec), int64(timeNs))

				default:
					pointTime = time.Now()
					log.WithFields(log.Fields{
						"id": index,
					}).Info("no time provided, using local time ", pointTime)
				}

				// finaly remove the time entry as it is a special value
				delete(fields, "time")

			} else {
				pointTime = time.Now()
				log.WithFields(log.Fields{
					"id": index,
				}).Info("no time provided, using local time ", pointTime)
			}

			// create a new point
			pt, _ := client.NewPoint(serieSplit[0], tags, fields, pointTime)
			influxPointbatch.AddPoint(pt)
			log.WithFields(log.Fields{
				"metric": serieSplit[0],
				"time":   pointTime,
			}).Info("new Metric added")
		}
	}
	err = influxClient.Write(influxPointbatch)
	if err != nil {
		log.WithFields(log.Fields{
			"error": err,
		}).Warn("error adding Metric")
		return
		//fmt.Println("Error: ", err.Error())
	}
}
