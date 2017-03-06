package main

import (
	"fmt"
	"time"
	"strings"
	"encoding/json"
	"regexp"

	"github.com/parnurzeal/gorequest"
	"gopkg.in/alecthomas/kingpin.v2"
)

const (
	ver string = "0.12"
)

var (
	esURL = kingpin.Flag("url", "elasticsearch URL").Default("http://localhost:9200").Short('u').String()
	measurementName = kingpin.Flag("measurement-name", "InfluxDB measurement name").Default("elasticsearch_shards").Short('m').String()
	timeout = kingpin.Flag("timeout", "timeout for HTTP requests").Default("10").Short('t').Int()
)

// Shard : struct containts shard data
type Shard struct {
	Index string `json:"index"`
	Shard string `json:"shard"`
	PriRep string `json:"prirep"`
	State string `json:"state"`
	Docs string `json:"docs"`
	Store string `json:"store"`
	IP string `json:"ip"`
	Node string `json:"node"`
}

func esQuery(esURL string, timeout int) (string, error) {
	request := gorequest.New()
	resp, body, errs := request.Get(esURL).Timeout(time.Duration(timeout) * time.Second).End()

	if errs != nil {
		errsStr := make([]string, 0)
		for _, e := range errs {
			errsStr = append(errsStr, fmt.Sprintf("%s", e))
		}
		return "", fmt.Errorf("%s", strings.Join(errsStr, ", "))
	}
	if resp.StatusCode != 200 {
		return "", fmt.Errorf("HTTP response code: %s", resp.Status)
	}
	return body, nil
}

func parseShards(data string) ([]Shard, error) {
	var shards []Shard
	err := json.Unmarshal([]byte(data), &shards)
	if err != nil {
		return shards, fmt.Errorf("JSON parse failed")
	}
	return shards, nil
}

func genInfluxDBLineProto(measurementName string, shards []Shard) string {
	output := make([]string, 0)

	for _, shard := range shards {
		values := []string{
			// redundand shard number as a value - influxdb doesn't allow to store records without values
			// shards in other state than "STARTED" don't have values, "docs", "store" are unavailable
			"shard_value=" + shard.Shard + "i",
		}
		tags := []string{
			measurementName,
			"index=" + shard.Index,
			"shard=" + shard.Shard,
			"prirep=" + shard.PriRep,
			"state=" + shard.State,
		}
		if shard.IP != "" {
			tags = append(tags, "ip=" + shard.IP)
		}
		if shard.Node != "" {
			tags = append(tags, "node=" + shard.Node)
		}
		if shard.Docs != "" {
			values = append(values, "docs=" + shard.Docs + "i")
		}
		if shard.Store != "" {
			values = append(values, "store=" + shard.Store + "i")
		}

		output = append(output, strings.Join(tags, ",") + " " + strings.Join(values, ","))
	}
	return strings.Join(output, "\n")
}

func normalizeFields(shards []Shard) []Shard {
	var normalizedShards []Shard
	re := regexp.MustCompile(`^(\S+)\s+.*\s(\S+)$`)
	for _, shard := range shards {
		s := shard
		if shard.State == "RELOCATING" {
			if matches := re.FindStringSubmatch(shard.Node); matches != nil {
				s.Node = matches[1] + "->" + matches[2]
			} else {
				continue
			}
		}
		s.Node = strings.Replace(s.Node, " ", "", -1)
		normalizedShards = append(normalizedShards, s)
	}
	return normalizedShards
}

func genShardStats(esURL string, timeout int, measurementName string) (string, error) {
	url := esURL + "/_cat/shards?format=json&bytes=b"

	esData, err := esQuery(url, timeout)
	if err != nil {
		return "", err
	}

	shards, err := parseShards(esData)
	if err != nil {
		return "", err
	}

	return genInfluxDBLineProto(measurementName, normalizeFields(shards)), nil
}

func main() {
	kingpin.Version(ver)
	kingpin.Parse()

	stats, err := genShardStats(*esURL, *timeout, *measurementName)
	if err != nil {
		panic(err)
	}
	fmt.Println(stats)
}
