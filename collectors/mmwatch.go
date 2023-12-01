// Copyright 2023 Jibu Tech
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package collectors

import (
	"bytes"
	"context"
	"net/url"
	"strings"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/go-kit/log/level"
	"github.com/prometheus/client_golang/prometheus"
)

var (
	mmwatchTimeout = kingpin.Flag("collector.mmwatch.timeout", "Timeout for mmwatch execution").Default("30").Int()
	//watchStates    = []string{"HEALTHY", "SUSPENDED", "DOWN"}
	watchSections = []string{"watchStatus", "watchNodeStatus"}
	//mmwatchMap     = map[string]string{
	//	"watchID":    "WatchID",
	//	"device":     "Device",
	//	"watchPath":  "WatchPath",
	//	"watchState": "WatchState",
	//	"nodeStatus": "NodeStatus",
	//	"nodeName":   "NodeName",
	//}
	MmwatchExec = mmwatchExec
)

type MMWatchMetric struct {
	Device     string
	WatchID    string
	WatchPath  string
	WatchState string
	Nodes      []NodeMetric
}

type NodeMetric struct {
	NodeName   string
	NodeStatus string
}

type MmwatchCollector struct {
	Watcher *prometheus.Desc
	Node    *prometheus.Desc
	logger  log.Logger
}

func init() {
	registerCollector("mmwatch", true, NewMmwatchCollector)
}

func NewMmwatchCollector(logger log.Logger) Collector {
	return MmwatchCollector{
		Watcher: prometheus.NewDesc(prometheus.BuildFQName(namespace, "watch", "status"),
			"GPFS watch status", []string{"device", "watchid", "watchpath", "watchstate"}, nil),
		Node: prometheus.NewDesc(prometheus.BuildFQName(namespace, "watch_node", "status"),
			"GPFS node status", []string{"nodename", "nodestatus"}, nil),
		logger: logger,
	}
}

// Collector Describes all metrics
func (c MmwatchCollector) Describe(ch chan<- *prometheus.Desc) {
	ch <- c.Watcher
	ch <- c.Node
}

// Collect Collects all metrics
func (c MmwatchCollector) Collect(ch chan<- prometheus.Metric) {
	collectTime := time.Now()
	timeout := 0
	errorMetric := 0
	level.Debug(c.logger).Log("msg", "Collecting mmwatch metrics")
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(*mmwatchTimeout)*time.Second)
	defer cancel()

	mmwatchOutput, err := MmwatchExec(ctx)

	if err == context.DeadlineExceeded {
		timeout = 1
		level.Error(c.logger).Log("msg", "Timeout executing mmwatch")
	} else if err != nil {
		level.Error(c.logger).Log("msg", err)
		errorMetric = 1
	}

	mmwatchMetric := parseMmwatchOutput(mmwatchOutput, c.logger)
	ch <- prometheus.MustNewConstMetric(c.Watcher, prometheus.GaugeValue, 1, mmwatchMetric.Device, mmwatchMetric.WatchID, mmwatchMetric.WatchPath, mmwatchMetric.WatchState)
	for _, nodeMetric := range mmwatchMetric.Nodes {
		ch <- prometheus.MustNewConstMetric(c.Node, prometheus.GaugeValue, 1, nodeMetric.NodeName, nodeMetric.NodeStatus)
	}

	ch <- prometheus.MustNewConstMetric(collectError, prometheus.GaugeValue, float64(errorMetric), "mmwatch")
	ch <- prometheus.MustNewConstMetric(collecTimeout, prometheus.GaugeValue, float64(timeout), "mmwatch")
	ch <- prometheus.MustNewConstMetric(collectDuration, prometheus.GaugeValue, time.Since(collectTime).Seconds(), "mmwatch")
}

func mmwatchExec(ctx context.Context) (string, error) {
	cmd := execCommand(ctx, *sudoCmd, "/usr/lpp/mmfs/bin/mmwatch", "all", "status", "-Y")
	var out bytes.Buffer
	cmd.Stdout = &out
	err := cmd.Run()
	if ctx.Err() == context.DeadlineExceeded {
		return "", ctx.Err()
	} else if err != nil {
		return "", err
	}
	return out.String(), nil
}

func parseMmwatchOutput(mmwatchOutput string, logger log.Logger) MMWatchMetric {
	var mmwatchMetric MMWatchMetric
	nodeMetrics := []NodeMetric{}
	headers := make(map[string][]string)
	lines := strings.Split(mmwatchOutput, "\n")
	for _, line := range lines {
		l := strings.TrimSpace(line)
		if !strings.HasPrefix(l, "mmwatch") {
			level.Debug(logger).Log("msg", "Skip due to prefix", "line", l)
			continue
		}
		items := strings.Split(l, ":")
		metricType := items[1]
		if !SliceContains(watchSections, metricType) {
			level.Debug(logger).Log("msg", "Skip unknown type", "type", metricType, "line", l)
			continue
		}
		if items[2] == "HEADER" {
			headers[items[1]] = append(headers[items[1]], items...)
			continue
		}

		var nodeMetric NodeMetric
		if metricType == "watchStatus" {
			if deviceIndex := SliceIndex(headers["watchStatus"], "device"); deviceIndex != -1 {
				mmwatchMetric.Device = items[deviceIndex]
			}
			if watchIDIndex := SliceIndex(headers["watchStatus"], "watchID"); watchIDIndex != -1 {
				mmwatchMetric.WatchID = items[watchIDIndex]
			}
			if watchPathIndex := SliceIndex(headers["watchStatus"], "watchPath"); watchPathIndex != -1 {
				watchPath, err := url.QueryUnescape(items[watchPathIndex])
				if err != nil {
					level.Debug(logger).Log("msg", "url parse error", err, "line", l)
					continue
				}
				mmwatchMetric.WatchPath = watchPath
			}
			if watchStateIndex := SliceIndex(headers["watchStatus"], "watchState"); watchStateIndex != -1 {
				mmwatchMetric.WatchState = items[watchStateIndex]
			}
		}
		if metricType == "watchNodeStatus" {
			if nodeNameIndex := SliceIndex(headers["watchNodeStatus"], "nodeName"); nodeNameIndex != -1 {
				nodeMetric.NodeName = items[nodeNameIndex]
			}
			if nodeStatusIndex := SliceIndex(headers["watchNodeStatus"], "nodeStatus"); nodeStatusIndex != -1 {
				nodeMetric.NodeStatus = items[nodeStatusIndex]
			}
			nodeMetrics = append(nodeMetrics, nodeMetric)
		}
	}
	mmwatchMetric.Nodes = nodeMetrics

	return mmwatchMetric
}
