package collectors

import (
	"context"
	"os/exec"
	"strings"
	"testing"
	"time"

	"github.com/alecthomas/kingpin/v2"
	"github.com/go-kit/log"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

var (
	mmwatchStdout = `
mmwatch:watchStatus:HEADER:version:RESERVED:RESERVED:RESERVED:watchID:device:fileset:clusterID:watchPath:watchType:watchState:RESERVED:RESERVED:RESERVED:
mmwatch:watchNodeStatus:HEADER:version:RESERVED:RESERVED:RESERVED:watchID:device:fileset:clusterID:nodeName:nodeStatus:watchProcessType:RESERVED:RESERVED:RESERVED:
mmwatch:watchStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:%2Fjibufs:FSYS:Active::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode1:HEALTHY:conduit::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode2:HEALTHY:conduit::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode3:HEALTHY:conduit::::
`
	mmwatchStdoutSuspended = `
mmwatch:watchStatus:HEADER:version:RESERVED:RESERVED:RESERVED:watchID:device:fileset:clusterID:watchPath:watchType:watchState:RESERVED:RESERVED:RESERVED:
mmwatch:watchNodeStatus:HEADER:version:RESERVED:RESERVED:RESERVED:watchID:device:fileset:clusterID:nodeName:nodeStatus:watchProcessType:RESERVED:RESERVED:RESERVED:
mmwatch:watchStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:%2Fjibufs:FSYS:Suspended::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode1:SUSPENDED:conduit::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode2:SUSPENDED:conduit::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode3:SUSPENDED:conduit::::
`
	mmwatchStdoutFailed = `
mmwatch:watchStatus:HEADER:version:RESERVED:RESERVED:RESERVED:watchID:device:fileset:clusterID:watchPath:watchType:watchState:RESERVED:RESERVED:RESERVED:
mmwatch:watchNodeStatus:HEADER:version:RESERVED:RESERVED:RESERVED:watchID:device:fileset:clusterID:nodeName:nodeStatus:watchProcessType:RESERVED:RESERVED:RESERVED:
mmwatch:watchStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:%2Fjibufs:FSYS:Active::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode1:FAILED:conduit::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode2:FAILED:conduit::::
mmwatch:watchNodeStatus:0:1::::CLW1681130306:jibufs::7802268055787884788:janode3:FAILED:conduit::::
`
)

func TestMmwatch(t *testing.T) {
	execCommand = fakeExecCommand
	mockedExitStatus = 0
	mockedStdout = "foo"
	defer func() { execCommand = exec.CommandContext }()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	out, err := mmwatchExec(ctx)
	if err != nil {
		t.Errorf("Unexpected error: %s", err.Error())
	}
	if out != mockedStdout {
		t.Errorf("Unexpected out: %s", out)
	}
}

func TestParseMmwatch(t *testing.T) {
	watchMetrics := parseMmwatchOutput(mmwatchStdout, log.NewNopLogger())
	if watchMetrics.Device != "jibufs" {
		t.Errorf("Unexpected device: %s", watchMetrics.Device)
	}
	if watchMetrics.WatchID != "CLW1681130306" {
		t.Errorf("Unexpected watchID: %s", watchMetrics.WatchID)
	}
	if watchMetrics.WatchPath != "/jibufs" {
		t.Errorf("Unexpected watchPath: %s", watchMetrics.WatchPath)
	}
	if watchMetrics.WatchState != "Active" {
		t.Errorf("Unexpected watchState: %s", watchMetrics.WatchState)
	}
	if len(watchMetrics.Nodes) != 3 {
		t.Errorf("Unexpected number of nodes: %d", len(watchMetrics.Nodes))
	}

	for _, node := range watchMetrics.Nodes {
		if node.NodeName != "janode1" && node.NodeName != "janode2" && node.NodeName != "janode3" {
			t.Errorf("Unexpected node: %s", node)
		}
		if node.NodeStatus != "HEALTHY" {
			t.Errorf("Unexpected node status: %s", node.NodeStatus)
		}
	}

}

func TestMmwatchCollector(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	MmwatchExec = func(ctx context.Context) (string, error) {
		return mmwatchStdout, nil
	}
	expected := `
		# HELP gpfs_watch_status GPFS watch status
		# TYPE gpfs_watch_status gauge
		gpfs_watch_status{device="jibufs",watchid="CLW1681130306",watchpath="/jibufs",watchstate="Active"} 1
		# HELP gpfs_watch_node_status GPFS node status
		# TYPE gpfs_watch_node_status gauge
		gpfs_watch_node_status{nodename="janode1",nodestatus="HEALTHY"} 1
		gpfs_watch_node_status{nodename="janode2",nodestatus="HEALTHY"} 1
		gpfs_watch_node_status{nodename="janode3",nodestatus="HEALTHY"} 1
	`
	collector := NewMmwatchCollector(log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 7 {
		t.Errorf("Unexpected collection count %d, expected 7", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"gpfs_watch_status", "gpfs_watch_node_status"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestMmwatchCollectorSuspended(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	MmwatchExec = func(ctx context.Context) (string, error) {
		return mmwatchStdoutSuspended, nil
	}
	expected := `
		# HELP gpfs_watch_status GPFS watch status
		# TYPE gpfs_watch_status gauge
		gpfs_watch_status{device="jibufs",watchid="CLW1681130306",watchpath="/jibufs",watchstate="Suspended"} 1
		# HELP gpfs_watch_node_status GPFS node status
		# TYPE gpfs_watch_node_status gauge
		gpfs_watch_node_status{nodename="janode1",nodestatus="SUSPENDED"} 1
		gpfs_watch_node_status{nodename="janode2",nodestatus="SUSPENDED"} 1
		gpfs_watch_node_status{nodename="janode3",nodestatus="SUSPENDED"} 1
	`
	collector := NewMmwatchCollector(log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 7 {
		t.Errorf("Unexpected collection count %d, expected 7", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"gpfs_watch_status", "gpfs_watch_node_status"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}

func TestMmwatchCollectorError(t *testing.T) {
	if _, err := kingpin.CommandLine.Parse([]string{}); err != nil {
		t.Fatal(err)
	}
	MmwatchExec = func(ctx context.Context) (string, error) {
		return mmwatchStdoutFailed, nil
	}
	expected := `
		# HELP gpfs_watch_status GPFS watch status
		# TYPE gpfs_watch_status gauge
		gpfs_watch_status{device="jibufs",watchid="CLW1681130306",watchpath="/jibufs",watchstate="Active"} 1
		# HELP gpfs_watch_node_status GPFS node status
		# TYPE gpfs_watch_node_status gauge
		gpfs_watch_node_status{nodename="janode1",nodestatus="FAILED"} 1
		gpfs_watch_node_status{nodename="janode2",nodestatus="FAILED"} 1
		gpfs_watch_node_status{nodename="janode3",nodestatus="FAILED"} 1
	`
	collector := NewMmwatchCollector(log.NewNopLogger())
	gatherers := setupGatherer(collector)
	if val, err := testutil.GatherAndCount(gatherers); err != nil {
		t.Errorf("Unexpected error: %v", err)
	} else if val != 7 {
		t.Errorf("Unexpected collection count %d, expected 7", val)
	}
	if err := testutil.GatherAndCompare(gatherers, strings.NewReader(expected),
		"gpfs_watch_status", "gpfs_watch_node_status"); err != nil {
		t.Errorf("unexpected collecting result:\n%s", err)
	}
}
