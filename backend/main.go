// Flink Studio Sidecar — the single backend executable declared in manifest.json.
// stdout is reserved for the stdio-jsonl protocol; diagnostics go to stderr and
// never contain credentials.
package main

import (
	"fmt"
	"os"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
	"github.com/xihonglei/flink-studio/backend/internal/connection"
	"github.com/xihonglei/flink-studio/backend/internal/transport"
)

const (
	pluginID      = "local.flink.studio"
	pluginVersion = "0.1.1" // Keep in sync with manifest.json (check-manifest.py asserts).
)

func main() {
	manager := connection.NewManager()
	handler := transport.NewHandler(manager)
	server := dbxpluginsdk.NewServer(dbxpluginsdk.Metadata{
		ID:      pluginID,
		Version: pluginVersion,
		Capabilities: []string{
			"flink/ping", "flink/test", "flink/overview", "flink/jobs", "flink/job",
			"flink/taskmanagers", "flink/taskmanager-logs", "flink/logfile",
			"flink/messages", "flink/config", "flink/request", "flink/batch",
			"connection/test", "connection/connect", "connection/disconnect", "connection/list",
		},
	}, handler)
	if err := server.Serve(); err != nil {
		fmt.Fprintln(os.Stderr, "[flink-studio] sidecar stopped:", err)
		os.Exit(1)
	}
}
