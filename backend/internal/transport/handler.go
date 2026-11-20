// Package transport implements the DBX stdio-jsonl Sidecar handler for Flink Studio.
// All JobManager interaction is read-only GET against the Flink REST API.
// "Consumed messages" are read from TaskManager stdout log files, where Flink's
// print sink emits SQL results (+I[...] rows).
package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"regexp"
	"strings"

	dbxpluginsdk "github.com/t8y2/dbx/plugins/sdk/go/dbx-plugin-sdk"
	"github.com/xihonglei/flink-studio/backend/internal/connection"
)

const pluginID = "local.flink.studio"

// Handler satisfies dbxpluginsdk.Handler.
type Handler struct{ manager *connection.Manager }

func NewHandler(m *connection.Manager) *Handler { return &Handler{manager: m} }

type callParams struct {
	ConnectionID string              `json:"connection_id"`
	Connection   json.RawMessage     `json:"connection"`
	Method       string              `json:"method"`
	Path         string              `json:"path"`
	Query        map[string][]string `json:"query"`
	Requests     []callParams        `json:"requests"`
	JobID        string              `json:"jobId"`
	TaskManager  string              `json:"taskManager"`
	FileName     string              `json:"fileName"`
	Limit        int                 `json:"limit"`
	Skill        string              `json:"skill"`
}

func (h *Handler) Handle(
	ctx dbxpluginsdk.RequestContext, method string, params json.RawMessage, emitter *dbxpluginsdk.Emitter,
) (any, *dbxpluginsdk.PluginError) {
	var p callParams
	if len(params) > 0 && string(params) != "null" {
		if err := json.Unmarshal(params, &p); err != nil {
			return nil, dbxpluginsdk.NewError(-32602, "invalid request parameters")
		}
	}
	// Lifecycle payload: host may send it as params.connection or the params itself.
	lc := p.Connection
	if len(lc) == 0 { lc = params }

	switch method {
	case "flink/ping":
		return map[string]any{"pong": true, "plugin": pluginID}, nil

	case "flink/test", "connection/test":
		return h.test(lc, p)

	case "connection/connect":
		return h.connect(lc)

	case "connection/disconnect":
		h.manager.Disconnect(p.ConnectionID)
		return map[string]any{"ok": true}, nil

	case "connection/list":
		return map[string]any{"connections": h.manager.List()}, nil

	case "flink/overview":
		return h.proxy(lc, p, "/overview")

	case "flink/jobs":
		return h.proxy(lc, p, "/jobs")

	case "flink/job":
		path := "/jobs/" + pathEscape(p.JobID)
		return h.proxy(lc, p, path)

	case "flink/taskmanagers":
		return h.proxy(lc, p, "/taskmanagers")

	case "flink/taskmanager-logs":
		path := "/taskmanagers/" + pathEscape(p.TaskManager) + "/logs"
		return h.proxy(lc, p, path)

	case "flink/logfile":
		return h.logfile(lc, p)

	case "flink/messages":
		return h.messages(lc, p)

	case "flink/config":
		return h.proxy(lc, p, "/jobmanager/config")

	case "flink/request":
		return h.proxy(lc, p, p.Path)

	case "flink/batch":
		return h.batch(lc, p)

	default:
		return nil, dbxpluginsdk.MethodNotFound(method)
	}
}

// --- helpers ---

func (h *Handler) session(lc json.RawMessage, id string) (*connection.Session, *dbxpluginsdk.PluginError) {
	s, err := h.manager.GetOrConnect(lc)
	if err != nil { return nil, dbxpluginsdk.NewError(-32000, err.Error()) }
	if id != "" {
		if cached, ok := h.manager.Get(id); ok { return cached, nil }
	}
	return s, nil
}

func (h *Handler) test(lc json.RawMessage, p callParams) (any, *dbxpluginsdk.PluginError) {
	s, perr := h.session(lc, p.ConnectionID)
	if perr != nil { return nil, perr }
	overview, err := s.Ping(context.Background())
	if err != nil { return nil, dbxpluginsdk.NewError(-32000, err.Error()) }
	return map[string]any{
		// success/ok/message keep the result compatible with DBX's connection
		// dialog, which requires a `message` field in the test result.
		"success":      true,
		"ok":           true,
		"message":      "Connected to Flink JobManager REST at " + s.Endpoint,
		"endpoint":     s.Endpoint,
		"flinkVersion": overview["flink-version"],
		"taskmanagers": overview["taskmanagers"],
		"jobsRunning":  overview["jobs-running"],
	}, nil
}

func (h *Handler) connect(lc json.RawMessage) (any, *dbxpluginsdk.PluginError) {
	s, perr := h.session(lc, "")
	if perr != nil { return nil, perr }
	return map[string]any{"id": s.ID, "name": s.Name, "endpoint": s.Endpoint, "readOnly": s.ReadOnly, "connected": true}, nil
}

// proxy performs one read-only GET and returns a decoded JSON body (or raw text fallback).
func (h *Handler) proxy(lc json.RawMessage, p callParams, path string) (any, *dbxpluginsdk.PluginError) {
	if err := validatePath(path); err != nil { return nil, dbxpluginsdk.NewError(-32602, err.Error()) }
	s, perr := h.session(lc, p.ConnectionID)
	if perr != nil { return nil, perr }
	status, body, _, err := s.Do(context.Background(), http.MethodGet, path, p.Query)
	if err != nil { return nil, dbxpluginsdk.NewError(-32000, err.Error()) }
	if status < 200 || status >= 300 {
		return nil, dbxpluginsdk.NewError(-32000, fmt.Sprintf("flink %s returned %d: %s", path, status, firstN(string(body), 200)))
	}
	return decoded(body), nil
}

func (h *Handler) batch(lc json.RawMessage, p callParams) (any, *dbxpluginsdk.PluginError) {
	if len(p.Requests) == 0 { return map[string]any{"results": []any{}}, nil }
	s, perr := h.session(lc, p.ConnectionID)
	if perr != nil { return nil, perr }
	results := make([]any, 0, len(p.Requests))
	for _, r := range p.Requests {
		path := r.Path
		if err := validatePath(path); err != nil {
			results = append(results, map[string]any{"error": err.Error()}); continue
		}
		status, body, _, err := s.Do(context.Background(), http.MethodGet, path, r.Query)
		if err != nil { results = append(results, map[string]any{"error": err.Error()}); continue }
		if status < 200 || status >= 300 {
			results = append(results, map[string]any{"status": status, "error": firstN(string(body), 200)}); continue
		}
		results = append(results, decoded(body))
	}
	return map[string]any{"results": results}, nil
}

// logfile returns a TaskManager log file as plain text.
func (h *Handler) logfile(lc json.RawMessage, p callParams) (any, *dbxpluginsdk.PluginError) {
	if p.TaskManager == "" || p.FileName == "" {
		return nil, dbxpluginsdk.NewError(-32602, "taskManager and fileName are required")
	}
	path := "/taskmanagers/" + pathEscape(p.TaskManager) + "/logs/" + pathEscape(p.FileName)
	s, perr := h.session(lc, p.ConnectionID)
	if perr != nil { return nil, perr }
	status, body, _, err := s.Do(context.Background(), http.MethodGet, path, nil)
	if err != nil { return nil, dbxpluginsdk.NewError(-32000, err.Error()) }
	if status < 200 || status >= 300 {
		return nil, dbxpluginsdk.NewError(-32000, fmt.Sprintf("log file returned %d", status))
	}
	return map[string]any{"taskManager": p.TaskManager, "fileName": p.FileName, "content": string(body)}, nil
}

// rowRe matches print-sink changelog rows such as: +I[2249, 成都, 显示器, 680.36]
var rowRe = regexp.MustCompile(`(?m)([+\-]I|[+\-]U|D)\s*\[([^\]\r\n]*)\]`)

// messages scans all TaskManager .out logs for print-sink rows and parses them.
func (h *Handler) messages(lc json.RawMessage, p callParams) (any, *dbxpluginsdk.PluginError) {
	s, perr := h.session(lc, p.ConnectionID)
	if perr != nil { return nil, perr }
	ctx := context.Background()

	// Resolve TaskManagers.
	tms := []string{}
	if p.TaskManager != "" { tms = append(tms, p.TaskManager) } else {
		status, body, _, err := s.Do(ctx, http.MethodGet, "/taskmanagers", nil)
		if err != nil { return nil, dbxpluginsdk.NewError(-32000, err.Error()) }
		if status < 200 || status >= 300 { return nil, dbxpluginsdk.NewError(-32000, fmt.Sprintf("taskmanagers returned %d", status)) }
		var tmResp struct{ TaskManagers []struct{ ID string `json:"id"` } `json:"taskmanagers"` }
		_ = json.Unmarshal(body, &tmResp)
		for _, tm := range tmResp.TaskManagers { tms = append(tms, tm.ID) }
	}

	type row struct {
		Op    string   `json:"op"`
		Cells []string `json:"cells"`
		TM    string   `json:"taskManager"`
		File  string   `json:"file"`
	}
	rows := []row{}
	filesScanned := 0
	for _, tm := range tms {
		status, body, _, err := s.Do(ctx, http.MethodGet, "/taskmanagers/"+pathEscape(tm)+"/logs", nil)
		if err != nil || status < 200 || status >= 300 { continue }
		var lr struct{ Logs []struct{ Name string `json:"name"`; Size int64 `json:"size"` } `json:"logs"` }
		_ = json.Unmarshal(body, &lr)
		for _, lf := range lr.Logs {
			if !strings.HasSuffix(lf.Name, ".out") || lf.Size == 0 { continue }
			filesScanned++
			_, content, _, err := s.Do(ctx, http.MethodGet, "/taskmanagers/"+pathEscape(tm)+"/logs/"+pathEscape(lf.Name), nil)
			if err != nil { continue }
			for _, m := range rowRe.FindAllStringSubmatch(string(content), -1) {
				cells := splitCells(m[2])
				rows = append(rows, row{Op: m[1], Cells: cells, TM: tm, File: lf.Name})
			}
		}
	}
	// Keep newest rows, preserve original order.
	limit := p.Limit
	if limit <= 0 { limit = 200 }
	if len(rows) > limit { rows = rows[len(rows)-limit:] }
	return map[string]any{
		"rows": rows, "total": len(rows), "filesScanned": filesScanned,
		"taskManagers": len(tms), "note": "rows parsed from Flink print-sink TaskManager stdout logs",
	}, nil
}

// splitCells splits a print-sink row body on commas not inside quotes/brackets.
func splitCells(s string) []string {
	out := []string{}
	cur := strings.Builder{}
	quote := byte(0)
	depth := 0
	for i := 0; i < len(s); i++ {
		c := s[i]
		if quote != 0 {
			cur.WriteByte(c)
			if c == quote && (i == 0 || s[i-1] != '\\') { quote = 0 }
			continue
		}
		switch c {
		case '\'', '"':
			quote = c; cur.WriteByte(c)
		case '[', '{':
			depth++; cur.WriteByte(c)
		case ']', '}':
			depth--; cur.WriteByte(c)
		case ',':
			if depth == 0 { out = append(out, strings.TrimSpace(cur.String())); cur.Reset() } else { cur.WriteByte(c) }
		default:
			cur.WriteByte(c)
		}
	}
	if cur.Len() > 0 { out = append(out, strings.TrimSpace(cur.String())) }
	return out
}

func validatePath(path string) error {
	if path == "" { return fmt.Errorf("path is required") }
	if !strings.HasPrefix(path, "/") { return fmt.Errorf("path must start with /") }
	if strings.Contains(path, "..") { return fmt.Errorf("path traversal is not allowed") }
	// job ids / tm ids may contain colon and dot; that's fine.
	return nil
}

func pathEscape(s string) string { return strings.ReplaceAll(s, "/", "%2F") }

func decoded(body []byte) any {
	var v any
	if err := json.Unmarshal(body, &v); err != nil { return map[string]any{"text": string(body)} }
	return v
}

func firstN(s string, n int) string { if len(s) > n { return s[:n] }; return s }
