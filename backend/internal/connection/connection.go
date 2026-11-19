// Package connection resolves DBX lifecycle payloads into a Flink JobManager
// REST endpoint and caches lightweight HTTP clients keyed by connection id.
// Credentials are kept only in json:"-" fields and never reach the UI.
package connection

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AuthKind string

const (
	AuthNone   AuthKind = ""
	AuthBasic  AuthKind = "basic"
	AuthBearer AuthKind = "bearer"
)

// Auth holds credentials. Never serialized (json:"-" on every field).
type Auth struct {
	Kind  AuthKind
	User  string
	Pass  string // basic
	Token string // bearer
}

// Config is the resolved connection.
type Config struct {
	ID          string
	Name        string
	Scheme      string
	Host        string
	Port        int
	Auth        Auth
	TLSVerify   bool
	Timeout     time.Duration
	ReadOnly    bool
	MaxBodyMB   int
}

// BaseURL is the only place host/port become a URL.
func (c Config) BaseURL() string {
	scheme, host, port := c.Scheme, c.Host, c.Port
	if scheme == "" { scheme = "http" }
	if host == "" { host = "127.0.0.1" }
	if port == 0 { port = 8081 }
	return scheme + "://" + net.JoinHostPort(host, strconv.Itoa(port))
}

// Session is the safe public projection of a connection.
type Session struct {
	ID        string `json:"id"`
	Name      string `json:"name"`
	Endpoint  string `json:"endpoint"`
	ReadOnly  bool   `json:"readOnly"`
	Connected bool   `json:"connected"`

	cfg    Config      `json:"-"`
	client *http.Client `json:"-"`
}

func (s *Session) Config() Config     { return s.cfg }
func (s *Session) Client() *http.Client { return s.client }

// Manager caches sessions by connection id (lifecycle events may replay).
type Manager struct {
	mu       sync.RWMutex
	sessions map[string]*Session
}

func NewManager() *Manager { return &Manager{sessions: map[string]*Session{}} }

func (m *Manager) Connect(cfg Config) (*Session, error) {
	if cfg.ID == "" { return nil, fmt.Errorf("connection.id is required") }
	m.mu.Lock()
	defer m.mu.Unlock()
	if s, ok := m.sessions[cfg.ID]; ok { return s, nil }
	tr := &http.Transport{}
	if !cfg.TLSVerify {
		tr.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402 - explicit opt-in
	}
	client := &http.Client{Transport: tr, Timeout: cfg.Timeout}
	s := &Session{
		ID: cfg.ID, Name: cfg.Name, Endpoint: cfg.BaseURL(),
		ReadOnly: cfg.ReadOnly, Connected: true, cfg: cfg, client: client,
	}
	m.sessions[cfg.ID] = s
	return s, nil
}

func (m *Manager) Get(id string) (*Session, bool) {
	m.mu.RLock(); defer m.mu.RUnlock()
	s, ok := m.sessions[id]; return s, ok
}

// GetOrConnect resolves a raw lifecycle payload on first use.
func (m *Manager) GetOrConnect(raw json.RawMessage) (*Session, error) {
	var probe struct {
		ConnectionID string `json:"connection_id"`
		Connection   struct{ ID string `json:"id"` } `json:"connection"`
	}
	_ = json.Unmarshal(raw, &probe)
	id := probe.ConnectionID
	if id == "" { id = probe.Connection.ID }
	if id != "" {
		if s, ok := m.Get(id); ok { return s, nil }
	}
	cfg, err := ParseLifecycle(raw)
	if err != nil { return nil, err }
	return m.Connect(cfg)
}

func (m *Manager) Disconnect(id string) bool {
	m.mu.Lock(); defer m.mu.Unlock()
	if _, ok := m.sessions[id]; !ok { return false }
	delete(m.sessions, id); return true
}

func (m *Manager) List() []Session {
	m.mu.RLock(); defer m.mu.RUnlock()
	out := make([]Session, 0, len(m.sessions))
	for _, s := range m.sessions { out = append(out, *s) }
	return out
}

// ParseLifecycle resolves connection fields across the layers the DBX host uses.
// Later layers win: runtime-injected endpoint overrides stored config.
func ParseLifecycle(raw json.RawMessage) (Config, error) {
	var root map[string]any
	if err := json.Unmarshal(raw, &root); err != nil {
		return Config{}, fmt.Errorf("invalid lifecycle payload: %w", err)
	}
	conn := asMap(root["connection"])
	ext := asMap(conn["external_config"])
	if len(ext) == 0 { ext = asMap(root["external_config"]) }
	runtime := asMap(root["runtime"])
	secrets := asMap(root["secrets"])
	connSecrets := asMap(conn["connection_secrets"])

	cfg := Config{
		ID:        asString(pick([]string{"id", "connection_id"}, root, conn)),
		Name:      asString(pick([]string{"name", "display_name"}, root, ext, conn)),
		Scheme:    asString(pick([]string{"scheme", "protocol"}, conn, ext, runtime, root)),
		Host:      asString(pick([]string{"host", "hostname", "jobmanager_url"}, conn, ext, runtime, root)),
		Port:      asInt(pick([]string{"port", "rest_port"}, conn, ext, runtime, root)),
		TLSVerify: asBool(pick([]string{"tls_verify", "verify_tls"}, conn, ext, runtime, root), true),
		ReadOnly:  asBool(pick([]string{"read_only", "readonly"}, conn, ext, root), true),
		MaxBodyMB: asInt(pick([]string{"max_body_mb"}, conn, ext, root)),
		Timeout:   time.Duration(asInt(pick([]string{"timeout_secs", "timeout"}, conn, ext, runtime, root))) * time.Second,
	}
	// A single jobmanager_url like "http://host:8081" is accepted.
	if cfg.Host != "" && strings.Contains(cfg.Host, "://") {
		u := cfg.Host
		if i := strings.Index(u, "://"); i >= 0 { cfg.Scheme = u[:i]; u = u[i+3:] }
		u = strings.TrimSuffix(u, "/")
		if h, p, err := net.SplitHostPort(u); err == nil { cfg.Host, _ = h, p; cfg.Port, _ = strconv.Atoi(p) } else { cfg.Host = u }
	}
	if cfg.Scheme == "" { cfg.Scheme = "http" }
	if cfg.Port == 0 { cfg.Port = 8081 }
	if cfg.Timeout <= 0 { cfg.Timeout = 12 * time.Second }
	if cfg.MaxBodyMB <= 0 { cfg.MaxBodyMB = 16 }
	if cfg.Name == "" { cfg.Name = cfg.ID }
	if cfg.ID == "" { return Config{}, fmt.Errorf("lifecycle payload has no connection.id") }

	authType := strings.ToLower(asString(pick([]string{"auth_type", "authentication"}, conn, ext, root)))
	secretLayers := []map[string]any{conn, ext, secrets, connSecrets, root}
	switch authType {
	case "basic", "basic_auth", "password":
		cfg.Auth = Auth{Kind: AuthBasic, User: asString(pick([]string{"username", "user"}, conn, ext, root)), Pass: asString(pick([]string{"password"}, secretLayers...))}
	case "bearer", "token":
		cfg.Auth = Auth{Kind: AuthBearer, Token: asString(pick([]string{"token", "bearer_token"}, secretLayers...))}
	default:
		cfg.Auth = Auth{Kind: AuthNone}
	}
	return cfg, nil
}

// Do performs a GET/HEAD against the JobManager REST API and returns raw bytes.
func (s *Session) Do(ctx context.Context, method, path string, query map[string][]string) (int, []byte, http.Header, error) {
	cfg := s.cfg
	u := strings.TrimRight(cfg.BaseURL(), "/") + path
	if len(query) > 0 {
		parts := []string{}
		for k, vals := range query {
			for _, v := range vals { parts = append(parts, urlQueryEscape(k)+"="+urlQueryEscape(v)) }
		}
		u += "?" + strings.Join(parts, "&")
	}
	req, err := http.NewRequestWithContext(ctx, method, u, nil)
	if err != nil { return 0, nil, nil, err }
	switch cfg.Auth.Kind {
	case AuthBasic:
		req.SetBasicAuth(cfg.Auth.User, cfg.Auth.Pass)
	case AuthBearer:
		req.Header.Set("Authorization", "Bearer "+cfg.Auth.Token)
	}
	req.Header.Set("Accept", "application/json, text/plain, */*")
	resp, err := s.client.Do(req)
	if err != nil { return 0, nil, nil, err }
	defer resp.Body.Close()
	limit := int64(cfg.MaxBodyMB) * 1024 * 1024
	body, err := io.ReadAll(io.LimitReader(resp.Body, limit))
	if err != nil { return resp.StatusCode, nil, resp.Header, err }
	return resp.StatusCode, body, resp.Header, nil
}

func urlQueryEscape(s string) string {
	// Minimal RFC3986 escaping using net/url under the hood.
	return url.QueryEscape(s)
}

// --- helpers ---
func pick(keys []string, layers ...map[string]any) any {
	var result any
	for _, layer := range layers {
		for _, k := range keys { if v, ok := layer[k]; ok && v != nil { result = v } }
	}
	return result
}
func asMap(v any) map[string]any { if m, ok := v.(map[string]any); ok { return m }; return map[string]any{} }
func asString(v any) string { if s, ok := v.(string); ok { return s }; return "" }
func asInt(v any) int {
	switch x := v.(type) {
	case float64: return int(x)
	case int: return x
	case string: n, _ := strconv.Atoi(x); return n
	}
	return 0
}
func asBool(v any, fallback bool) bool {
	if b, ok := v.(bool); ok { return b }
	if s, ok := v.(string); ok { if b, err := strconv.ParseBool(s); err == nil { return b } }
	return fallback
}

// Ping is a connectivity probe: GET /overview.
func (s *Session) Ping(ctx context.Context) (map[string]any, error) {
	status, body, _, err := s.Do(ctx, http.MethodGet, "/overview", nil)
	if err != nil { return nil, err }
	if status < 200 || status >= 300 { return nil, fmt.Errorf("flink responded %d: %s", status, strings.TrimSpace(string(bytes.TrimSpace(body)))) }
	var overview map[string]any
	if err := json.Unmarshal(body, &overview); err != nil { return nil, fmt.Errorf("invalid /overview: %w", err) }
	return overview, nil
}
