package log

import (
	"bytes"
	"strings"
	"testing"
)

func TestBasicLogging(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(DEBUG)
	SetFormat(FormatText)

	Info(CatAuth, "client connected", "client_id", "abc123", "remote", "1.2.3.4:5678")
	out := buf.String()
	if !strings.Contains(out, "[INFO]") {
		t.Errorf("expected [INFO], got: %s", out)
	}
	if !strings.Contains(out, "client connected") {
		t.Errorf("expected 'client connected', got: %s", out)
	}
	if !strings.Contains(out, "client_id=abc123") {
		t.Errorf("expected client_id=abc123, got: %s", out)
	}
	if !strings.Contains(out, "log_test.go:") {
		t.Errorf("expected caller file info, got: %s", out)
	}
}

func TestSanitizeToken(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(DEBUG)
	SetFormat(FormatText)

	Info(CatProxy, "proxy created", "token", "super-secret-token-value", "port", "8080")
	out := buf.String()
	if strings.Contains(out, "super-secret-token-value") {
		t.Errorf("token leaked: %s", out)
	}
	if !strings.Contains(out, "token=***") {
		t.Errorf("expected token=***, got: %s", out)
	}
	if !strings.Contains(out, "port=8080") {
		t.Errorf("expected port=8080, got: %s", out)
	}
}

func TestJSONFormat(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(DEBUG)
	SetFormat(FormatJSON)

	Info(CatAuth, "client connected", "client_id", "abc123")
	out := buf.String()
	if !strings.Contains(out, `"level":"INFO"`) {
		t.Errorf("expected JSON level, got: %s", out)
	}
	if !strings.Contains(out, `"msg":"client connected"`) {
		t.Errorf("expected JSON msg, got: %s", out)
	}
	if !strings.Contains(out, `"client_id":"abc123"`) {
		t.Errorf("expected JSON field, got: %s", out)
	}
	if !strings.Contains(out, `"file":"log_test.go"`) {
		t.Errorf("expected caller file in JSON, got: %s", out)
	}
	if !strings.Contains(out, `"pid":`) {
		t.Errorf("expected PID in JSON, got: %s", out)
	}
}

func TestLevelFiltering(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(WARN)
	SetFormat(FormatText)

	Info(CatSystem, "should not appear")
	Warn(CatSystem, "should appear")
	out := buf.String()
	if strings.Contains(out, "should not appear") {
		t.Errorf("INFO should be filtered: %s", out)
	}
	if !strings.Contains(out, "should appear") {
		t.Errorf("WARN should appear: %s", out)
	}
}

func TestEntry(t *testing.T) {
	var buf bytes.Buffer
	SetOutput(&buf)
	SetLevel(DEBUG)
	SetFormat(FormatText)

	entry := With("client_id", "abc123")
	entry.Info(CatTunnel, "tunnel established", "port", "25565")
	out := buf.String()
	if !strings.Contains(out, "client_id=abc123") {
		t.Errorf("expected client_id=abc123, got: %s", out)
	}
	if !strings.Contains(out, "port=25565") {
		t.Errorf("expected port=25565, got: %s", out)
	}
}

func TestMask(t *testing.T) {
	if Mask("abcdef") != "abc***" {
		t.Errorf("Mask failed: %s", Mask("abcdef"))
	}
	if Mask("ab") != "***" {
		t.Errorf("Mask failed for short string: %s", Mask("ab"))
	}
}
