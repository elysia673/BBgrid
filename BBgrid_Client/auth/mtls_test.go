package auth

import (
	"os"
	"testing"
)

func TestInitMTLS_SetsTLSConfig(t *testing.T) {
	tmpDir, _ := os.MkdirTemp("", "auth-test-*")
	defer os.RemoveAll(tmpDir)

	certPath := tmpDir + "/server.crt"
	keyPath := tmpDir + "/client.key"
	caPath := tmpDir + "/ca.crt"

	os.WriteFile(certPath, []byte("x"), 0600)
	os.WriteFile(keyPath, []byte("x"), 0600)

	mgr := NewManager(Config{
		Mode:           AuthModeMTLS,
		CertPath:       certPath,
		PrivateKeyPath: keyPath,
		CACertPath:     caPath,
		Insecure:       true,
	})

	// initMTLS should fail with invalid cert, that's expected
	// But the key test: does GetTLSConfig return nil when Init fails?
	err := mgr.Init()
	if err == nil {
		t.Log("initMTLS succeeded (unexpected with dummy cert)")
	}
	tlsCfg := mgr.GetTLSConfig()
	if err != nil && tlsCfg == nil {
		t.Logf("✅ initMTLS fails → GetTLSConfig returns nil (correct)")
	} else if err == nil && tlsCfg != nil {
		t.Logf("✅ initMTLS succeeds → GetTLSConfig returns config (correct)")
	} else {
		t.Fatalf("BUG: err=%v but tlsCfg=%v (inconsistent!)", err, tlsCfg)
	}
}

func TestInitToken_SetsTLSConfig(t *testing.T) {
	mgr := NewManager(Config{
		Mode:  AuthModeToken,
		Token: "test-token",
	})

	err := mgr.Init()
	if err != nil {
		t.Fatal(err)
	}

	tlsCfg := mgr.GetTLSConfig()
	if tlsCfg == nil {
		t.Fatal("BUG: initToken should set TLS config for wss:// connections")
	}
	t.Log("✅ initToken sets TLS config for wss:// connections")
}
