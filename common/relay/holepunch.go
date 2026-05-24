package relay

import (
	alog "BBgrid/common/log"
	"context"
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"io"
	"net"
	"sync"
	"time"
)

const (
	MaxPacketSize  = 65507
	PunchMagic     = 0x50 // 'P'
	PunchAckMagic  = 0x41 // 'A'
	PunchPingMagic = 0x50 // Ping
	PunchPongMagic = 0x47 // Pong('G')
)

// Session represents a P2P session configuration
type Session struct {
	SourcePort    int
	TargetPort    int
	TargetLocalIP string
	Token         string
	PeerAddr      string
	Role          string // "source" or "target"
}

func GenerateToken() string {
	b := make([]byte, 16)
	rand.Read(b)
	return hex.EncodeToString(b)
}

// ============== UDP Hole Punching ==============

func UDPPunch(ctx context.Context, sess *Session) (*net.UDPConn, error) {
	peerAddr, err := net.ResolveUDPAddr("udp", sess.PeerAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve peer: %w", err)
	}

	localAddr, err := net.ResolveUDPAddr("udp", ":0")
	if err != nil {
		return nil, fmt.Errorf("resolve local: %w", err)
	}

	conn, err := net.ListenUDP("udp", localAddr)
	if err != nil {
		return nil, fmt.Errorf("listen udp: %w", err)
	}

	localPort := conn.LocalAddr().(*net.UDPAddr).Port
	alog.Info(alog.CatRelay, "P2P UDP bound, punching", "port", localPort, "peer", peerAddr.String())

	done := make(chan struct{})
	var established bool
	var establishedMu sync.Mutex // avoid data race on 'established'
	deadline := time.Now().Add(30 * time.Second)
	conn.SetReadDeadline(deadline)

	tokenBytes := []byte(sess.Token)

	go func() {
		ticker := time.NewTicker(100 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-done:
				return
			case <-ticker.C:
				establishedMu.Lock()
				est := established
				establishedMu.Unlock()
				if est {
					return
				}
				pkt := make([]byte, 2+len(tokenBytes))
				pkt[0] = PunchMagic
				pkt[1] = byte(len(tokenBytes))
				copy(pkt[2:], tokenBytes)
				conn.WriteToUDP(pkt, peerAddr)
			}
		}
	}()

	buf := make([]byte, MaxPacketSize)
	for {
		select {
		case <-ctx.Done():
			conn.Close()
			close(done)
			return nil, ctx.Err()
		default:
		}

		conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		n, remote, err := conn.ReadFromUDP(buf)
		if err != nil {
			establishedMu.Lock()
			est := established
			establishedMu.Unlock()
			if est {
				close(done)
				conn.SetReadDeadline(time.Time{})
				return conn, nil
			}
			if time.Now().After(deadline) {
				conn.Close()
				close(done)
				return nil, fmt.Errorf("hole punch timeout")
			}
			continue
		}

		if n < 2 {
			continue
		}

		if buf[0] != PunchMagic && buf[0] != PunchAckMagic {
			continue
		}

		tokLen := int(buf[1])
		if n < 2+tokLen {
			continue
		}

		recvToken := string(buf[2 : 2+tokLen])
		expectedToken := sess.Token

		if buf[0] == PunchMagic && recvToken == expectedToken {
			ack := make([]byte, 2+tokLen)
			ack[0] = PunchAckMagic
			ack[1] = byte(tokLen)
			copy(ack[2:], tokenBytes)
			conn.WriteToUDP(ack, remote)
			establishedMu.Lock()
			established = true
			establishedMu.Unlock()
			close(done)
			conn.SetReadDeadline(time.Time{})
			alog.Info(alog.CatRelay, "P2P UDP hole punched", "remote", remote.String())
			return conn, nil
		}

		if buf[0] == PunchAckMagic && recvToken == expectedToken {
			establishedMu.Lock()
			established = true
			establishedMu.Unlock()
			close(done)
			conn.SetReadDeadline(time.Time{})
			alog.Info(alog.CatRelay, "P2P UDP hole punched", "remote", remote.String())
			return conn, nil
		}
	}
}

// ============== TCP Hole Punching ==============

func TCPPunch(ctx context.Context, sess *Session) (net.Conn, error) {
	peerAddr, err := net.ResolveTCPAddr("tcp", sess.PeerAddr)
	if err != nil {
		return nil, fmt.Errorf("resolve peer: %w", err)
	}

	dialer := net.Dialer{
		Timeout:   10 * time.Second,
		LocalAddr: &net.TCPAddr{IP: net.IPv4zero, Port: 0},
	}

	ctx2, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()

	resultCh := make(chan net.Conn, 2)
	errCh := make(chan error, 2)

	var tryCount int
retryLoop:
	for {
		select {
		case <-ctx2.Done():
			break retryLoop
		default:
		}

		conn, err := dialer.DialContext(ctx2, "tcp", peerAddr.String())
		if err != nil {
			tryCount++
			if tryCount > 5 {
				errCh <- fmt.Errorf("tcp punch failed after %d tries: %w", tryCount, err)
				break
			}
			time.Sleep(500 * time.Millisecond)
			continue
		}

		if err := tcpHandshake(conn, sess.Token); err != nil {
			conn.Close()
			tryCount++
			time.Sleep(500 * time.Millisecond)
			continue
		}

		alog.Info(alog.CatRelay, "P2P TCP connected", "remote", conn.RemoteAddr().String())
		resultCh <- conn
		return <-resultCh, nil
	}

	select {
	case conn := <-resultCh:
		return conn, nil
	case err := <-errCh:
		return nil, err
	case <-ctx2.Done():
		return nil, ctx2.Err()
	}
}

func tcpHandshake(conn net.Conn, token string) error {
	tokenBytes := []byte(token)
	tokLen := len(tokenBytes)

	auth := make([]byte, 1+1+tokLen)
	auth[0] = PunchMagic
	auth[1] = byte(tokLen)
	copy(auth[2:], tokenBytes)

	if _, err := conn.Write(auth); err != nil {
		return fmt.Errorf("write auth: %w", err)
	}

	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	peerTokLen, err := readPeerAuth(conn, token)
	if err != nil {
		return fmt.Errorf("read peer auth: %w", err)
	}

	ack := []byte{PunchAckMagic}
	if _, err := conn.Write(ack); err != nil {
		return fmt.Errorf("write ack: %w", err)
	}

	_ = peerTokLen
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	resp := make([]byte, 1)
	if _, err := io.ReadFull(conn, resp); err != nil {
		return fmt.Errorf("read peer ack: %w", err)
	}
	if resp[0] != PunchAckMagic {
		return fmt.Errorf("invalid ack: 0x%02x", resp[0])
	}

	conn.SetReadDeadline(time.Time{})
	return nil
}

func readPeerAuth(conn net.Conn, expectedToken string) (int, error) {
	header := make([]byte, 2)
	if _, err := io.ReadFull(conn, header); err != nil {
		return 0, err
	}
	if header[0] != PunchMagic {
		return 0, fmt.Errorf("invalid magic: 0x%02x", header[0])
	}
	tokLen := int(header[1])
	if tokLen < 1 || tokLen > 256 {
		return 0, fmt.Errorf("invalid token length: %d", tokLen)
	}
	tokenBuf := make([]byte, tokLen)
	if _, err := io.ReadFull(conn, tokenBuf); err != nil {
		return 0, err
	}
	if string(tokenBuf) != expectedToken {
		return 0, fmt.Errorf("token mismatch")
	}
	return tokLen, nil
}

// ============== Listener for TCP P2P source side ==============

type TCPPunchListener struct {
	ln       net.Listener
	sess     *Session
	peerIP   string
	peerPort int
}

func NewTCPPunchListener(ctx context.Context, sess *Session) (*TCPPunchListener, func() (net.Conn, error)) {
	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", sess.SourcePort))
	if err != nil {
		return nil, nil
	}

	l := &TCPPunchListener{
		ln:   ln,
		sess: sess,
	}

	return l, l.acceptPunch(ctx)
}

func (l *TCPPunchListener) acceptPunch(ctx context.Context) func() (net.Conn, error) {
	return func() (net.Conn, error) {
		for {
			select {
			case <-ctx.Done():
				l.ln.Close()
				return nil, ctx.Err()
			default:
			}

			localConn, err := l.ln.Accept()
			if err != nil {
				return nil, err
			}

			return localConn, nil
		}
	}
}

func (l *TCPPunchListener) Close() error {
	return l.ln.Close()
}
