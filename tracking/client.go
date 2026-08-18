package tracking

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// MaxPublishHz is the hard cap for accepted Publish calls (points per second).
const MaxPublishHz = 50

// MinPublishInterval is the minimum gap between accepted online points.
const MinPublishInterval = time.Second / MaxPublishHz

// MaxEventBytes / MaxEventHz bound opaque custom events (ephemeral fan-out).
const MaxEventBytes = 4 * 1024
const MaxEventHz = 1
const MinEventInterval = time.Second / MaxEventHz

// ConnectionState mirrors the tracking client state machine.
type ConnectionState string

const (
	StateConnecting   ConnectionState = "connecting"
	StateOpen         ConnectionState = "open"
	StateReconnecting ConnectionState = "reconnecting"
	StateClosed       ConnectionState = "closed"
)

// DeviceAuth authenticates a publisher device.
type DeviceAuth struct {
	ClientID     string
	ClientSecret string
}

// ListenerAuth authenticates a dashboard/listener JWT.
type ListenerAuth struct {
	AccessToken string
}

// RefreshAuthFunc returns fresh credentials after AUTH / UNAUTHORIZED.
// Exactly one of device / listener should be non-nil.
type RefreshAuthFunc func(ctx context.Context) (device *DeviceAuth, listener *ListenerAuth, err error)

type subOpts struct {
	includeEvents bool
	minInterval   uint16
	handle        uint8
}

// Config opens a session against a Pickpoint tracking endpoint.
type Config struct {
	// Endpoint is the tracking host, e.g. "wss://tracking.pickpoint.io".
	// The SDK appends /v2/ws unless WSPath is set.
	Endpoint string
	Device   *DeviceAuth
	Listener *ListenerAuth
	// Path for WS (default /v2/ws).
	WSPath string
	// Subscribe: listener device UIDs to watch after Hello (and after reconnect).
	Subscribe []string

	// DisableReconnect turns off auto-reconnect (default enabled).
	DisableReconnect bool
	// Reconnect backoff (defaults: 500ms … 30s, unlimited attempts).
	ReconnectMinDelay    time.Duration
	ReconnectMaxDelay    time.Duration
	ReconnectMaxAttempts int

	// RefreshAuth is called on AUTH / UNAUTHORIZED before giving up.
	RefreshAuth RefreshAuthFunc

	// MaxQueueSize bounds Staging+InFlight (default 10_000).
	MaxQueueSize int
	// HelloTimeout waits for Hello after dial (default 10s).
	HelloTimeout time.Duration
}

type sender interface {
	Send(ClientMsg) error
	SendRaw([]byte) error
	Close() error
}

type pendingStart struct {
	ctx context.Context
	ch  chan startResult
}

type startResult struct {
	uid string
	err error
}

type pendingStop struct {
	ctx context.Context
	ch  chan error
}

type pendingResume struct {
	ctx context.Context
	ch  chan resumeResult
}

type resumeResult struct {
	acked uint32
	err   error
}

// Client is a tracking session (device or listener).
type Client struct {
	cfg Config

	mu            sync.Mutex
	send          sender
	state         ConnectionState
	trackUID      string
	clientSeq     uint64
	lastAckedSeq  uint64
	buf           *Buffer
	filter        NoiseFilter
	unackedFrames int
	backoff       BackoffState
	nextPublishAt time.Time
	nextEventAt   time.Time
	subscriptions map[string]*subOpts
	subByHandle   map[uint8]string
	intentional   bool
	dialGen       uint64
	wsConn        *websocket.Conn

	recvCh chan ServerMsg
	cmdCh  chan Command
	errCh  chan error

	startWait  *pendingStart
	stopWait   *pendingStop
	resumeWait *pendingResume
	starting   bool

	reconnectTimer *time.Timer
}

// Connect opens a tracking session and waits for Hello (or follows Relocate).
func Connect(ctx context.Context, cfg Config) (*Client, error) {
	if cfg.Endpoint == "" {
		return nil, fmt.Errorf("tracking: Endpoint is required")
	}
	if cfg.Device == nil && cfg.Listener == nil {
		return nil, fmt.Errorf("tracking: Device or Listener auth is required")
	}
	helloTO := cfg.HelloTimeout
	if helloTO <= 0 {
		helloTO = 10 * time.Second
	}
	cfg.HelloTimeout = helloTO

	c := &Client{
		cfg:           cfg,
		state:         StateConnecting,
		recvCh:        make(chan ServerMsg, 64),
		cmdCh:         make(chan Command, 16),
		errCh:         make(chan error, 1),
		subscriptions: make(map[string]*subOpts),
		subByHandle:   make(map[uint8]string),
		backoff:       NewBackoff(cfg.ReconnectMinDelay, cfg.ReconnectMaxDelay, cfg.ReconnectMaxAttempts),
		buf:           NewBuffer(cfg.MaxQueueSize, nil),
	}
	for _, uid := range cfg.Subscribe {
		if uid != "" {
			c.subscriptions[uid] = &subOpts{includeEvents: true}
		}
	}

	if err := c.dial(ctx, false); err != nil {
		return nil, err
	}
	return c, nil
}

func (c *Client) setState(s ConnectionState) {
	c.state = s
}

// State returns the connection state machine value.
func (c *Client) State() ConnectionState {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state
}

// TrackUID is the active track, if any.
func (c *Client) TrackUID() string {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.trackUID
}

// ClientSeq is the last assigned publish sequence.
func (c *Client) ClientSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.clientSeq
}

// LastAckedSeq is the highest server-acked client sequence.
func (c *Client) LastAckedSeq() uint64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lastAckedSeq
}

func (c *Client) dial(ctx context.Context, sendResume bool) error {
	c.mu.Lock()
	c.clearReconnectTimerLocked()
	c.dialGen++
	gen := c.dialGen
	if c.state == StateOpen || c.state == StateReconnecting {
		c.setState(StateReconnecting)
	} else {
		c.setState(StateConnecting)
	}
	cfg := c.cfg
	c.mu.Unlock()

	u, err := buildWSURL(cfg)
	if err != nil {
		return err
	}
	dialer := websocket.Dialer{Subprotocols: []string{Subprotocol}}
	conn, resp, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		if resp != nil {
			return fmt.Errorf("tracking: ws dial: %w (status %s)", err, resp.Status)
		}
		return fmt.Errorf("tracking: ws dial: %w", err)
	}
	if conn.Subprotocol() != Subprotocol {
		_ = conn.Close()
		return fmt.Errorf("tracking: server did not accept %s", Subprotocol)
	}

	c.mu.Lock()
	if gen != c.dialGen || c.intentional {
		c.mu.Unlock()
		_ = conn.Close()
		return fmt.Errorf("tracking: dial superseded")
	}
	if c.send != nil {
		_ = c.send.Close()
	}
	ws := &wsSender{conn: conn}
	c.send = ws
	c.wsConn = conn
	c.unackedFrames = 0
	c.mu.Unlock()

	helloCtx, cancel := context.WithTimeout(ctx, c.cfg.HelloTimeout)
	defer cancel()

	msg, err := readOneWS(helloCtx, conn)
	if err != nil {
		_ = conn.Close()
		return err
	}

	switch {
	case msg.Hello != nil:
		if msg.Hello.Version != ProtocolVersion {
			_ = conn.Close()
			return fmt.Errorf("tracking: unsupported protocol version %d", msg.Hello.Version)
		}
	case msg.Relocate != nil:
		_ = conn.Close()
		return c.handleRelocate(ctx, msg.Relocate, sendResume)
	case msg.Error != nil:
		_ = conn.Close()
		return errorFromWire(msg.Error)
	default:
		_ = conn.Close()
		return fmt.Errorf("tracking: expected hello, got %+v", msg)
	}

	c.mu.Lock()
	if gen != c.dialGen || c.intentional {
		c.mu.Unlock()
		_ = conn.Close()
		return fmt.Errorf("tracking: dial superseded")
	}
	c.setState(StateOpen)
	ResetBackoff(&c.backoff)
	c.mu.Unlock()

	go c.readLoopWS(conn, gen)

	if sendResume {
		if err := c.sendResumeAndWait(ctx); err != nil {
			if isFatalResumeError(codeOf(err)) && !isAuthError(codeOf(err)) {
				// TRACK_NOT_FOUND: stay connected; app must StartTrack.
			} else if isAuthError(codeOf(err)) {
				return err
			} else if !isRetryResumeError(codeOf(err)) {
				return err
			} else {
				return err
			}
		}
	}
	c.resubscribe()
	return nil
}

func codeOf(err error) ErrorCode {
	if e, ok := err.(*Error); ok {
		return e.Code
	}
	return 0
}

func readOneWS(ctx context.Context, conn *websocket.Conn) (ServerMsg, error) {
	type result struct {
		msg ServerMsg
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		mt, data, err := conn.ReadMessage()
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			ch <- result{err: err}
			return
		}
		if mt == websocket.TextMessage {
			closeProtocol(conn)
			ch <- result{err: ErrInvalidFrame}
			return
		}
		msg, err := DecodeServerMsg(data)
		if err != nil {
			closeProtocol(conn)
			ch <- result{err: err}
			return
		}
		ch <- result{msg: msg}
	}()
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return ServerMsg{}, fmt.Errorf("tracking: hello timeout: %w", ctx.Err())
	case r := <-ch:
		return r.msg, r.err
	}
}

func closeProtocol(conn *websocket.Conn) {
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseProtocolError, ""),
		time.Now().Add(time.Second),
	)
	_ = conn.Close()
}

func (c *Client) handleRelocate(ctx context.Context, rel *Relocate, sendResume bool) error {
	if rel.Endpoint != "" {
		c.mu.Lock()
		c.cfg.Endpoint = rel.Endpoint
		c.mu.Unlock()
	}
	delay := time.Duration(rel.RetryAfterMs) * time.Millisecond
	if delay > 0 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
	}
	c.mu.Lock()
	if c.trackUID != "" {
		sendResume = true
	}
	intentional := c.intentional
	c.mu.Unlock()
	if intentional {
		return fmt.Errorf("tracking: closed")
	}
	return c.dial(ctx, sendResume)
}

type wsSender struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *wsSender) Send(msg ClientMsg) error {
	if msg.Loc != nil && len(msg.Loc.Points) > 1 {
		frames := EncodeLocFrames(msg.Loc.Seq, msg.Loc.Points)
		for _, f := range frames {
			if err := w.SendRaw(f); err != nil {
				return err
			}
		}
		return nil
	}
	b, err := EncodeClientMsg(msg)
	if err != nil {
		return err
	}
	return w.SendRaw(b)
}

func (w *wsSender) SendRaw(b []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteMessage(websocket.BinaryMessage, b)
}

func (w *wsSender) Close() error {
	return w.conn.Close()
}

func (c *Client) readLoopWS(conn *websocket.Conn, gen uint64) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			c.onSocketClosed(gen)
			return
		}
		if mt == websocket.TextMessage {
			closeProtocol(conn)
			c.onSocketClosed(gen)
			return
		}
		msg, err := DecodeServerMsg(data)
		if err != nil {
			closeProtocol(conn)
			c.onSocketClosed(gen)
			return
		}
		if msg == (ServerMsg{}) {
			continue // unknown server type
		}
		c.dispatch(msg)
	}
}

func (c *Client) dispatch(msg ServerMsg) {
	switch {
	case msg.Relocate != nil:
		go func() {
			_ = c.handleRelocate(context.Background(), msg.Relocate, true)
		}()
		return
	case msg.ResumeOk != nil:
		c.mu.Lock()
		if uid := msg.ResumeOk.TrackUID; uid != "" {
			c.trackUID = uid
		}
		c.lastAckedSeq = uint64(msg.ResumeOk.LastAcked)
		if c.clientSeq < c.lastAckedSeq {
			c.clientSeq = c.lastAckedSeq
		}
		c.buf.AckThrough(msg.ResumeOk.LastAcked)
		c.unackedFrames = 0
		wait := c.resumeWait
		c.resumeWait = nil
		c.mu.Unlock()
		c.resendInFlight()
		c.flushStaging()
		if wait != nil {
			select {
			case wait.ch <- resumeResult{acked: msg.ResumeOk.LastAcked}:
			default:
			}
		}
		c.pushRecv(msg)
		return
	case msg.TrackStarted != nil:
		c.mu.Lock()
		c.trackUID = msg.TrackStarted.TrackUID
		c.clientSeq = 0
		c.lastAckedSeq = 0
		c.unackedFrames = 0
		c.starting = false
		wait := c.startWait
		c.startWait = nil
		c.mu.Unlock()
		if wait != nil {
			select {
			case wait.ch <- startResult{uid: msg.TrackStarted.TrackUID}:
			default:
			}
		}
		c.flushStaging()
		c.pushRecv(msg)
		return
	case msg.TrackStopped != nil:
		c.mu.Lock()
		if c.trackUID == msg.TrackStopped.TrackUID || msg.TrackStopped.TrackUID == "" {
			c.trackUID = ""
			c.buf.Clear()
			c.filter.Reset()
		}
		wait := c.stopWait
		c.stopWait = nil
		c.mu.Unlock()
		if wait != nil {
			select {
			case wait.ch <- nil:
			default:
			}
		}
		c.pushRecv(msg)
		return
	case msg.Ack != nil:
		c.mu.Lock()
		if uint64(msg.Ack.Seq) > c.lastAckedSeq {
			c.lastAckedSeq = uint64(msg.Ack.Seq)
		}
		c.buf.AckThrough(msg.Ack.Seq)
		c.unackedFrames = 0
		c.mu.Unlock()
		c.flushStaging()
		return
	case msg.Command != nil:
		select {
		case c.cmdCh <- *msg.Command:
		default:
		}
		return
	case msg.Error != nil:
		err := errorFromWire(msg.Error)
		c.mu.Lock()
		if c.resumeWait != nil {
			w := c.resumeWait
			c.resumeWait = nil
			if isFatalResumeError(err.Code) {
				c.trackUID = ""
				c.buf.Clear()
				c.filter.Reset()
				c.clientSeq = 0
				c.lastAckedSeq = 0
			}
			c.mu.Unlock()
			select {
			case w.ch <- resumeResult{err: err}:
			default:
			}
		} else {
			if err.Code == ErrorTrackNotFound {
				c.trackUID = ""
				c.buf.Clear()
				c.filter.Reset()
			}
			c.mu.Unlock()
		}
		c.mu.Lock()
		if c.startWait != nil {
			w := c.startWait
			c.startWait = nil
			c.starting = false
			c.mu.Unlock()
			select {
			case w.ch <- startResult{err: err}:
			default:
			}
		} else {
			c.mu.Unlock()
		}
		c.mu.Lock()
		if c.stopWait != nil {
			w := c.stopWait
			c.stopWait = nil
			c.mu.Unlock()
			select {
			case w.ch <- err:
			default:
			}
		} else {
			c.mu.Unlock()
		}
		if isAuthError(err.Code) {
			go c.handleAuthError(err)
		}
		c.pushRecv(msg)
		return
	case msg.Subscribed != nil:
		c.mu.Lock()
		if opts, ok := c.subscriptions[msg.Subscribed.DeviceUID]; ok {
			opts.handle = msg.Subscribed.Sub
			c.subByHandle[msg.Subscribed.Sub] = msg.Subscribed.DeviceUID
		}
		c.mu.Unlock()
		c.pushRecv(msg)
		return
	default:
		c.pushRecv(msg)
	}
}

func (c *Client) pushRecv(msg ServerMsg) {
	select {
	case c.recvCh <- msg:
	default:
	}
}

func (c *Client) handleAuthError(_ *Error) {
	c.mu.Lock()
	refresh := c.cfg.RefreshAuth
	c.mu.Unlock()
	if refresh == nil {
		c.mu.Lock()
		c.intentional = true
		c.clearReconnectTimerLocked()
		c.setState(StateClosed)
		if c.wsConn != nil {
			_ = c.wsConn.Close()
			c.wsConn = nil
		}
		c.send = nil
		c.mu.Unlock()
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	device, listener, rerr := refresh(ctx)
	if rerr != nil {
		c.mu.Lock()
		c.intentional = true
		c.setState(StateClosed)
		c.mu.Unlock()
		return
	}
	c.mu.Lock()
	if device != nil {
		c.cfg.Device = device
		c.cfg.Listener = nil
	}
	if listener != nil {
		c.cfg.Listener = listener
		c.cfg.Device = nil
	}
	sendResume := c.trackUID != ""
	intentional := c.intentional
	c.dialGen++
	c.clearReconnectTimerLocked()
	if c.wsConn != nil {
		_ = c.wsConn.Close()
		c.wsConn = nil
	}
	c.send = nil
	c.mu.Unlock()
	if intentional {
		return
	}
	_ = c.dial(context.Background(), sendResume)
}

func (c *Client) onSocketClosed(gen uint64) {
	c.mu.Lock()
	if gen != c.dialGen {
		c.mu.Unlock()
		return
	}
	c.wsConn = nil
	c.send = nil
	if c.intentional {
		c.setState(StateClosed)
		c.mu.Unlock()
		return
	}
	if c.cfg.DisableReconnect {
		c.setState(StateClosed)
		c.rejectPendingLocked(fmt.Errorf("tracking: connection closed"))
		c.mu.Unlock()
		return
	}
	c.scheduleReconnectLocked()
	c.mu.Unlock()
}

func (c *Client) scheduleReconnectLocked() {
	c.setState(StateReconnecting)
	delay, ok := NextDelay(&c.backoff, nil)
	if !ok {
		c.setState(StateClosed)
		c.rejectPendingLocked(fmt.Errorf("tracking: reconnect attempts exhausted"))
		return
	}
	sendResume := c.trackUID != ""
	c.clearReconnectTimerLocked()
	c.reconnectTimer = time.AfterFunc(delay, func() {
		c.mu.Lock()
		c.reconnectTimer = nil
		intentional := c.intentional
		c.mu.Unlock()
		if intentional {
			return
		}
		if err := c.dial(context.Background(), sendResume); err != nil {
			c.mu.Lock()
			if c.intentional || c.state == StateOpen {
				c.mu.Unlock()
				return
			}
			// FENCED / TRY_AGAIN: keep track_uid, retry Resume.
			c.scheduleReconnectLocked()
			c.mu.Unlock()
		}
	})
}

func (c *Client) rejectPendingLocked(err error) {
	if c.startWait != nil {
		select {
		case c.startWait.ch <- startResult{err: err}:
		default:
		}
		c.startWait = nil
		c.starting = false
	}
	if c.stopWait != nil {
		select {
		case c.stopWait.ch <- err:
		default:
		}
		c.stopWait = nil
	}
	if c.resumeWait != nil {
		select {
		case c.resumeWait.ch <- resumeResult{err: err}:
		default:
		}
		c.resumeWait = nil
	}
}

func (c *Client) clearReconnectTimerLocked() {
	if c.reconnectTimer != nil {
		c.reconnectTimer.Stop()
		c.reconnectTimer = nil
	}
}

func (c *Client) resubscribe() {
	c.mu.Lock()
	type item struct {
		uid  string
		opts subOpts
	}
	subs := make([]item, 0, len(c.subscriptions))
	for d, o := range c.subscriptions {
		subs = append(subs, item{uid: d, opts: *o})
	}
	c.subByHandle = make(map[uint8]string)
	for _, o := range c.subscriptions {
		o.handle = 0
	}
	c.mu.Unlock()
	for _, s := range subs {
		_ = c.Send(ClientMsg{Subscribe: &Subscribe{
			DeviceUID:     s.uid,
			IncludeEvents: s.opts.includeEvents,
			MinIntervalMs: s.opts.minInterval,
		}})
	}
}

func (c *Client) sendResumeAndWait(ctx context.Context) error {
	for {
		c.mu.Lock()
		uid := c.trackUID
		seq := uint32(c.clientSeq)
		if uid == "" {
			c.mu.Unlock()
			return nil
		}
		ch := make(chan resumeResult, 1)
		c.resumeWait = &pendingResume{ctx: ctx, ch: ch}
		c.mu.Unlock()

		if err := c.Send(ClientMsg{Resume: &Resume{TrackUID: uid, LastSeq: seq}}); err != nil {
			c.mu.Lock()
			c.resumeWait = nil
			c.mu.Unlock()
			return err
		}

		select {
		case <-ctx.Done():
			c.mu.Lock()
			c.resumeWait = nil
			c.mu.Unlock()
			return ctx.Err()
		case r := <-ch:
			if r.err == nil {
				return nil
			}
			if isRetryResumeError(codeOf(r.err)) {
				delay := time.Duration(0)
				if e, ok := r.err.(*Error); ok && e.RetryAfterMs > 0 {
					delay = time.Duration(e.RetryAfterMs) * time.Millisecond
				}
				if delay == 0 {
					c.mu.Lock()
					d, ok := NextDelay(&c.backoff, nil)
					c.mu.Unlock()
					if !ok {
						return r.err
					}
					delay = d
				}
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(delay):
				}
				continue
			}
			return r.err
		}
	}
}

func (c *Client) resendInFlight() {
	c.mu.Lock()
	open := c.state == StateOpen && c.send != nil && c.trackUID != ""
	pts := c.buf.PeekInFlight()
	sender := c.send
	c.mu.Unlock()
	if !open || len(pts) == 0 || sender == nil {
		return
	}
	frames := EncodeInFlightFrames(pts)
	c.mu.Lock()
	c.unackedFrames += len(frames)
	c.mu.Unlock()
	for _, f := range frames {
		_ = sender.SendRaw(f)
	}
}

func (c *Client) flushStaging() {
	c.mu.Lock()
	open := c.state == StateOpen && c.send != nil && c.trackUID != ""
	window := MaxInFlightFrames - c.unackedFrames
	assigned := c.buf.AssignFromStaging(&c.clientSeq, window)
	sender := c.send
	c.mu.Unlock()
	if !open || len(assigned) == 0 || sender == nil {
		return
	}
	frames := EncodeInFlightFrames(assigned)
	c.mu.Lock()
	c.unackedFrames += len(frames)
	c.mu.Unlock()
	for _, f := range frames {
		_ = sender.SendRaw(f)
	}
}

func (c *Client) sendAssigned(pts []InFlightPoint) {
	c.mu.Lock()
	open := c.state == StateOpen && c.send != nil
	sender := c.send
	c.mu.Unlock()
	if !open || len(pts) == 0 || sender == nil {
		return
	}
	frames := EncodeInFlightFrames(pts)
	c.mu.Lock()
	c.unackedFrames += len(frames)
	c.mu.Unlock()
	for _, f := range frames {
		_ = sender.SendRaw(f)
	}
}

// Recv returns the next server message (Loc, Subscribed, …).
// Device Ack is not delivered here. Commands go to Commands().
func (c *Client) Recv(ctx context.Context) (ServerMsg, error) {
	select {
	case <-ctx.Done():
		return ServerMsg{}, ctx.Err()
	case err := <-c.errCh:
		return ServerMsg{}, err
	case msg, ok := <-c.recvCh:
		if !ok {
			return ServerMsg{}, io.EOF
		}
		return msg, nil
	}
}

// Commands is a stream of server→device Command injects.
func (c *Client) Commands() <-chan Command {
	return c.cmdCh
}

// AckCommand acknowledges a received Command.
func (c *Client) AckCommand(commandID string, status CommandAckStatus, message string) error {
	return c.Send(ClientMsg{CommandAck: &CommandAck{
		CommandID: commandID,
		Status:    status,
		Message:   message,
	}})
}

// Send writes a client message on the session.
func (c *Client) Send(msg ClientMsg) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.state == StateClosed && c.intentional {
		return fmt.Errorf("tracking: closed")
	}
	if c.send == nil {
		return fmt.Errorf("tracking: socket not open")
	}
	return c.send.Send(msg)
}

// StartTrack sends TrackStart and waits for TrackStarted (or error).
func (c *Client) StartTrack(ctx context.Context, loc *LatLng, route []LatLng) (string, error) {
	return c.StartTrackMeta(ctx, loc, route, nil)
}

// StartTrackMeta is StartTrack with opaque metadata (≤4 KiB).
func (c *Client) StartTrackMeta(ctx context.Context, loc *LatLng, route []LatLng, metadata []byte) (string, error) {
	ch := make(chan startResult, 1)
	c.mu.Lock()
	c.buf.Clear()
	c.filter.Reset()
	c.clientSeq = 0
	c.lastAckedSeq = 0
	c.starting = true
	c.startWait = &pendingStart{ctx: ctx, ch: ch}
	c.mu.Unlock()

	if err := c.Send(ClientMsg{TrackStart: &TrackStart{
		Location: loc,
		Route:    route,
		Metadata: metadata,
	}}); err != nil {
		c.mu.Lock()
		c.startWait = nil
		c.starting = false
		c.mu.Unlock()
		return "", err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.startWait = nil
		c.starting = false
		c.mu.Unlock()
		return "", ctx.Err()
	case r := <-ch:
		return r.uid, r.err
	}
}

// Resume sends resume and waits for ResumeOk (manual; auto-reconnect also resumes).
func (c *Client) Resume(ctx context.Context, trackUID string, lastClientSeq uint64) (uint64, error) {
	c.mu.Lock()
	c.trackUID = trackUID
	c.clientSeq = lastClientSeq
	c.mu.Unlock()
	if err := c.sendResumeAndWait(ctx); err != nil {
		return 0, err
	}
	return c.LastAckedSeq(), nil
}

// Publish filters a GPS sample and either sends Loc or stages it.
// If there is no live track, the first call sends TrackStart (this point is the start location).
func (c *Client) Publish(point *LatLng) (seq uint64, accepted bool) {
	if point == nil {
		return 0, false
	}
	c.mu.Lock()
	if c.trackUID == "" && !c.starting {
		c.starting = true
		c.buf.Clear()
		c.filter.Reset()
		c.clientSeq = 0
		c.lastAckedSeq = 0
		loc := *point
		c.mu.Unlock()
		if err := c.Send(ClientMsg{TrackStart: &TrackStart{Location: &loc}}); err != nil {
			c.mu.Lock()
			c.starting = false
			c.mu.Unlock()
			return 0, false
		}
		return 0, true
	}
	now := time.Now()
	emitted, ok := c.filter.Push(*point, now)
	if !ok {
		seq = c.clientSeq
		c.mu.Unlock()
		return seq, false
	}
	open := c.state == StateOpen && c.send != nil && c.trackUID != ""
	windowOK := c.unackedFrames < MaxInFlightFrames
	if !open || !windowOK {
		StampLatLng(&emitted)
		c.buf.PushStaging(emitted)
		seq = c.clientSeq
		c.mu.Unlock()
		return seq, true
	}
	if !CanAcceptPublish(c.nextPublishAt, now, 1) {
		seq = c.clientSeq
		c.mu.Unlock()
		return seq, false
	}
	c.nextPublishAt = NextPublishAllowedAt(c.nextPublishAt, now, 1)
	c.clientSeq++
	seq = c.clientSeq
	item := InFlightPoint{Seq: uint32(seq), Point: emitted}
	c.buf.inFlight = append(c.buf.inFlight, item)
	c.buf.enforceCap()
	c.mu.Unlock()

	c.sendAssigned([]InFlightPoint{item})
	return seq, true
}

// StopTrack sends TrackStop and waits for TrackStopped (or error).
// No active track is a no-op.
func (c *Client) StopTrack(ctx context.Context, trackUID string) error {
	if trackUID == "" {
		trackUID = c.TrackUID()
	}
	if trackUID == "" {
		return nil
	}
	ch := make(chan error, 1)
	c.mu.Lock()
	c.stopWait = &pendingStop{ctx: ctx, ch: ch}
	c.mu.Unlock()

	if err := c.Send(ClientMsg{TrackStop: &TrackStop{}}); err != nil {
		c.mu.Lock()
		c.stopWait = nil
		c.mu.Unlock()
		return err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.stopWait = nil
		c.mu.Unlock()
		return ctx.Err()
	case err := <-ch:
		return err
	}
}

// SendEvent fans out an opaque payload (≤4 KiB, ≤1 Hz) on the active track.
func (c *Client) SendEvent(payload []byte) (bool, error) {
	if len(payload) > MaxEventBytes {
		return false, NewError(ErrorInvalid, "event payload exceeds 4 KiB")
	}
	c.mu.Lock()
	if c.trackUID == "" {
		c.mu.Unlock()
		return false, NewError(ErrorInvalid, "startTrack() before sendEvent()")
	}
	now := time.Now()
	if !c.nextEventAt.IsZero() && now.Before(c.nextEventAt) {
		c.mu.Unlock()
		return false, nil
	}
	c.nextEventAt = now.Add(MinEventInterval)
	open := c.state == StateOpen && c.send != nil
	c.mu.Unlock()

	if !open {
		return true, nil
	}
	err := c.Send(ClientMsg{Event: &Event{Payload: payload, TimestampMs: now.UnixMilli()}})
	return err == nil, err
}

// Subscribe sends subscribe for a device (include_events on, no extra throttle).
func (c *Client) Subscribe(deviceUID string) error {
	return c.SubscribeFilter(deviceUID, true, 0)
}

// SubscribeFilter is Subscribe with include_events and min_interval_ms.
func (c *Client) SubscribeFilter(deviceUID string, includeEvents bool, minIntervalMs uint16) error {
	c.mu.Lock()
	if existing, ok := c.subscriptions[deviceUID]; ok {
		existing.includeEvents = includeEvents
		existing.minInterval = minIntervalMs
	} else {
		c.subscriptions[deviceUID] = &subOpts{includeEvents: includeEvents, minInterval: minIntervalMs}
	}
	c.mu.Unlock()
	return c.Send(ClientMsg{Subscribe: &Subscribe{
		DeviceUID:     deviceUID,
		IncludeEvents: includeEvents,
		MinIntervalMs: minIntervalMs,
	}})
}

// Unsubscribe drops a listener handle (u8 from Subscribed). Unknown sub is a no-op on the server.
func (c *Client) Unsubscribe(sub uint8) error {
	c.mu.Lock()
	if uid, ok := c.subByHandle[sub]; ok {
		delete(c.subByHandle, sub)
		delete(c.subscriptions, uid)
	}
	c.mu.Unlock()
	return c.Send(ClientMsg{Unsubscribe: &Unsubscribe{Sub: sub}})
}

// Close sends TrackStop if a track is live, then hangs up. The SDK will not Resume afterwards.
func (c *Client) Close() error {
	uid := c.TrackUID()
	if uid != "" {
		_ = c.Send(ClientMsg{TrackStop: &TrackStop{}})
	}
	c.mu.Lock()
	c.intentional = true
	c.clearReconnectTimerLocked()
	c.setState(StateClosed)
	c.rejectPendingLocked(NewError(ErrorInvalid, "client closed"))
	send := c.send
	c.send = nil
	c.wsConn = nil
	c.mu.Unlock()
	if send != nil {
		return send.Close()
	}
	return nil
}
