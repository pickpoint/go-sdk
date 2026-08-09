package tracking

import (
	"context"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/gorilla/websocket"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/proto"

	pb "github.com/pickpoint/go-sdk/tracking/v2"
)

const Subprotocol = "tracking.v2.proto"

// MaxPublishHz is the hard cap for Publish calls (points per second).
const MaxPublishHz = 50

// MinPublishInterval is the minimum gap between accepted points.
const MinPublishInterval = time.Second / MaxPublishHz

// MaxEventBytes / MaxEventHz bound opaque custom events (ephemeral fan-out).
const MaxEventBytes = 4 * 1024
const MaxEventHz = 1
const MinEventInterval = time.Second / MaxEventHz

// Transport selects the edge protocol.
type Transport int

const (
	// TransportWS is the default: binary protobuf on /v2/tracking/ws (lowest overhead).
	TransportWS Transport = iota
	// TransportGRPC is optional for mesh/agents that already speak gRPC.
	TransportGRPC
)

// ConnectionState mirrors the JS tracking client state machine.
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

// Config opens a session against a Pickpoint tracking endpoint.
type Config struct {
	// Endpoint:
	//   WS:   "ws://host:3100", "wss://…", or "host:3100" (→ ws://…/v2/tracking/ws)
	//   gRPC: "host:3101"
	Endpoint  string
	Transport Transport
	Device    *DeviceAuth
	Listener  *ListenerAuth
	// Path for WS (default /v2/tracking/ws).
	WSPath string
	// DialOptions optional extras for gRPC.
	DialOptions []grpc.DialOption

	// DisableReconnect turns off auto-reconnect (WS only; default enabled).
	DisableReconnect bool
	// Reconnect backoff (defaults: 500ms … 30s, unlimited attempts).
	ReconnectMinDelay    time.Duration
	ReconnectMaxDelay    time.Duration
	ReconnectMaxAttempts int

	// RefreshAuth is called on AUTH / UNAUTHORIZED before giving up (WS).
	RefreshAuth RefreshAuthFunc

	// MaxQueueSize bounds offline points for resume flush (default 10_000).
	MaxQueueSize int
	// HelloTimeout waits for Hello after dial (default 10s).
	HelloTimeout time.Duration
}

type sender interface {
	Send(*pb.ClientMsg) error
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
	acked uint64
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
	queue         *OfflineQueue
	backoff       BackoffState
	nextPublishAt time.Time
	nextEventAt   time.Time
	subscriptions map[string]struct{}
	intentional   bool
	dialGen       uint64
	wsConn        *websocket.Conn

	recvCh chan *pb.ServerMsg
	cmdCh  chan *pb.Command
	errCh  chan error

	startWait  *pendingStart
	stopWait   *pendingStop
	resumeWait *pendingResume

	reconnectTimer *time.Timer
}

// Connect opens a tracking session (WS binary protobuf by default).
// For WS, waits for Hello (or follows Relocate) before returning.
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
		recvCh:        make(chan *pb.ServerMsg, 64),
		cmdCh:         make(chan *pb.Command, 16),
		errCh:         make(chan error, 1),
		subscriptions: make(map[string]struct{}),
		backoff:       NewBackoff(cfg.ReconnectMinDelay, cfg.ReconnectMaxDelay, cfg.ReconnectMaxAttempts),
		queue:         NewOfflineQueue(cfg.MaxQueueSize, nil),
	}

	switch cfg.Transport {
	case TransportGRPC:
		if err := c.connectGRPC(ctx); err != nil {
			return nil, err
		}
		c.setState(StateOpen)
		return c, nil
	default:
		if err := c.dial(ctx, false); err != nil {
			return nil, err
		}
		return c, nil
	}
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
	c.mu.Unlock()

	helloCtx, cancel := context.WithTimeout(ctx, c.cfg.HelloTimeout)
	defer cancel()

	msg, err := readOneWS(helloCtx, conn)
	if err != nil {
		_ = conn.Close()
		return err
	}

	switch b := msg.Body.(type) {
	case *pb.ServerMsg_Hello:
		// ok
	case *pb.ServerMsg_Relocate:
		_ = conn.Close()
		return c.handleRelocate(ctx, b.Relocate, sendResume)
	case *pb.ServerMsg_Error:
		_ = conn.Close()
		return errorFromWire(b.Error)
	default:
		_ = conn.Close()
		return fmt.Errorf("tracking: expected hello, got %T", msg.Body)
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
			return err
		}
	}
	c.resubscribe()
	return nil
}

func readOneWS(ctx context.Context, conn *websocket.Conn) (*pb.ServerMsg, error) {
	type result struct {
		msg *pb.ServerMsg
		err error
	}
	ch := make(chan result, 1)
	go func() {
		_ = conn.SetReadDeadline(time.Now().Add(30 * time.Second))
		_, data, err := conn.ReadMessage()
		_ = conn.SetReadDeadline(time.Time{})
		if err != nil {
			ch <- result{err: err}
			return
		}
		var msg pb.ServerMsg
		if err := proto.Unmarshal(data, &msg); err != nil {
			ch <- result{err: err}
			return
		}
		ch <- result{msg: &msg}
	}()
	select {
	case <-ctx.Done():
		_ = conn.Close()
		return nil, fmt.Errorf("tracking: hello timeout: %w", ctx.Err())
	case r := <-ch:
		return r.msg, r.err
	}
}

func (c *Client) handleRelocate(ctx context.Context, rel *pb.Relocate, sendResume bool) error {
	if rel.GetEndpoint() != "" {
		c.mu.Lock()
		c.cfg.Endpoint = rel.GetEndpoint()
		c.mu.Unlock()
	}
	delay := time.Duration(rel.GetRetryAfterMs()) * time.Millisecond
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

func (c *Client) connectGRPC(ctx context.Context) error {
	opts := append([]grpc.DialOption{grpc.WithTransportCredentials(insecure.NewCredentials())}, c.cfg.DialOptions...)
	conn, err := grpc.NewClient(c.cfg.Endpoint, opts...)
	if err != nil {
		return fmt.Errorf("tracking: dial: %w", err)
	}

	md := metadata.MD{}
	if c.cfg.Device != nil {
		md.Set("x-client-id", c.cfg.Device.ClientID)
		md.Set("x-client-secret", c.cfg.Device.ClientSecret)
	} else {
		md.Set("authorization", "Bearer "+c.cfg.Listener.AccessToken)
	}
	ctx = metadata.NewOutgoingContext(ctx, md)

	stream, err := pb.NewTrackingClient(conn).Session(ctx)
	if err != nil {
		_ = conn.Close()
		return fmt.Errorf("tracking: session: %w", err)
	}

	c.mu.Lock()
	c.send = &grpcSender{conn: conn, stream: stream}
	c.mu.Unlock()
	go c.readLoopGRPC(stream)
	return nil
}

type wsSender struct {
	conn *websocket.Conn
	mu   sync.Mutex
}

func (w *wsSender) Send(msg *pb.ClientMsg) error {
	b, err := proto.Marshal(msg)
	if err != nil {
		return err
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.conn.WriteMessage(websocket.BinaryMessage, b)
}

func (w *wsSender) Close() error {
	return w.conn.Close()
}

type grpcSender struct {
	conn   *grpc.ClientConn
	stream grpc.BidiStreamingClient[pb.ClientMsg, pb.ServerMsg]
}

func (g *grpcSender) Send(msg *pb.ClientMsg) error {
	return g.stream.Send(msg)
}

func (g *grpcSender) Close() error {
	_ = g.stream.CloseSend()
	return g.conn.Close()
}

func (c *Client) readLoopWS(conn *websocket.Conn, gen uint64) {
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			c.onSocketClosed(gen)
			return
		}
		var msg pb.ServerMsg
		if err := proto.Unmarshal(data, &msg); err != nil {
			continue
		}
		c.dispatch(&msg)
	}
}

func (c *Client) readLoopGRPC(stream grpc.BidiStreamingClient[pb.ClientMsg, pb.ServerMsg]) {
	defer close(c.recvCh)
	defer close(c.cmdCh)
	for {
		msg, err := stream.Recv()
		if err != nil {
			if err != io.EOF && !c.isClosed() {
				select {
				case c.errCh <- err:
				default:
				}
			}
			return
		}
		c.dispatch(msg)
	}
}

func (c *Client) dispatch(msg *pb.ServerMsg) {
	switch b := msg.Body.(type) {
	case *pb.ServerMsg_Relocate:
		go func() {
			_ = c.handleRelocate(context.Background(), b.Relocate, true)
		}()
		return
	case *pb.ServerMsg_ResumeOk:
		c.mu.Lock()
		if uid := b.ResumeOk.GetTrackUid(); uid != "" {
			c.trackUID = uid
		}
		c.lastAckedSeq = b.ResumeOk.GetLastAckedSeq()
		if c.clientSeq < c.lastAckedSeq {
			c.clientSeq = c.lastAckedSeq
		}
		c.queue.AckThrough(c.lastAckedSeq)
		wait := c.resumeWait
		c.resumeWait = nil
		c.mu.Unlock()
		c.flushQueue()
		if wait != nil {
			select {
			case wait.ch <- resumeResult{acked: b.ResumeOk.GetLastAckedSeq()}:
			default:
			}
		}
		c.pushRecv(msg)
		return
	case *pb.ServerMsg_TrackStarted:
		c.mu.Lock()
		c.trackUID = b.TrackStarted.GetTrackUid()
		c.clientSeq = 0
		c.lastAckedSeq = 0
		c.queue.Clear()
		wait := c.startWait
		c.startWait = nil
		c.mu.Unlock()
		if wait != nil {
			select {
			case wait.ch <- startResult{uid: b.TrackStarted.GetTrackUid()}:
			default:
			}
		}
		c.pushRecv(msg)
		return
	case *pb.ServerMsg_TrackStopped:
		c.mu.Lock()
		if c.trackUID == b.TrackStopped.GetTrackUid() {
			c.trackUID = ""
			c.queue.Clear()
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
	case *pb.ServerMsg_LocationAdded:
		c.mu.Lock()
		if b.LocationAdded.GetClientSeq() > c.lastAckedSeq {
			c.lastAckedSeq = b.LocationAdded.GetClientSeq()
		}
		c.queue.AckThrough(b.LocationAdded.GetClientSeq())
		c.mu.Unlock()
		c.pushRecv(msg)
		return
	case *pb.ServerMsg_Command:
		select {
		case c.cmdCh <- b.Command:
		default:
		}
		return
	case *pb.ServerMsg_Error:
		err := errorFromWire(b.Error)
		c.mu.Lock()
		if c.resumeWait != nil {
			w := c.resumeWait
			c.resumeWait = nil
			if isFatalResumeError(err.Code) {
				c.trackUID = ""
				c.queue.Clear()
			}
			c.mu.Unlock()
			select {
			case w.ch <- resumeResult{err: err}:
			default:
			}
		} else {
			c.mu.Unlock()
		}
		c.mu.Lock()
		if c.startWait != nil {
			w := c.startWait
			c.startWait = nil
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
	default:
		c.pushRecv(msg)
	}
}

func (c *Client) pushRecv(msg *pb.ServerMsg) {
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
	// Invalidate the rejected socket's readLoop so it won't schedule reconnect.
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
	if c.cfg.DisableReconnect || c.cfg.Transport == TransportGRPC {
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

func (c *Client) isClosed() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.state == StateClosed && c.intentional
}

func (c *Client) resubscribe() {
	c.mu.Lock()
	subs := make([]string, 0, len(c.subscriptions))
	for d := range c.subscriptions {
		subs = append(subs, d)
	}
	c.mu.Unlock()
	for _, d := range subs {
		_ = c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_Subscribe{Subscribe: &pb.Subscribe{DeviceUid: d}}})
	}
}

func (c *Client) sendResumeAndWait(ctx context.Context) error {
	c.mu.Lock()
	uid := c.trackUID
	seq := c.clientSeq
	if uid == "" {
		c.mu.Unlock()
		return nil
	}
	ch := make(chan resumeResult, 1)
	c.resumeWait = &pendingResume{ctx: ctx, ch: ch}
	c.mu.Unlock()

	if err := c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_Resume{Resume: &pb.Resume{
		TrackUid: uid, LastClientSeq: seq,
	}}}); err != nil {
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
		return r.err
	}
}

func (c *Client) flushQueue() {
	c.mu.Lock()
	uid := c.trackUID
	pending := c.queue.PeekAll()
	open := c.state == StateOpen && c.send != nil
	c.mu.Unlock()
	if uid == "" || !open || len(pending) == 0 {
		return
	}
	points := make([]*pb.LatLng, len(pending))
	for i, p := range pending {
		points[i] = p.Point
	}
	last := pending[len(pending)-1].Seq
	_ = c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_LocationBatch{LocationBatch: &pb.LocationBatch{
		TrackUid: uid, ClientSeq: last, Points: stampLatLngs(points),
	}}})
}

// Recv returns the next server message (LocationAdded, Subscribed, …).
// Device Commands are delivered on Commands(), not here.
func (c *Client) Recv(ctx context.Context) (*pb.ServerMsg, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case err := <-c.errCh:
		return nil, err
	case msg, ok := <-c.recvCh:
		if !ok {
			return nil, io.EOF
		}
		return msg, nil
	}
}

// Commands is a stream of server→device Command injects (HTTP API).
func (c *Client) Commands() <-chan *pb.Command {
	return c.cmdCh
}

// AckCommand acknowledges a received Command.
func (c *Client) AckCommand(commandID string, status pb.CommandAckStatus, message string) error {
	var msg *string
	if message != "" {
		msg = &message
	}
	return c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_CommandAck{CommandAck: &pb.CommandAck{
		CommandId: commandID,
		Status:    status,
		Message:   msg,
	}}})
}

// Send writes a client message on the session.
func (c *Client) Send(msg *pb.ClientMsg) error {
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

// StartTrack sends track_start and waits for track_started (or error).
func (c *Client) StartTrack(ctx context.Context, loc *pb.LatLng, route []*pb.LatLng) (string, error) {
	return c.StartTrackMeta(ctx, loc, route, nil)
}

// StartTrackMeta is StartTrack with opaque metadata (≤4 KiB).
func (c *Client) StartTrackMeta(ctx context.Context, loc *pb.LatLng, route []*pb.LatLng, metadata []byte) (string, error) {
	ch := make(chan startResult, 1)
	c.mu.Lock()
	c.startWait = &pendingStart{ctx: ctx, ch: ch}
	c.mu.Unlock()

	if err := c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_TrackStart{TrackStart: &pb.TrackStart{
		Location: stampLatLng(loc),
		Route:    stampLatLngs(route),
		Metadata: metadata,
	}}}); err != nil {
		c.mu.Lock()
		c.startWait = nil
		c.mu.Unlock()
		return "", err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.startWait = nil
		c.mu.Unlock()
		return "", ctx.Err()
	case r := <-ch:
		return r.uid, r.err
	}
}

// Resume sends resume and waits for resume_ok (manual; auto-reconnect also resumes).
func (c *Client) Resume(ctx context.Context, trackUID string, lastClientSeq uint64) (uint64, error) {
	c.mu.Lock()
	c.trackUID = trackUID
	c.clientSeq = lastClientSeq
	ch := make(chan resumeResult, 1)
	c.resumeWait = &pendingResume{ctx: ctx, ch: ch}
	c.mu.Unlock()

	if err := c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_Resume{Resume: &pb.Resume{
		TrackUid: trackUID, LastClientSeq: lastClientSeq,
	}}}); err != nil {
		c.mu.Lock()
		c.resumeWait = nil
		c.mu.Unlock()
		return 0, err
	}

	select {
	case <-ctx.Done():
		c.mu.Lock()
		c.resumeWait = nil
		c.mu.Unlock()
		return 0, ctx.Err()
	case r := <-ch:
		return r.acked, r.err
	}
}

// Publish sends a location_add on the active track (managed clientSeq).
// When over MaxPublishHz, returns (currentSeq, false).
func (c *Client) Publish(point *pb.LatLng) (seq uint64, accepted bool) {
	c.mu.Lock()
	if c.trackUID == "" {
		c.mu.Unlock()
		return 0, false
	}
	now := time.Now()
	if !CanAcceptPublish(c.nextPublishAt, now, 1) {
		seq = c.clientSeq
		c.mu.Unlock()
		return seq, false
	}
	c.nextPublishAt = NextPublishAllowedAt(c.nextPublishAt, now, 1)
	c.clientSeq++
	seq = c.clientSeq
	uid := c.trackUID
	pt := stampLatLng(cloneLatLng(point))
	c.queue.Enqueue(seq, pt)
	open := c.state == StateOpen && c.send != nil
	c.mu.Unlock()

	if open {
		_ = c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_LocationAdd{LocationAdd: &pb.LocationAdd{
			TrackUid: uid, ClientSeq: seq, Point: pt,
		}}})
	}
	return seq, true
}

func cloneLatLng(p *pb.LatLng) *pb.LatLng {
	if p == nil {
		return nil
	}
	cp := proto.Clone(p).(*pb.LatLng)
	return cp
}

// StopTrack sends track_stop and waits for track_stopped (or error).
func (c *Client) StopTrack(ctx context.Context, trackUID string) error {
	if trackUID == "" {
		trackUID = c.TrackUID()
	}
	if trackUID == "" {
		return NewError(pb.ErrorCode_ERROR_CODE_INVALID, "no active track")
	}
	ch := make(chan error, 1)
	c.mu.Lock()
	c.stopWait = &pendingStop{ctx: ctx, ch: ch}
	c.mu.Unlock()

	if err := c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_TrackStop{TrackStop: &pb.TrackStop{
		TrackUid: trackUID,
	}}}); err != nil {
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
		return false, NewError(pb.ErrorCode_ERROR_CODE_INVALID, "event payload exceeds 4 KiB")
	}
	c.mu.Lock()
	uid := c.trackUID
	if uid == "" {
		c.mu.Unlock()
		return false, NewError(pb.ErrorCode_ERROR_CODE_INVALID, "startTrack() before sendEvent()")
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
	ts := now.UnixMilli()
	err := c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_Event{Event: &pb.Event{
		TrackUid: uid, Payload: payload, TimestampMs: &ts,
	}}})
	return err == nil, err
}

// Subscribe sends subscribe for a device.
func (c *Client) Subscribe(deviceUID string) error {
	c.mu.Lock()
	c.subscriptions[deviceUID] = struct{}{}
	c.mu.Unlock()
	return c.Send(&pb.ClientMsg{Body: &pb.ClientMsg_Subscribe{Subscribe: &pb.Subscribe{
		DeviceUid: deviceUID,
	}}})
}

// Close ends the session.
func (c *Client) Close() error {
	c.mu.Lock()
	c.intentional = true
	c.clearReconnectTimerLocked()
	c.setState(StateClosed)
	c.rejectPendingLocked(NewError(pb.ErrorCode_ERROR_CODE_INVALID, "client closed"))
	send := c.send
	c.send = nil
	c.wsConn = nil
	c.mu.Unlock()
	if send != nil {
		return send.Close()
	}
	return nil
}
