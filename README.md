# go-sdk

Official Go SDK for [Pickpoint](https://pickpoint.io) — a geolocation platform with four APIs under one key:

| API | What it does |
|-----|----------------|
| **Geocoding** | Address ↔ coordinates (forward, reverse, place lookup) |
| **Address search** | Typeahead / autocomplete for address inputs |
| **Routing** | Routes, matrices, optimized multi-stop, elevation |
| **Device tracking** | Register devices over HTTP; stream live GPS over WebSocket / gRPC |

Built for maps, delivery, logistics, and anything that needs places, routes, or live location. Data is OpenStreetMap-backed; HTTP responses are plain JSON / GeoJSON. Docs: [pickpoint.io/docs](https://pickpoint.io/docs).

**This module** is the idiomatic Go client for that platform:

| Package | Import | Role |
|---------|--------|------|
| [`pickpoint`](#public-api--pickpoint) | `github.com/pickpoint/go-sdk/pickpoint` | HTTP: geocode, search, routing, devices, client-tokens |
| [`tracking`](#tracking) | `github.com/pickpoint/go-sdk/tracking` | Realtime tracks (WebSocket by default, optional gRPC) |
| `tracking/v2` | `github.com/pickpoint/go-sdk/tracking/v2` | Generated protobuf (`tracking.v2`) |

Apache-2.0. JS sibling: [`@pickpoint/sdk`](https://github.com/pickpoint/pickpoint-js). Wire schema: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).

```bash
go get github.com/pickpoint/go-sdk@latest
```

```
go-sdk/
  pickpoint/       # HTTP client
  tracking/        # tracking session client
  tracking/v2/     # protobuf stubs
```

---

## Public API — `pickpoint`

One `Client`, one auth session, whole public HTTP surface (same idea as JS `PickPoint`).

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func main() {
	ctx := context.Background()

	pp, err := pickpoint.New(pickpoint.Config{
		APIKey: os.Getenv("PICKPOINT_API_KEY"),
	})
	if err != nil {
		log.Fatal(err)
	}

	// Flat shortcuts
	places, err := pp.Forward(ctx, pickpoint.Query{"q": "Berlin", "limit": "5"})
	if err != nil {
		log.Fatal(err)
	}
	fmt.Println(places)

	rev, err := pp.Reverse(ctx, pickpoint.Query{"lat": "52.52", "lon": "13.405"})
	_, _ = pp.Search(ctx, pickpoint.Query{"q": "Alexanderplatz"})
	_, _ = pp.Route(ctx, map[string]any{
		"locations": []map[string]any{
			{"lat": 52.52, "lon": 13.40},
			{"lat": 52.53, "lon": 13.42},
		},
		"costing": "auto",
	})

	// Namespaced (clearer for devices / routing)
	list, err := pp.Devices.List(ctx, pickpoint.DeviceListQuery{Take: 25})
	_ = list
	_ = pp.Geocoding.Lookup
	_ = pp.Routing.Matrix
}
```

### API map

| Method | HTTP | Notes |
|--------|------|--------|
| `Forward` / `Geocoding.Forward` | `GET /v2/geocode/forward` | Nominatim-style; returns `[]any` |
| `Reverse` / `Geocoding.Reverse` | `GET /v2/geocode/reverse` | `map` or `nil` if no hit |
| `Lookup` / `Geocoding.Lookup` | `GET /v2/address/lookup` | e.g. `osm_ids` |
| `ForwardBatch` / `ReverseBatch` / `LookupBatch` | same | Geocoding **only**; conveyor ≤20 in flight |
| `Search` / `Address.Search` | `GET /v2/address/search` | Photon autocomplete |
| `Route` / `OptimizedRoute` / `Matrix` / `Locate` / `Elevation` | `POST /v2/route…` | Valhalla JSON body → `json.RawMessage` |
| `Devices.List` / `Get` / `Create` / `Update` / `Delete` | `/v2/devices` | Typed structs |
| `Devices.Command` | `POST …/command` | Payload `[]byte` (SDK base64-encodes) |
| `MintClientTokens` | `POST /v2/client-tokens` | Package helper; needs secret `APIKey` |

Query params for geocode/address are `pickpoint.Query` (`map[string]string`) — pass whatever the public API accepts (`q`, `lat`, `lon`, `limit`, `accept-language`, …).

### Auth

Provide **exactly one** of:

| Field | Header | Use |
|-------|--------|-----|
| `APIKey` | `x-api-key` | Backends, workers, CLIs |
| `ClientAuth` | `Authorization: Bearer` | Short-lived pair; auto-refresh |
| `AccessToken` | `Authorization: Bearer` | Static token, no refresh |

Keep the secret API key on the server. For client apps (mobile, desktop, SPA, …) mint **client-tokens** and pass `ClientAuth` — or return the pair to the browser SDK.

```go
// After your own session check
pair, err := pickpoint.MintClientTokens(ctx, pickpoint.Config{
	APIKey: os.Getenv("PICKPOINT_API_KEY"),
}, []string{"geocoding", "address", "routing", "devices"}, 600)
// scopes nil/empty → all client-tokenable permissions on the key

pp, err := pickpoint.New(pickpoint.Config{
	ClientAuth: &pickpoint.ClientAuth{
		AccessToken:  pair.AccessToken,
		RefreshToken: pair.RefreshToken,
		ExpiresAt:    pair.ExpiresAt, // unix ms
	},
})
```

Refresh behavior (same as JS):

1. Proactive refresh at **~50% of access TTL** (single-flight).
2. On HTTP **401**, one refresh + retry.
3. If refresh fails → `errors.Is(err, pickpoint.ErrAuth)`.

| Scope on mint | Unlocks |
|---------------|---------|
| `geocoding` | Forward / Reverse / Lookup (+ batches) |
| `address` | Search |
| `routing` | Route family |
| `devices` | `Devices.*` |
| — | `/v2/api-keys*` is **not** available with client tokens |

### Config

```go
pp, err := pickpoint.New(pickpoint.Config{
	APIKey:      os.Getenv("PICKPOINT_API_KEY"),
	BaseURL:     "https://api.pickpoint.io", // default
	Timeout:     30 * time.Second,           // per attempt; default 30s
	MaxRetries:  3,                          // 5xx / network; default 3
	RetryBase:   time.Second,                // full-jitter backoff; min 200ms
	Concurrency: 20,                         // geocode batch workers; max 20
	HTTPClient:  nil,                        // optional custom *http.Client
})
```

| Constant | Value |
|----------|--------|
| `DefaultBaseURL` | `https://api.pickpoint.io` |
| `DefaultTimeout` | 30s |
| `DefaultMaxRetries` | 3 |
| `DefaultRetryBase` | 1s (`MinRetryBase` = 200ms) |
| `MaxConcurrency` | 20 |

### Batch geocoding

Only geocoding supports batch. A worker-pool conveyor keeps up to `Concurrency` (default/max **20**) requests in flight — when one finishes, the next starts immediately (not wave chunks of 20). First fatal error cancels the rest.

```go
out, err := pp.ForwardBatch(ctx, []pickpoint.Query{
	{"q": "Paris"},
	{"q": "Rome"},
	{"q": "Berlin"},
})
// out[i] aligns with input[i]; soft 4xx → empty slice in that slot
```

| Response | Geocode slot | Address / routing / devices |
|----------|--------------|-----------------------------|
| `2xx` | parsed JSON | parsed / typed |
| `400` / other `4xx` (except auth) | empty (`[]` / `nil`) | `*APIError` |
| `401` | refresh once, else error | same |
| `402` / `403` | `ErrAuth` | same |
| `409` | — | `ErrConflict` (e.g. device offline) |
| `404` | empty (geocode) | `ErrNotFound` |
| `≥500` / network | retry, then error | same |

### Errors

```go
places, err := pp.Devices.Get(ctx, uid)
if err != nil {
	switch {
	case errors.Is(err, pickpoint.ErrNotFound):
		// 404
	case errors.Is(err, pickpoint.ErrAuth):
		// 401 / 402 / 403 / refresh failed
	case errors.Is(err, pickpoint.ErrConflict):
		// 409
	case errors.Is(err, pickpoint.ErrInvalidConfig):
		// bad New() / Mint args
	default:
		var api *pickpoint.APIError
		if errors.As(err, &api) {
			log.Printf("status=%d code=%s body=%s", api.Status, api.Code, api.Body)
		}
	}
}
```

### Devices example

```go
dev, err := pp.Devices.Create(ctx, pickpoint.DeviceInput{
	Name: "Van 12",
	Type: "car",
})
_, err = pp.Devices.Update(ctx, dev.UID, pickpoint.DeviceInput{
	Name: "Van 12",
	Type: "car",
})
_, err = pp.Devices.Command(ctx, dev.UID, []byte(`{"action":"ping"}`))
err = pp.Devices.Delete(ctx, dev.UID)
```

---

## Tracking

Realtime publisher / listener over **binary WebSocket** (`tracking.v2.proto` subprotocol). Optional **gRPC** for mesh/agents (`TransportGRPC`).

```go
import (
	"context"

	"github.com/pickpoint/go-sdk/tracking"
	pb "github.com/pickpoint/go-sdk/tracking/v2"
)

ctx := context.Background()

client, err := tracking.Connect(ctx, tracking.Config{
	Endpoint: "wss://tracking.pickpoint.io", // local: "ws://127.0.0.1:3100"
	Device: &tracking.DeviceAuth{
		ClientID:     deviceUID,
		ClientSecret: deviceSecret,
	},
})
if err != nil {
	log.Fatal(err)
}
defer client.Close()

trackUID, err := client.StartTrack(ctx, &pb.LatLng{
	Latitude: 55.75, Longitude: 37.61,
}, nil)
_ = trackUID

seq, ok := client.Publish(&pb.LatLng{
	Latitude: 55.76, Longitude: 37.62,
})
_ = seq // managed clientSeq; ok=false if rate-limited locally

_ = client.StopTrack(ctx, "")
```

### Auth modes

| Config | Role |
|--------|------|
| `Device: &DeviceAuth{ClientID, ClientSecret}` | Publisher (device) |
| `Listener: &ListenerAuth{AccessToken}` | Dashboard / subscriber JWT |

Exactly one of `Device` / `Listener` is required.

### Config

| Field | Meaning |
|-------|---------|
| `Endpoint` | WS: `ws(s)://host`, or `host:port`. gRPC: `host:port` |
| `Transport` | `TransportWS` (default) or `TransportGRPC` |
| `WSPath` | Default `/v2/tracking/ws` |
| `DialOptions` | Extra gRPC dial options |
| `DisableReconnect` | Opt out of WS auto-reconnect (default: reconnect on) |
| `ReconnectMinDelay` / `MaxDelay` / `MaxAttempts` | Full-jitter backoff |
| `RefreshAuth` | Fresh creds on AUTH / UNAUTHORIZED |
| `MaxQueueSize` | Offline points kept for resume flush (default 10_000) |

### Main methods

| Method | Purpose |
|--------|---------|
| `StartTrack` / `StartTrackMeta` | Open a track; returns `trackUID` |
| `Publish` | Point on active track (managed `clientSeq`); capped at **50 Hz** |
| `Resume` | Manual resume (`trackUID`, `lastClientSeq`); auto-reconnect also resumes |
| `StopTrack` | End track |
| `SendEvent` | Opaque event ≤4 KiB; capped at **1 Hz** |
| `Subscribe` | Listener: subscribe to a device UID |
| `Recv` | Next `*pb.ServerMsg` (blocking with ctx) |
| `Commands` | Channel of inbound `*pb.Command` |
| `AckCommand` | ACK a command |
| `State` / `TrackUID` / `ClientSeq` | Session cursors |
| `Close` | Tear down session |

```go
// Listener sketch
client, err := tracking.Connect(ctx, tracking.Config{
	Endpoint: "wss://tracking.pickpoint.io",
	Listener: &tracking.ListenerAuth{AccessToken: jwt},
})
_ = client.Subscribe(deviceUID)

for {
	msg, err := client.Recv(ctx)
	if err != nil {
		break
	}
	switch p := msg.Payload.(type) {
	case *pb.ServerMsg_Location:
		_ = p.Location
	default:
	}
}
```

Limits enforced client-side: `MaxPublishHz = 50`, `MaxEventBytes = 4 KiB`, `MaxEventHz = 1`.

Protocol details and reconnect semantics: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto) / [reconnect spec](https://github.com/pickpoint/pickpoint-proto/blob/main/spec/reconnect.md).

---

## Develop

```bash
go test ./...
```

Live geocode batch e2e (skipped unless the key is set):

```bash
PICKPOINT_API_KEY=… go test ./pickpoint -run E2E -count=1
# optional: PICKPOINT_BASE_URL=https://api.pickpoint.io
```

Protobuf stubs under `tracking/v2` are generated from [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto); regenerate when the schema moves, then commit the Go output with this module.
