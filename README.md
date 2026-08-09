# go-sdk

Module **`github.com/pickpoint/go-sdk`** (Apache-2.0).

```bash
go get github.com/pickpoint/go-sdk@latest
```

```go
import "github.com/pickpoint/go-sdk/tracking"

client, err := tracking.Connect(ctx, tracking.Config{
    Endpoint:  "tracking.example.com:443",
    Transport: tracking.TransportGRPC,
    Device:    &tracking.DeviceAuth{ClientID: id, ClientSecret: secret},
})
```

## Status

| Package | Status |
|---------|--------|
| `tracking` | stub (gRPC primary, WS optional) |
| `geocoding` / … | reserved — gRPC/batch preferred for bulk workloads |

Protocol: [`pickpoint-proto`](https://github.com/pickpoint/pickpoint-proto).
