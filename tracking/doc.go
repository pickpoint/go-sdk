// Package tracking is the Go client for Pickpoint realtime tracking.
//
// Default transport is binary WebSocket (tracking.v2.proto). gRPC remains available
// via Config.Transport = TransportGRPC.
//
//	client, err := tracking.Connect(ctx, tracking.Config{
//	    Endpoint: "ws://127.0.0.1:3100",
//	    Device:   &tracking.DeviceAuth{ClientID: "dev-1", ClientSecret: "secret"},
//	})
package tracking
