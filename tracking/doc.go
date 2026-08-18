// Package tracking is the Go client for Pickpoint live GPS.
//
// WebSocket only: wss://tracking.pickpoint.io/v2/ws, subprotocol tracking.v2.
// Endpoint is the host; the SDK appends /v2/ws. First Publish starts the trip;
// Close sends TrackStop. A dropped socket is Resume, not a new trip.
//
//	session, err := tracking.Connect(ctx, tracking.Config{
//	    Endpoint: "wss://tracking.pickpoint.io",
//	    Device:   &tracking.DeviceAuth{ClientID: deviceUID, ClientSecret: secret},
//	})
package tracking
