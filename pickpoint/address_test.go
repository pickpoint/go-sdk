package pickpoint_test

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/pickpoint/go-sdk/pickpoint"
)

func TestAddressSearch400Throws(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(400)
		_, _ = w.Write([]byte(`{"message":"bad","errorCode":400}`))
	}))
	defer srv.Close()
	c, _ := pickpoint.New(pickpoint.Config{APIKey: "k", BaseURL: srv.URL})
	_, err := c.Search(context.Background(), pickpoint.Query{"q": "x"})
	var api *pickpoint.APIError
	if !errors.As(err, &api) || api.Status != 400 {
		t.Fatalf("%v", err)
	}
}
