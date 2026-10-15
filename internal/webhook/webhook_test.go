package webhook

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"strings"
	"testing"
)

func TestEndpointInputValidate(t *testing.T) {
	ok := []string{
		"https://example.com/hooks",
		"http://localhost:9000/x",
		"http://10.0.0.5/receive",
		" https://example.com/trim ",
	}
	for _, u := range ok {
		in := EndpointInput{URL: u}
		if err := in.Validate(); err != nil {
			t.Errorf("%q should be valid: %v", u, err)
		}
		if strings.TrimSpace(in.URL) != in.URL {
			t.Errorf("url not trimmed: %q", in.URL)
		}
	}

	bad := map[string]string{
		"":                                     "url",
		"ftp://example.com/x":                  "url",
		"example.com/no-scheme":                "url",
		"https://":                             "url",
		"https://user:pw@example.com/x":        "url",
		"https://" + strings.Repeat("a", 2100): "url",
	}
	for u, field := range bad {
		in := EndpointInput{URL: u}
		var verr *ValidationError
		if err := in.Validate(); !errors.As(err, &verr) || verr.Fields[field] == "" {
			t.Errorf("%q should fail on %s, got %v", u, field, err)
		}
	}

	long := EndpointInput{URL: "https://example.com", Secret: strings.Repeat("s", 300)}
	var verr *ValidationError
	if err := long.Validate(); !errors.As(err, &verr) || verr.Fields["secret"] == "" {
		t.Errorf("oversized secret should fail: %v", err)
	}
}

func TestSign(t *testing.T) {
	body := []byte(`{"hello":"world"}`)
	got := Sign("top-secret", body)

	mac := hmac.New(sha256.New, []byte("top-secret"))
	mac.Write(body)
	want := "sha256=" + hex.EncodeToString(mac.Sum(nil))
	if got != want {
		t.Errorf("Sign = %s, want %s", got, want)
	}
	if Sign("other", body) == got {
		t.Error("different secrets must produce different signatures")
	}
}
