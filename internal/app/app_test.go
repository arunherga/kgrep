package app

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"net/http"
	"runtime"
	"strings"
	"testing"
	"time"

	"kgrep/internal/core"
	"kgrep/internal/selfupdate"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func jsonResponse(request *http.Request, body string) *http.Response {
	return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func TestRowForMessageDisplaysDecodedAvroKey(t *testing.T) {
	message := core.DecodedMessage{Topic: "topic-a", Partition: 0, Offset: 10, RawKey: []byte{0, 0, 0, 0, 1}, Key: map[string]any{"CountryCode": "CN", "CartId": "357b75b6"}, Value: map[string]any{"CartId": "357b75b6"}, KeyFormat: "avro", ValueFormat: "avro"}
	result := core.MatchResult{ValueMatches: map[string]struct{}{"357b75b6": {}}}
	row, err := RowForMessage(message, result, "UTC")
	if err != nil {
		t.Fatal(err)
	}
	if row["kafka_key"] != `{"CartId": "357b75b6", "CountryCode": "CN"}` {
		t.Fatalf("unexpected key: %s", row["kafka_key"])
	}
}

func TestFormatDuration(t *testing.T) {
	for duration, expected := range map[time.Duration]string{123 * time.Millisecond: "123 ms", 2500 * time.Millisecond: "2.50 seconds", 65250 * time.Millisecond: "1m 5.25s"} {
		if actual := FormatDuration(duration); actual != expected {
			t.Errorf("got %q, want %q", actual, expected)
		}
	}
}

func TestFormatBadRecordIncludesLocationAndSeparateErrors(t *testing.T) {
	record := core.BadRecord{
		Topic:      "topic-a",
		Partition:  4,
		Offset:     99,
		KeyError:   "invalid key\nJSON",
		ValueError: "invalid value JSON",
	}
	actual := FormatBadRecord(record)
	expected := "BAD topic-a[4]@99 key_error=invalid key JSON value_error=invalid value JSON"
	if actual != expected {
		t.Fatalf("got %q, want %q", actual, expected)
	}
}

func TestParseConsumeOptionsSupportsRepeatableFieldsAndReadAll(t *testing.T) {
	var stderr bytes.Buffer
	options, code := parseConsumeOptions("consume", []string{"--topic", "topic-a", "--read-all", "--key-field", "id", "--key-field", "nested.id"}, &stderr)
	if code != -1 || options.topic != "topic-a" || !options.readAll || len(options.keyFields) != 2 {
		t.Fatalf("options=%#v code=%d stderr=%q", options, code, stderr.String())
	}
}

func TestRunUpdateRejectsDevBuilds(t *testing.T) {
	client := &selfupdate.Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		t.Fatal("no network call expected for a dev build")
		return nil, nil
	})}}
	install := func(string, []byte) error { t.Fatal("install should not be called"); return nil }
	var stdout, stderr bytes.Buffer
	code := runUpdate(client, "dev", install, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "not available for development builds") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestRunUpdateReportsAlreadyUpToDate(t *testing.T) {
	client := &selfupdate.Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, `{"tag_name":"v2.0.1","assets":[]}`), nil
	})}}
	install := func(string, []byte) error { t.Fatal("install should not be called"); return nil }
	var stdout, stderr bytes.Buffer
	code := runUpdate(client, "v2.0.1", install, &stdout, &stderr)
	if code != 0 || !strings.Contains(stdout.String(), "already up to date") {
		t.Fatalf("code=%d stdout=%q", code, stdout.String())
	}
}

func TestRunUpdateDownloadsVerifiesAndInstalls(t *testing.T) {
	assetName := selfupdate.AssetName(runtime.GOOS, runtime.GOARCH)
	binaryContent := []byte("new-binary-bytes")
	sum := sha256.Sum256(binaryContent)
	checksums := hex.EncodeToString(sum[:]) + "  " + assetName + "\n"

	client := &selfupdate.Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.String() {
		case "https://api.github.com/repos/" + selfupdate.Repo + "/releases/latest":
			return jsonResponse(request, `{"tag_name":"v9.9.9","assets":[{"name":"`+assetName+`","browser_download_url":"https://example.com/`+assetName+`"},{"name":"checksums.txt","browser_download_url":"https://example.com/checksums.txt"}]}`), nil
		case "https://example.com/" + assetName:
			return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(bytes.NewReader(binaryContent)), Request: request}, nil
		case "https://example.com/checksums.txt":
			return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(checksums)), Request: request}, nil
		default:
			t.Fatalf("unexpected request: %s", request.URL)
			return nil, nil
		}
	})}}

	var installedPath string
	var installedContent []byte
	install := func(path string, content []byte) error {
		installedPath = path
		installedContent = content
		return nil
	}

	var stdout, stderr bytes.Buffer
	code := runUpdate(client, "v1.0.0", install, &stdout, &stderr)
	if code != 0 || stderr.Len() != 0 {
		t.Fatalf("code=%d stdout=%q stderr=%q", code, stdout.String(), stderr.String())
	}
	if string(installedContent) != string(binaryContent) {
		t.Fatalf("installed content=%q, want %q", installedContent, binaryContent)
	}
	if installedPath == "" {
		t.Fatal("expected install to be called with the current executable path")
	}
	if !strings.Contains(stdout.String(), "Updated kgrep v1.0.0 -> v9.9.9") {
		t.Fatalf("unexpected stdout: %q", stdout.String())
	}
}

func TestRunUpdateAbortsOnChecksumMismatch(t *testing.T) {
	assetName := selfupdate.AssetName(runtime.GOOS, runtime.GOARCH)
	client := &selfupdate.Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		switch request.URL.String() {
		case "https://api.github.com/repos/" + selfupdate.Repo + "/releases/latest":
			return jsonResponse(request, `{"tag_name":"v9.9.9","assets":[{"name":"`+assetName+`","browser_download_url":"https://example.com/`+assetName+`"},{"name":"checksums.txt","browser_download_url":"https://example.com/checksums.txt"}]}`), nil
		case "https://example.com/" + assetName:
			return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader("binary-bytes")), Request: request}, nil
		case "https://example.com/checksums.txt":
			return &http.Response{StatusCode: 200, Status: "200 OK", Header: make(http.Header), Body: io.NopCloser(strings.NewReader(strings.Repeat("0", 64) + "  " + assetName + "\n")), Request: request}, nil
		default:
			t.Fatalf("unexpected request: %s", request.URL)
			return nil, nil
		}
	})}}
	install := func(string, []byte) error { t.Fatal("install should not be called on checksum mismatch"); return nil }
	var stdout, stderr bytes.Buffer
	code := runUpdate(client, "v1.0.0", install, &stdout, &stderr)
	if code != 1 || !strings.Contains(stderr.String(), "checksum mismatch") {
		t.Fatalf("code=%d stderr=%q", code, stderr.String())
	}
}

func TestCheckForUpdatePrintsNoticeWhenNewer(t *testing.T) {
	client := &selfupdate.Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, `{"tag_name":"v9.9.9","assets":[]}`), nil
	})}}
	var stderr bytes.Buffer
	checkForUpdate(client, "v1.0.0", &stderr)
	if !strings.Contains(stderr.String(), "v9.9.9") || !strings.Contains(stderr.String(), "kgrep update") {
		t.Fatalf("unexpected stderr: %q", stderr.String())
	}
}

func TestCheckForUpdateSkipsForDevBuildAndWhenDisabled(t *testing.T) {
	client := &selfupdate.Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		t.Fatal("no network call expected")
		return nil, nil
	})}}
	var stderr bytes.Buffer
	checkForUpdate(client, "dev", &stderr)
	if stderr.Len() != 0 {
		t.Fatalf("expected no output for dev build, got %q", stderr.String())
	}
	t.Setenv("KGREP_SKIP_UPDATE_CHECK", "1")
	checkForUpdate(client, "v1.0.0", &stderr)
	if stderr.Len() != 0 {
		t.Fatalf("expected no output when disabled, got %q", stderr.String())
	}
}
