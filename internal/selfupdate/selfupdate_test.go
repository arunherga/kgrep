package selfupdate

import (
	"context"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (function roundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return function(request)
}

func jsonResponse(request *http.Request, status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Status: http.StatusText(status), Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body)), Request: request}
}

func TestIsNewer(t *testing.T) {
	tests := []struct {
		current, latest string
		want            bool
	}{
		{"v1.0.0", "v1.0.1", true},
		{"v1.0.0", "v2.0.0", true},
		{"v1.2.0", "v1.10.0", true},
		{"v1.0.1", "v1.0.0", false},
		{"v1.0.0", "v1.0.0", false},
		{"dev", "v1.0.0", false},
		{"v1.0.0", "not-a-version", false},
	}
	for _, test := range tests {
		if got := IsNewer(test.current, test.latest); got != test.want {
			t.Errorf("IsNewer(%q, %q)=%v, want %v", test.current, test.latest, got, test.want)
		}
	}
}

func TestAssetName(t *testing.T) {
	if got := AssetName("linux", "amd64"); got != "kgrep-linux-amd64" {
		t.Fatalf("got %q", got)
	}
	if got := AssetName("windows", "amd64"); got != "kgrep-windows-amd64.exe" {
		t.Fatalf("got %q", got)
	}
}

func TestVerifyChecksum(t *testing.T) {
	data := []byte("hello world")
	// sha256("hello world")
	const sum = "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"
	checksums := []byte(sum + "  kgrep-linux-amd64\nabc  other-file\n")
	if err := VerifyChecksum(data, "kgrep-linux-amd64", checksums); err != nil {
		t.Fatalf("expected match, got %v", err)
	}
	if err := VerifyChecksum(data, "other-file", checksums); err == nil {
		t.Fatal("expected checksum mismatch error")
	}
	if err := VerifyChecksum(data, "missing-file", checksums); err == nil {
		t.Fatal("expected missing-entry error")
	}
	// asterisk-prefixed (binary mode) filenames must still match
	binaryModeChecksums := []byte(sum + "  *kgrep-linux-amd64\n")
	if err := VerifyChecksum(data, "kgrep-linux-amd64", binaryModeChecksums); err != nil {
		t.Fatalf("expected match with binary-mode marker, got %v", err)
	}
}

func TestLatestParsesTagAndAssets(t *testing.T) {
	client := &Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		if request.URL.String() != "https://api.github.com/repos/"+Repo+"/releases/latest" {
			t.Fatalf("unexpected URL: %s", request.URL)
		}
		return jsonResponse(request, 200, `{"tag_name":"v2.1.0","assets":[{"name":"kgrep-linux-amd64","browser_download_url":"https://example.com/kgrep-linux-amd64"},{"name":"checksums.txt","browser_download_url":"https://example.com/checksums.txt"}]}`), nil
	})}}
	release, err := client.Latest(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if release.Tag != "v2.1.0" {
		t.Fatalf("unexpected tag: %s", release.Tag)
	}
	asset, ok := FindAsset(release, "kgrep-linux-amd64")
	if !ok || asset.URL != "https://example.com/kgrep-linux-amd64" {
		t.Fatalf("unexpected asset: %#v ok=%v", asset, ok)
	}
	if _, ok := FindAsset(release, "does-not-exist"); ok {
		t.Fatal("expected no match for nonexistent asset")
	}
}

func TestLatestReturnsErrorOnNon2xx(t *testing.T) {
	client := &Client{HTTP: &http.Client{Transport: roundTripFunc(func(request *http.Request) (*http.Response, error) {
		return jsonResponse(request, 403, `{"message":"rate limited"}`), nil
	})}}
	if _, err := client.Latest(context.Background()); err == nil {
		t.Fatal("expected error on non-2xx response")
	}
}

func TestReplaceExecutableInstallsNewContent(t *testing.T) {
	directory := t.TempDir()
	path := filepath.Join(directory, "kgrep-test-binary")
	if err := os.WriteFile(path, []byte("old content"), 0o755); err != nil {
		t.Fatal(err)
	}
	if err := ReplaceExecutable(path, []byte("new content")); err != nil {
		t.Fatal(err)
	}
	contents, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if string(contents) != "new content" {
		t.Fatalf("got %q", contents)
	}
	entries, err := os.ReadDir(directory)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 {
		t.Fatalf("expected only the installed file to remain, found: %v", entries)
	}
}
