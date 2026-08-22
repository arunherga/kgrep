package selfupdate

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"time"
)

const Repo = "arunherga/kgrep"

type Asset struct {
	Name string
	URL  string
}

type Release struct {
	Tag    string
	Assets []Asset
}

type Client struct {
	HTTP *http.Client
}

func New() *Client {
	return &Client{HTTP: &http.Client{Timeout: 15 * time.Second}}
}

// Latest fetches the newest published release's tag and asset download URLs.
func (c *Client) Latest(ctx context.Context) (Release, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, "https://api.github.com/repos/"+Repo+"/releases/latest", nil)
	if err != nil {
		return Release{}, err
	}
	request.Header.Set("Accept", "application/vnd.github+json")
	response, err := c.HTTP.Do(request)
	if err != nil {
		return Release{}, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return Release{}, fmt.Errorf("GitHub API returned %s", response.Status)
	}
	var payload struct {
		TagName string `json:"tag_name"`
		Assets  []struct {
			Name               string `json:"name"`
			BrowserDownloadURL string `json:"browser_download_url"`
		} `json:"assets"`
	}
	if err := json.NewDecoder(response.Body).Decode(&payload); err != nil {
		return Release{}, fmt.Errorf("decode GitHub API response: %w", err)
	}
	release := Release{Tag: payload.TagName}
	for _, asset := range payload.Assets {
		release.Assets = append(release.Assets, Asset{Name: asset.Name, URL: asset.BrowserDownloadURL})
	}
	return release, nil
}

// Download fetches the raw contents of a release asset.
func (c *Client) Download(ctx context.Context, url string) ([]byte, error) {
	request, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}
	response, err := c.HTTP.Do(request)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, fmt.Errorf("download %s: %s", url, response.Status)
	}
	return io.ReadAll(response.Body)
}

func FindAsset(release Release, name string) (Asset, bool) {
	for _, asset := range release.Assets {
		if asset.Name == name {
			return asset, true
		}
	}
	return Asset{}, false
}

// AssetName returns the release asset filename this project's release
// workflow builds for a given OS/architecture (see .github/workflows/release.yml).
func AssetName(goos, goarch string) string {
	name := fmt.Sprintf("kgrep-%s-%s", goos, goarch)
	if goos == "windows" {
		name += ".exe"
	}
	return name
}

// IsNewer reports whether latest is a greater vMAJOR.MINOR.PATCH version than
// current. It returns false (rather than erroring) if either version isn't in
// that form, so non-release "dev" builds never falsely claim to be current.
func IsNewer(current, latest string) bool {
	currentParts, ok := parseVersion(current)
	if !ok {
		return false
	}
	latestParts, ok := parseVersion(latest)
	if !ok {
		return false
	}
	for index := range currentParts {
		if latestParts[index] != currentParts[index] {
			return latestParts[index] > currentParts[index]
		}
	}
	return false
}

func parseVersion(version string) ([3]int, bool) {
	var parts [3]int
	version = strings.TrimPrefix(strings.TrimSpace(version), "v")
	fields := strings.SplitN(version, ".", 3)
	if len(fields) != 3 {
		return parts, false
	}
	for index, field := range fields {
		number, err := strconv.Atoi(field)
		if err != nil {
			return parts, false
		}
		parts[index] = number
	}
	return parts, true
}

// VerifyChecksum checks data's SHA-256 digest against the entry for assetName
// in a checksums.txt-formatted file (as produced by `sha256sum`).
func VerifyChecksum(data []byte, assetName string, checksums []byte) error {
	sum := sha256.Sum256(data)
	actual := hex.EncodeToString(sum[:])
	for _, line := range strings.Split(string(checksums), "\n") {
		fields := strings.Fields(line)
		if len(fields) != 2 {
			continue
		}
		name := strings.TrimPrefix(fields[1], "*")
		if name == assetName {
			if fields[0] != actual {
				return fmt.Errorf("checksum mismatch for %s: expected %s, got %s", assetName, fields[0], actual)
			}
			return nil
		}
	}
	return fmt.Errorf("no checksum entry found for %s", assetName)
}

// ReplaceExecutable atomically installs newContent at path. On Windows, the
// currently running executable can't be overwritten directly, so the existing
// file is renamed aside first and restored if installing the new one fails.
func ReplaceExecutable(path string, newContent []byte) error {
	directory := filepath.Dir(path)
	temp, err := os.CreateTemp(directory, filepath.Base(path)+".new-*")
	if err != nil {
		return fmt.Errorf("create temp file: %w", err)
	}
	tempPath := temp.Name()
	if _, err := temp.Write(newContent); err != nil {
		temp.Close()
		os.Remove(tempPath)
		return fmt.Errorf("write temp file: %w", err)
	}
	if err := temp.Close(); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("close temp file: %w", err)
	}
	if err := os.Chmod(tempPath, 0o755); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("set executable permission: %w", err)
	}

	if runtime.GOOS == "windows" {
		oldPath := path + ".old"
		os.Remove(oldPath) // best-effort cleanup of a previous update's leftover
		if err := os.Rename(path, oldPath); err != nil {
			os.Remove(tempPath)
			return fmt.Errorf("move current executable aside: %w", err)
		}
		if err := os.Rename(tempPath, path); err != nil {
			os.Rename(oldPath, path) // best-effort restore
			return fmt.Errorf("install new executable: %w", err)
		}
		os.Remove(oldPath) // best-effort; harmless if it's still in use and this fails
		return nil
	}

	if err := os.Rename(tempPath, path); err != nil {
		os.Remove(tempPath)
		return fmt.Errorf("install new executable: %w", err)
	}
	return nil
}
