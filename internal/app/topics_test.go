package app

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"time"

	"kgrep/internal/config"
	"kgrep/internal/kafkaclient"
)

func TestWriteTopicsTableAlignsAndCountsRows(t *testing.T) {
	var stdout bytes.Buffer
	writeTopicsTable(&stdout, []kafkaclient.TopicSummary{
		{Name: "orders-crud", Partitions: 6, Internal: false},
		{Name: "__consumer_offsets", Partitions: 50, Internal: true},
	})
	output := stdout.String()
	if !strings.Contains(output, "orders-crud") || !strings.Contains(output, "regular") {
		t.Fatalf("missing regular topic row: %q", output)
	}
	if !strings.Contains(output, "__consumer_offsets") || !strings.Contains(output, "internal") {
		t.Fatalf("missing internal topic row: %q", output)
	}
	if !strings.Contains(output, "2 topic(s)") {
		t.Fatalf("missing topic count: %q", output)
	}
}

func TestFormatIntList(t *testing.T) {
	if got := formatIntList(nil); got != "-" {
		t.Fatalf("got %q, want -", got)
	}
	if got := formatIntList([]int{3, 1, 2}); got != "1,2,3" {
		t.Fatalf("got %q, want sorted 1,2,3", got)
	}
}

func TestWriteTopicDetailReportsEmptyTopicAndNoGroups(t *testing.T) {
	var stdout bytes.Buffer
	detail := kafkaclient.TopicDetail{
		Name:       "orders-crud",
		Internal:   false,
		Partitions: []kafkaclient.PartitionDetail{{ID: 0, Leader: 1, Low: 0, High: 0}},
	}
	writeTopicDetail(&stdout, detail, config.SchemaRegistrySettings{})
	output := stdout.String()
	if !strings.Contains(output, "Last message written: (topic is empty)") {
		t.Fatalf("expected empty-topic notice: %q", output)
	}
	if !strings.Contains(output, "(Schema Registry not configured)") {
		t.Fatalf("expected schema registry notice: %q", output)
	}
	if !strings.Contains(output, "(none)") {
		t.Fatalf("expected no-groups notice: %q", output)
	}
}

func TestWriteTopicDetailShowsGroupsAndLastMessageTime(t *testing.T) {
	lastWrite := time.Date(2026, 4, 13, 12, 0, 0, 0, time.UTC)
	var stdout bytes.Buffer
	detail := kafkaclient.TopicDetail{
		Name:            "orders-crud",
		Partitions:      []kafkaclient.PartitionDetail{{ID: 0, Leader: 1, Low: 0, High: 100}},
		TotalMessages:   100,
		LastMessageTime: &lastWrite,
		Groups: []kafkaclient.GroupSummary{
			{GroupID: "billing-service", State: "Stable", Active: true, Lag: 5, Members: []kafkaclient.GroupMember{{ClientID: "billing-1", ClientHost: "/10.0.0.5", Partitions: []int{0}}}},
			{GroupID: "reporting", State: "Empty", Active: false, Lag: 42},
		},
	}
	writeTopicDetail(&stdout, detail, config.SchemaRegistrySettings{})
	output := stdout.String()
	if !strings.Contains(output, "2026-04-13 12:00:00 UTC") {
		t.Fatalf("expected formatted last-write time: %q", output)
	}
	if !strings.Contains(output, "billing-service [Stable, active] lag=5") {
		t.Fatalf("expected active group summary: %q", output)
	}
	if !strings.Contains(output, "billing-1 @ /10.0.0.5 (partitions 0)") {
		t.Fatalf("expected member detail: %q", output)
	}
	if !strings.Contains(output, "reporting [Empty, inactive] lag=42") {
		t.Fatalf("expected inactive group summary: %q", output)
	}
}

func TestWriteTopicDetailReportsGroupsError(t *testing.T) {
	var stdout bytes.Buffer
	detail := kafkaclient.TopicDetail{Name: "orders-crud", GroupsError: errors.New("broker unreachable")}
	writeTopicDetail(&stdout, detail, config.SchemaRegistrySettings{})
	if !strings.Contains(stdout.String(), "could not retrieve consumer-group info") {
		t.Fatalf("expected groups-error notice: %q", stdout.String())
	}
}
