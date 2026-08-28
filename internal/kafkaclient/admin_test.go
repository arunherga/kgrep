package kafkaclient

import (
	"reflect"
	"testing"

	"github.com/segmentio/kafka-go"
)

func TestBuildPartitionDetailsSortsAndSumsRetainedMessages(t *testing.T) {
	partitions := []kafka.Partition{
		{ID: 1, Leader: kafka.Broker{ID: 2}, Replicas: []kafka.Broker{{ID: 2}, {ID: 3}}, Isr: []kafka.Broker{{ID: 2}}},
		{ID: 0, Leader: kafka.Broker{ID: 1}, Replicas: []kafka.Broker{{ID: 1}}, Isr: []kafka.Broker{{ID: 1}}},
	}
	offsets := map[int]kafka.PartitionOffsets{
		0: {Partition: 0, FirstOffset: 10, LastOffset: 20},
		1: {Partition: 1, FirstOffset: 0, LastOffset: 0},
	}
	details, total := buildPartitionDetails(partitions, offsets)
	if len(details) != 2 || details[0].ID != 0 || details[1].ID != 1 {
		t.Fatalf("expected partitions sorted by ID, got %#v", details)
	}
	if details[0].Low != 10 || details[0].High != 20 {
		t.Fatalf("unexpected watermarks for partition 0: %#v", details[0])
	}
	if !reflect.DeepEqual(details[0].Replicas, []int{1}) {
		t.Fatalf("unexpected replicas: %#v", details[0].Replicas)
	}
	if total != 10 {
		t.Fatalf("expected total retained messages 10 (empty partition 1 contributes 0), got %d", total)
	}
}

func TestSummarizeGroupComputesLagAndActiveState(t *testing.T) {
	offsetsByPartition := map[int]kafka.PartitionOffsets{
		0: {Partition: 0, LastOffset: 100},
		1: {Partition: 1, LastOffset: 50},
	}
	group := kafka.DescribeGroupsResponseGroup{
		GroupID:    "billing-service",
		GroupState: "Stable",
		Members: []kafka.DescribeGroupsResponseMember{
			{ClientID: "billing-1", ClientHost: "/10.0.0.5", MemberAssignments: kafka.DescribeGroupsResponseAssignments{
				Topics: []kafka.GroupMemberTopic{{Topic: "orders", Partitions: []int{0, 1}}},
			}},
		},
	}
	committed := []kafka.OffsetFetchPartition{
		{Partition: 0, CommittedOffset: 90},
		{Partition: 1, CommittedOffset: 50},
	}

	summary, ok := summarizeGroup(group, "orders", committed, offsetsByPartition)
	if !ok {
		t.Fatal("expected group to be reported as associated with the topic")
	}
	if summary.Lag != 10 {
		t.Fatalf("expected lag 10 (100-90 on partition 0, 0 on partition 1), got %d", summary.Lag)
	}
	if !summary.Active {
		t.Fatal("expected group with members to be active")
	}
	if len(summary.Members) != 1 || summary.Members[0].ClientID != "billing-1" || !reflect.DeepEqual(summary.Members[0].Partitions, []int{0, 1}) {
		t.Fatalf("unexpected members: %#v", summary.Members)
	}
}

func TestSummarizeGroupReportsInactiveWhenNoMembers(t *testing.T) {
	offsetsByPartition := map[int]kafka.PartitionOffsets{0: {Partition: 0, LastOffset: 100}}
	group := kafka.DescribeGroupsResponseGroup{GroupID: "old-consumer", GroupState: "Empty"}
	committed := []kafka.OffsetFetchPartition{{Partition: 0, CommittedOffset: 100}}

	summary, ok := summarizeGroup(group, "orders", committed, offsetsByPartition)
	if !ok {
		t.Fatal("expected group with a committed offset to be reported")
	}
	if summary.Active {
		t.Fatal("expected group with no members to be inactive")
	}
	if summary.Lag != 0 {
		t.Fatalf("expected zero lag when caught up, got %d", summary.Lag)
	}
}

func TestSummarizeGroupReportsActiveFromStateWhenMemberDecodeFailed(t *testing.T) {
	// Regression test: kafka-go populates GroupID/GroupState before it
	// attempts to decode each member's metadata, so a decode failure there
	// (observed for real against a Java consumer client's subscription
	// format) leaves Members empty while GroupState still correctly says
	// "Stable" — Active must be derived from GroupState, not len(Members),
	// or a group with a real, connected member gets silently misreported
	// as inactive.
	offsetsByPartition := map[int]kafka.PartitionOffsets{0: {Partition: 0, LastOffset: 100}}
	group := kafka.DescribeGroupsResponseGroup{GroupID: "real-consumer", GroupState: "Stable", Members: nil}
	committed := []kafka.OffsetFetchPartition{{Partition: 0, CommittedOffset: 100}}

	summary, ok := summarizeGroup(group, "orders", committed, offsetsByPartition)
	if !ok {
		t.Fatal("expected group with a committed offset to be reported")
	}
	if !summary.Active {
		t.Fatal("expected Stable group to be reported active even with no decoded members")
	}
}

func TestSummarizeGroupIgnoresGroupsWithNoCommittedOffsetForTopic(t *testing.T) {
	offsetsByPartition := map[int]kafka.PartitionOffsets{0: {Partition: 0, LastOffset: 100}}
	group := kafka.DescribeGroupsResponseGroup{GroupID: "unrelated-group"}
	// CommittedOffset -1 is Kafka's sentinel for "no offset committed on this partition by this group."
	committed := []kafka.OffsetFetchPartition{{Partition: 0, CommittedOffset: -1}}

	if _, ok := summarizeGroup(group, "orders", committed, offsetsByPartition); ok {
		t.Fatal("expected group with only sentinel (-1) offsets to be excluded")
	}
	if _, ok := summarizeGroup(group, "orders", nil, offsetsByPartition); ok {
		t.Fatal("expected group with no committed entries at all to be excluded")
	}
}
