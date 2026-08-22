package kafkaclient

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"

	"github.com/segmentio/kafka-go"

	"kgrep/internal/config"
)

// Concurrency/batching tuning for admin-protocol calls that scale poorly as
// a single request but well when split into many concurrent smaller ones.
// Empirically (against a real 85-consumer-group cluster): a single
// DescribeGroups call for all 85 groups took ~45s; batches of 5 groups each,
// run with this much concurrency, took ~2s. A single-group-at-a-time
// OffsetFetch loop (kafka-go has no bulk variant) took ~61s sequential vs
// ~5s at this concurrency.
const (
	describeGroupsBatchSize = 5
	adminFetchConcurrency   = 20
)

// Admin talks to the cluster for metadata/diagnostic purposes (topic
// listing, watermarks, consumer-group lag) rather than reading records.
type Admin struct {
	client *kafka.Client
}

func NewAdmin(settings config.KafkaSettings) (*Admin, error) {
	if len(settings.BootstrapServers) == 0 {
		return nil, fmt.Errorf("no Kafka bootstrap servers configured")
	}
	dialer, err := newDialer(settings)
	if err != nil {
		return nil, err
	}
	transport := &kafka.Transport{TLS: dialer.TLS, SASL: dialer.SASLMechanism, ClientID: "kgrep", DialTimeout: 10 * time.Second}
	client := &kafka.Client{Addr: kafka.TCP(settings.BootstrapServers...), Timeout: 30 * time.Second, Transport: transport}
	return &Admin{client: client}, nil
}

type TopicSummary struct {
	Name       string
	Internal   bool
	Partitions int
}

// ListTopics returns every topic visible in the cluster.
func (a *Admin) ListTopics(ctx context.Context) ([]TopicSummary, error) {
	response, err := a.client.Metadata(ctx, &kafka.MetadataRequest{})
	if err != nil {
		return nil, fmt.Errorf("list topics: %w", err)
	}
	summaries := make([]TopicSummary, 0, len(response.Topics))
	for _, topic := range response.Topics {
		if topic.Error != nil {
			continue
		}
		summaries = append(summaries, TopicSummary{Name: topic.Name, Internal: topic.Internal, Partitions: len(topic.Partitions)})
	}
	sort.Slice(summaries, func(i, j int) bool { return summaries[i].Name < summaries[j].Name })
	return summaries, nil
}

type PartitionDetail struct {
	ID              int
	Leader          int
	Replicas        []int
	ISR             []int
	Low, High       int64
	LastMessageTime *time.Time
}

type GroupMember struct {
	ClientID   string
	ClientHost string
	Partitions []int
}

type GroupSummary struct {
	GroupID string
	State   string
	Active  bool
	Members []GroupMember
	Lag     int64
}

type TopicDetail struct {
	Name            string
	Internal        bool
	Partitions      []PartitionDetail
	TotalMessages   int64
	LastMessageTime *time.Time
	Groups          []GroupSummary
	// GroupsError is set if consumer-group information could not be
	// retrieved at all (e.g. ListGroups/DescribeGroups failed); the rest of
	// the topic detail is still returned in that case.
	GroupsError error
}

// DescribeTopic gathers partition layout, message counts, last-write time,
// and consumer-group lag for a single topic.
func (a *Admin) DescribeTopic(ctx context.Context, topic string) (TopicDetail, error) {
	metaResponse, err := a.client.Metadata(ctx, &kafka.MetadataRequest{Topics: []string{topic}})
	if err != nil {
		return TopicDetail{}, fmt.Errorf("describe topic %q: %w", topic, err)
	}
	var topicMeta *kafka.Topic
	for index := range metaResponse.Topics {
		if metaResponse.Topics[index].Name == topic {
			topicMeta = &metaResponse.Topics[index]
			break
		}
	}
	if topicMeta == nil {
		return TopicDetail{}, fmt.Errorf("topic %q not found", topic)
	}
	if topicMeta.Error != nil {
		return TopicDetail{}, fmt.Errorf("topic %q: %w", topic, topicMeta.Error)
	}

	detail := TopicDetail{Name: topicMeta.Name, Internal: topicMeta.Internal}

	// A PartitionOffsets entry only has the field matching what was asked for
	// populated (FirstOffsetOf leaves LastOffset as -1 and vice versa) —
	// both must be requested to get a complete low/high watermark pair.
	offsetRequests := make([]kafka.OffsetRequest, 0, len(topicMeta.Partitions)*2)
	for _, partition := range topicMeta.Partitions {
		offsetRequests = append(offsetRequests, kafka.FirstOffsetOf(partition.ID), kafka.LastOffsetOf(partition.ID))
	}
	offsetsResponse, err := a.client.ListOffsets(ctx, &kafka.ListOffsetsRequest{Topics: map[string][]kafka.OffsetRequest{topic: offsetRequests}})
	if err != nil {
		return TopicDetail{}, fmt.Errorf("list offsets for topic %q: %w", topic, err)
	}
	offsetsByPartition := make(map[int]kafka.PartitionOffsets, len(offsetsResponse.Topics[topic]))
	for _, partitionOffsets := range offsetsResponse.Topics[topic] {
		offsetsByPartition[partitionOffsets.Partition] = partitionOffsets
	}

	detail.Partitions, detail.TotalMessages = buildPartitionDetails(topicMeta.Partitions, offsetsByPartition)
	detail.LastMessageTime = a.fillLastMessageTimes(ctx, topic, detail.Partitions)

	detail.Groups, detail.GroupsError = a.groupsForTopic(ctx, topic, offsetsByPartition)
	return detail, nil
}

// buildPartitionDetails combines partition metadata with their watermarks
// (already-fetched, so this does no I/O) and returns them sorted by ID along
// with the topic's total retained message count.
func buildPartitionDetails(partitions []kafka.Partition, offsetsByPartition map[int]kafka.PartitionOffsets) ([]PartitionDetail, int64) {
	sorted := append([]kafka.Partition(nil), partitions...)
	sort.Slice(sorted, func(i, j int) bool { return sorted[i].ID < sorted[j].ID })

	var total int64
	details := make([]PartitionDetail, 0, len(sorted))
	for _, partition := range sorted {
		offsets := offsetsByPartition[partition.ID]
		low, high := offsets.FirstOffset, offsets.LastOffset
		total += max(high-low, 0)
		details = append(details, PartitionDetail{
			ID:       partition.ID,
			Leader:   partition.Leader.ID,
			Replicas: brokerIDs(partition.Replicas),
			ISR:      brokerIDs(partition.Isr),
			Low:      low,
			High:     high,
		})
	}
	return details, total
}

func brokerIDs(brokers []kafka.Broker) []int {
	ids := make([]int, len(brokers))
	for index, broker := range brokers {
		ids[index] = broker.ID
	}
	return ids
}

// fillLastMessageTimes fetches each non-empty partition's last message
// timestamp concurrently (one Fetch call per partition; a topic can have
// enough partitions that doing this sequentially is a real cost, the same
// pattern that made the consumer-group lookups below slow one at a time)
// and returns the latest one across the whole topic.
func (a *Admin) fillLastMessageTimes(ctx context.Context, topic string, partitions []PartitionDetail) *time.Time {
	var mu sync.Mutex
	var topicLatest *time.Time
	var wg sync.WaitGroup
	sem := make(chan struct{}, adminFetchConcurrency)
	for index := range partitions {
		partition := &partitions[index]
		if partition.High <= partition.Low {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(partition *PartitionDetail) {
			defer wg.Done()
			defer func() { <-sem }()
			lastTime, err := a.lastMessageTime(ctx, topic, partition.ID, partition.High-1)
			if err != nil {
				return
			}
			partition.LastMessageTime = lastTime
			mu.Lock()
			if topicLatest == nil || lastTime.After(*topicLatest) {
				topicLatest = lastTime
			}
			mu.Unlock()
		}(partition)
	}
	wg.Wait()
	return topicLatest
}

func (a *Admin) lastMessageTime(ctx context.Context, topic string, partition int, offset int64) (*time.Time, error) {
	response, err := a.client.Fetch(ctx, &kafka.FetchRequest{Topic: topic, Partition: partition, Offset: offset, MinBytes: 1, MaxBytes: 1 << 20, MaxWait: 5 * time.Second})
	if err != nil {
		return nil, err
	}
	if response.Error != nil {
		return nil, response.Error
	}
	if response.Records == nil {
		return nil, fmt.Errorf("no records returned at offset %d", offset)
	}
	record, err := response.Records.ReadRecord()
	if err != nil {
		return nil, err
	}
	recordTime := record.Time
	return &recordTime, nil
}

// groupsForTopic finds every consumer group with committed offsets against
// topic (whether or not it currently has active members) and computes its
// lag. Failure to list/describe groups at all is returned as an error;
// failure to fetch one particular group's offsets just skips that group,
// since transient per-group issues shouldn't sink the whole report.
func (a *Admin) groupsForTopic(ctx context.Context, topic string, offsetsByPartition map[int]kafka.PartitionOffsets) ([]GroupSummary, error) {
	listResponse, err := a.client.ListGroups(ctx, &kafka.ListGroupsRequest{})
	if err != nil {
		return nil, fmt.Errorf("list consumer groups: %w", err)
	}
	if listResponse.Error != nil {
		return nil, fmt.Errorf("list consumer groups: %w", listResponse.Error)
	}
	if len(listResponse.Groups) == 0 {
		return nil, nil
	}

	groupIDs := make([]string, len(listResponse.Groups))
	for index, group := range listResponse.Groups {
		groupIDs[index] = group.GroupID
	}
	describedGroups := a.describeGroupsBatched(ctx, groupIDs)

	partitionIDs := make([]int, 0, len(offsetsByPartition))
	for id := range offsetsByPartition {
		partitionIDs = append(partitionIDs, id)
	}

	var mu sync.Mutex
	var summaries []GroupSummary
	var wg sync.WaitGroup
	sem := make(chan struct{}, adminFetchConcurrency)
	for _, group := range describedGroups {
		if group.Error != nil {
			continue
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(group kafka.DescribeGroupsResponseGroup) {
			defer wg.Done()
			defer func() { <-sem }()
			offsetResponse, err := a.client.OffsetFetch(ctx, &kafka.OffsetFetchRequest{GroupID: group.GroupID, Topics: map[string][]int{topic: partitionIDs}})
			if err != nil || offsetResponse.Error != nil {
				return
			}
			if summary, ok := summarizeGroup(group, topic, offsetResponse.Topics[topic], offsetsByPartition); ok {
				mu.Lock()
				summaries = append(summaries, summary)
				mu.Unlock()
			}
		}(group)
	}
	wg.Wait()

	sort.Slice(summaries, func(i, j int) bool { return summaries[i].GroupID < summaries[j].GroupID })
	return summaries, nil
}

// describeGroupsBatched calls DescribeGroups in small batches, concurrently
// — see the tuning constants' doc comment for why. A batch that fails is
// skipped rather than failing the whole lookup, consistent with per-group
// OffsetFetch failures also being tolerated in groupsForTopic.
func (a *Admin) describeGroupsBatched(ctx context.Context, groupIDs []string) []kafka.DescribeGroupsResponseGroup {
	var mu sync.Mutex
	var groups []kafka.DescribeGroupsResponseGroup
	var wg sync.WaitGroup
	sem := make(chan struct{}, adminFetchConcurrency)
	for start := 0; start < len(groupIDs); start += describeGroupsBatchSize {
		batch := groupIDs[start:min(start+describeGroupsBatchSize, len(groupIDs))]
		wg.Add(1)
		sem <- struct{}{}
		go func(batch []string) {
			defer wg.Done()
			defer func() { <-sem }()
			response, err := a.client.DescribeGroups(ctx, &kafka.DescribeGroupsRequest{GroupIDs: batch})
			if err != nil {
				return
			}
			mu.Lock()
			groups = append(groups, response.Groups...)
			mu.Unlock()
		}(batch)
	}
	wg.Wait()
	return groups
}

// summarizeGroup computes one group's lag and topic-relevant membership from
// already-fetched data (no I/O). It reports ok=false if the group has no
// committed offset for any partition of topic, meaning it isn't associated
// with this topic at all and shouldn't appear in the report.
func summarizeGroup(group kafka.DescribeGroupsResponseGroup, topic string, committed []kafka.OffsetFetchPartition, offsetsByPartition map[int]kafka.PartitionOffsets) (GroupSummary, bool) {
	if len(committed) == 0 {
		return GroupSummary{}, false
	}
	touchesTopic := false
	var lag int64
	for _, partitionOffset := range committed {
		if partitionOffset.CommittedOffset < 0 {
			continue
		}
		touchesTopic = true
		if offsets, ok := offsetsByPartition[partitionOffset.Partition]; ok {
			if diff := offsets.LastOffset - partitionOffset.CommittedOffset; diff > 0 {
				lag += diff
			}
		}
	}
	if !touchesTopic {
		return GroupSummary{}, false
	}

	var members []GroupMember
	for _, member := range group.Members {
		var partitions []int
		for _, memberTopic := range member.MemberAssignments.Topics {
			if memberTopic.Topic == topic {
				partitions = memberTopic.Partitions
				break
			}
		}
		members = append(members, GroupMember{ClientID: member.ClientID, ClientHost: member.ClientHost, Partitions: partitions})
	}

	return GroupSummary{
		GroupID: group.GroupID,
		State:   group.GroupState,
		Active:  len(group.Members) > 0,
		Members: members,
		Lag:     lag,
	}, true
}
