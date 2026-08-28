package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"time"

	"kgrep/internal/config"
	"kgrep/internal/decode"
	"kgrep/internal/kafkaclient"
)

func runTopics(stdout, stderr io.Writer) int {
	settings, err := config.KafkaFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	admin, err := kafkaclient.NewAdmin(settings, false, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()
	topics, err := admin.ListTopics(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	writeTopicsTable(stdout, topics)
	return 0
}

func writeTopicsTable(stdout io.Writer, topics []kafkaclient.TopicSummary) {
	nameWidth := len("TOPIC")
	for _, topic := range topics {
		nameWidth = max(nameWidth, len(topic.Name))
	}
	fmt.Fprintf(stdout, "%-*s  %-10s  %s\n", nameWidth, "TOPIC", "PARTITIONS", "TYPE")
	for _, topic := range topics {
		topicType := "regular"
		if topic.Internal {
			topicType = "internal"
		}
		fmt.Fprintf(stdout, "%-*s  %-10d  %s\n", nameWidth, topic.Name, topic.Partitions, topicType)
	}
	fmt.Fprintf(stdout, "\n%d topic(s)\n", len(topics))
}

func parseDescribeTopicOptions(args []string, stderr io.Writer) (string, bool, int) {
	flags := flag.NewFlagSet("describe-topic", flag.ContinueOnError)
	flags.SetOutput(stderr)
	topic := flags.String("topic", "", "Kafka topic; defaults to KAFKA_DEFAULT_TOPIC")
	verbose := flags.Bool("verbose", false, "Print diagnostics for consumer groups excluded from the report")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", false, 0
		}
		return "", false, 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "error: unexpected arguments: %s\n", strings.Join(flags.Args(), " "))
		return "", false, 2
	}
	return *topic, *verbose, -1
}

func runDescribeTopic(topicFlag string, verbose bool, stdout, stderr io.Writer) int {
	settings, err := config.KafkaFromEnv()
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	topic := topicFlag
	if topic == "" {
		topic = settings.DefaultTopic
	}
	if topic == "" {
		fmt.Fprintln(stderr, "error: --topic or KAFKA_DEFAULT_TOPIC is required")
		return 2
	}
	admin, err := kafkaclient.NewAdmin(settings, verbose, stderr)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	defer cancel()
	detail, err := admin.DescribeTopic(ctx, topic)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return 1
	}
	writeTopicDetail(stdout, detail, config.SchemaRegistryFromEnv())
	return 0
}

func writeTopicDetail(stdout io.Writer, detail kafkaclient.TopicDetail, schemaSettings config.SchemaRegistrySettings) {
	topicType := "regular"
	if detail.Internal {
		topicType = "internal"
	}
	fmt.Fprintf(stdout, "Topic: %s (%s)\n", detail.Name, topicType)
	fmt.Fprintf(stdout, "Partitions: %d\n", len(detail.Partitions))
	fmt.Fprintf(stdout, "Retained messages: %d\n", detail.TotalMessages)
	if detail.LastMessageTime != nil {
		fmt.Fprintf(stdout, "Last message written: %s\n", detail.LastMessageTime.UTC().Format("2006-01-02 15:04:05 MST"))
	} else {
		fmt.Fprintln(stdout, "Last message written: (topic is empty)")
	}

	fmt.Fprintln(stdout, "\nSchema:")
	writeSchemaType(stdout, schemaSettings, detail.Name, "key")
	writeSchemaType(stdout, schemaSettings, detail.Name, "value")

	fmt.Fprintln(stdout, "\nPartitions:")
	fmt.Fprintf(stdout, "%-6s %-8s %-16s %-16s %10s %10s %10s\n", "ID", "LEADER", "REPLICAS", "ISR", "LOW", "HIGH", "MESSAGES")
	for _, partition := range detail.Partitions {
		fmt.Fprintf(stdout, "%-6d %-8d %-16s %-16s %10d %10d %10d\n",
			partition.ID, partition.Leader, formatIntList(partition.Replicas), formatIntList(partition.ISR),
			partition.Low, partition.High, max(partition.High-partition.Low, 0))
	}

	fmt.Fprintln(stdout, "\nConsumer groups:")
	switch {
	case detail.GroupsError != nil:
		fmt.Fprintf(stdout, "  could not retrieve consumer-group info: %v\n", detail.GroupsError)
	case len(detail.Groups) == 0:
		fmt.Fprintln(stdout, "  (none)")
	default:
		for _, group := range detail.Groups {
			state := "inactive"
			if group.Active {
				state = "active"
			}
			fmt.Fprintf(stdout, "  %s [%s, %s] lag=%d\n", group.GroupID, group.State, state, group.Lag)
			for _, member := range group.Members {
				fmt.Fprintf(stdout, "    - %s @ %s (partitions %s)\n", member.ClientID, member.ClientHost, formatIntList(member.Partitions))
			}
		}
	}
}

func writeSchemaType(stdout io.Writer, settings config.SchemaRegistrySettings, topic, field string) {
	if strings.TrimSpace(settings.URL) == "" {
		fmt.Fprintf(stdout, "  %s: (Schema Registry not configured)\n", field)
		return
	}
	info, err := decode.NewRegistryClient(settings).LatestSubject(topic + "-" + field)
	if err != nil {
		fmt.Fprintf(stdout, "  %s: %v\n", field, err)
		return
	}
	fmt.Fprintf(stdout, "  %s: %s (subject %s, version %d)\n", field, info.SchemaType, info.Subject, info.Version)
}

func formatIntList(values []int) string {
	if len(values) == 0 {
		return "-"
	}
	sorted := append([]int(nil), values...)
	sort.Ints(sorted)
	parts := make([]string, len(sorted))
	for index, value := range sorted {
		parts[index] = strconv.Itoa(value)
	}
	return strings.Join(parts, ",")
}
