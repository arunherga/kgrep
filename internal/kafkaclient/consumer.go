package kafkaclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"net"
	"sort"
	"strings"
	"time"

	"github.com/segmentio/kafka-go"
	"github.com/segmentio/kafka-go/sasl/plain"
	"github.com/segmentio/kafka-go/sasl/scram"

	"kgrep/internal/config"
	"kgrep/internal/core"
	"kgrep/internal/decode"
)

type Consumer struct {
	settings     config.KafkaSettings
	topic        string
	deserializer *decode.Deserializer
	fromMS       *int64
	toMS         *int64
	idlePolls    int
	verbose      bool
	pollTimeout  time.Duration
	dialer       *kafka.Dialer
}

func New(settings config.KafkaSettings, topic string, deserializer *decode.Deserializer, fromMS, toMS *int64, idlePolls int, verbose bool) (*Consumer, error) {
	dialer, err := newDialer(settings)
	if err != nil {
		return nil, err
	}
	if idlePolls < 1 {
		return nil, fmt.Errorf("idle-polls must be at least 1")
	}
	return &Consumer{settings: settings, topic: topic, deserializer: deserializer, fromMS: fromMS, toMS: toMS, idlePolls: idlePolls, verbose: verbose, pollTimeout: time.Second, dialer: dialer}, nil
}

func newDialer(settings config.KafkaSettings) (*kafka.Dialer, error) {
	dialer := &kafka.Dialer{Timeout: 10 * time.Second, DualStack: true, ClientID: "kgrep"}
	protocol := strings.ToUpper(settings.SecurityProtocol)
	if protocol == "SSL" || protocol == "SASL_SSL" {
		dialer.TLS = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	if protocol == "SASL_SSL" || protocol == "SASL_PLAINTEXT" {
		switch strings.ToUpper(settings.SASLMechanism) {
		case "PLAIN":
			dialer.SASLMechanism = plain.Mechanism{Username: settings.Username, Password: settings.Password}
		case "SCRAM-SHA-256":
			mechanism, err := scram.Mechanism(scram.SHA256, settings.Username, settings.Password)
			if err != nil {
				return nil, fmt.Errorf("configure SCRAM-SHA-256: %w", err)
			}
			dialer.SASLMechanism = mechanism
		case "SCRAM-SHA-512":
			mechanism, err := scram.Mechanism(scram.SHA512, settings.Username, settings.Password)
			if err != nil {
				return nil, fmt.Errorf("configure SCRAM-SHA-512: %w", err)
			}
			dialer.SASLMechanism = mechanism
		default:
			return nil, fmt.Errorf("unsupported KAFKA_SASL_MECHANISM %q; supported mechanisms: PLAIN, SCRAM-SHA-256, SCRAM-SHA-512", settings.SASLMechanism)
		}
	}
	switch protocol {
	case "SSL", "SASL_SSL", "PLAINTEXT", "SASL_PLAINTEXT":
	default:
		return nil, fmt.Errorf("unsupported KAFKA_SECURITY_PROTOCOL %q", settings.SecurityProtocol)
	}
	return dialer, nil
}

func (c *Consumer) Iterate(
	ctx context.Context,
	maxMessages int,
	emit func(core.DecodedMessage) error,
	reportBad func(core.BadRecord) error,
) error {
	if len(c.settings.BootstrapServers) == 0 {
		return fmt.Errorf("no Kafka bootstrap servers configured")
	}
	var partitions []kafka.Partition
	var err error
	var lookupErrors []error
	for _, broker := range c.settings.BootstrapServers {
		partitions, err = c.dialer.LookupPartitions(ctx, "tcp", broker, c.topic)
		if err == nil {
			break
		}
		lookupErrors = append(lookupErrors, fmt.Errorf("broker %s: %w", broker, err))
	}
	if err != nil {
		return fmt.Errorf("load topic %q metadata: %w", c.topic, errors.Join(lookupErrors...))
	}
	if len(partitions) == 0 {
		return fmt.Errorf("topic %q not found or has no partitions", c.topic)
	}
	partitionIDs := make([]int, 0, len(partitions))
	seen := make(map[int]struct{})
	for _, partition := range partitions {
		if _, ok := seen[partition.ID]; !ok {
			partitionIDs = append(partitionIDs, partition.ID)
			seen[partition.ID] = struct{}{}
		}
	}
	sort.Ints(partitionIDs)
	if c.verbose {
		fmt.Printf("Topic %q partitions: %v\n", c.topic, partitionIDs)
	}

	processed := 0
	retainedTotal := int64(0)
	for _, partition := range partitionIDs {
		bounds, err := c.partitionBounds(ctx, partition)
		if err != nil {
			return err
		}
		retainedTotal += max(bounds.high-bounds.low, 0)
		if c.verbose {
			fmt.Printf("Partition %d: low=%d high=%d retained=%d\n", partition, bounds.low, bounds.high, max(bounds.high-bounds.low, 0))
		}
		if bounds.low >= bounds.high {
			continue
		}
		reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.settings.BootstrapServers, Topic: c.topic, Partition: partition, MinBytes: 1, MaxBytes: 10e6, MaxWait: c.pollTimeout, Dialer: c.dialer})
		if c.fromMS != nil {
			if err := reader.SetOffsetAt(ctx, time.UnixMilli(*c.fromMS)); err != nil {
				reader.Close()
				return fmt.Errorf("find offset for partition %d at requested time: %w", partition, err)
			}
		} else if err := reader.SetOffset(bounds.low); err != nil {
			reader.Close()
			return fmt.Errorf("set partition %d start offset: %w", partition, err)
		}
		if c.verbose {
			fmt.Printf("Partition %d: scanning through offset %d\n", partition, bounds.high-1)
		}

		idle := 0
		for {
			pollContext, cancel := context.WithTimeout(ctx, c.pollTimeout)
			message, err := reader.FetchMessage(pollContext)
			cancel()
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					idle++
					if idle >= c.idlePolls {
						break
					}
					continue
				}
				reader.Close()
				if errors.Is(err, context.Canceled) && ctx.Err() != nil {
					return ctx.Err()
				}
				return fmt.Errorf("read partition %d: %w", partition, err)
			}
			idle = 0
			if message.Offset >= bounds.high {
				break
			}
			var timestamp *int64
			if !message.Time.IsZero() {
				value := message.Time.UnixMilli()
				timestamp = &value
			}
			if c.fromMS != nil && timestamp != nil && *timestamp < *c.fromMS {
				continue
			}
			if c.toMS != nil && timestamp != nil && *timestamp > *c.toMS {
				break
			}
			decoded, bad := c.decodeMessage(message, timestamp)
			if bad != nil {
				if reportBad != nil {
					if err := reportBad(*bad); err != nil {
						reader.Close()
						return err
					}
				}
			} else {
				if err := emit(decoded); err != nil {
					reader.Close()
					return err
				}
			}
			processed++
			if message.Offset >= bounds.high-1 || (maxMessages > 0 && processed >= maxMessages) {
				break
			}
		}
		if err := reader.Close(); err != nil {
			return fmt.Errorf("close partition %d reader: %w", partition, err)
		}
		if maxMessages > 0 && processed >= maxMessages {
			break
		}
	}
	if c.verbose {
		fmt.Printf("Estimated retained messages across all partitions: %d\n", retainedTotal)
	}
	return nil
}

func (c *Consumer) decodeMessage(message kafka.Message, timestamp *int64) (core.DecodedMessage, *core.BadRecord) {
	key, keyFormat, keyErr := c.deserializer.DecodeKey(message.Key)
	value, valueFormat, valueErr := c.deserializer.DecodeValue(message.Value)
	if keyErr != nil || valueErr != nil {
		bad := &core.BadRecord{
			Topic:       message.Topic,
			Partition:   message.Partition,
			Offset:      message.Offset,
			TimestampMS: timestamp,
		}
		if keyErr != nil {
			bad.KeyError = keyErr.Error()
		}
		if valueErr != nil {
			bad.ValueError = valueErr.Error()
		}
		return core.DecodedMessage{}, bad
	}
	return core.DecodedMessage{
		Topic:       message.Topic,
		Partition:   message.Partition,
		Offset:      message.Offset,
		TimestampMS: timestamp,
		RawKey:      message.Key,
		RawValue:    message.Value,
		Key:         key,
		Value:       value,
		KeyFormat:   keyFormat,
		ValueFormat: valueFormat,
	}, nil
}

type bounds struct{ low, high int64 }

func (c *Consumer) partitionBounds(ctx context.Context, partition int) (bounds, error) {
	var connection *kafka.Conn
	var dialErrors []error
	for _, broker := range c.settings.BootstrapServers {
		candidate, err := c.dialer.DialLeader(ctx, "tcp", broker, c.topic, partition)
		if err == nil {
			connection = candidate
			break
		}
		dialErrors = append(dialErrors, fmt.Errorf("broker %s: %w", broker, err))
	}
	if connection == nil {
		return bounds{}, fmt.Errorf("connect to topic %q partition %d leader: %w", c.topic, partition, errors.Join(dialErrors...))
	}
	defer connection.Close()
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		return bounds{}, fmt.Errorf("set partition %d metadata deadline: %w", partition, err)
	}
	low, err := connection.ReadFirstOffset()
	if err != nil {
		return bounds{}, fmt.Errorf("read partition %d low watermark: %w", partition, err)
	}
	high, err := connection.ReadLastOffset()
	if err != nil {
		return bounds{}, fmt.Errorf("read partition %d high watermark: %w", partition, err)
	}
	return bounds{low: low, high: high}, nil
}

func IsNetworkError(err error) bool {
	var networkError net.Error
	return errors.As(err, &networkError)
}
