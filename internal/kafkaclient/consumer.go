package kafkaclient

import (
	"context"
	"crypto/tls"
	"errors"
	"fmt"
	"io"
	"sort"
	"strings"
	"sync"
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
	diagnostics  io.Writer
	pollTimeout  time.Duration
	dialer       *kafka.Dialer
}

func New(settings config.KafkaSettings, topic string, deserializer *decode.Deserializer, fromMS, toMS *int64, idlePolls int, verbose bool, diagnostics io.Writer) (*Consumer, error) {
	dialer, err := newDialer(settings)
	if err != nil {
		return nil, err
	}
	if idlePolls < 1 {
		return nil, fmt.Errorf("idle-polls must be at least 1")
	}
	return &Consumer{settings: settings, topic: topic, deserializer: deserializer, fromMS: fromMS, toMS: toMS, idlePolls: idlePolls, verbose: verbose, diagnostics: diagnostics, pollTimeout: pollTimeout, dialer: dialer}, nil
}

// pollTimeout is both the reader's MaxWait (how long the broker may hold a fetch
// request open waiting for data) and the duration of one "idle poll" for
// --idle-polls bookkeeping. It must be generous enough that a single fetch
// attempt survives a broker-side throttling delay rather than timing out and
// immediately re-requesting: against a Confluent Cloud cluster enforcing a
// consume-bandwidth quota, a too-short MaxWait (previously 1s) caused fetches
// to time out and retry faster than the broker's throttle delay cleared,
// starving partitions of data that was in fact still there.
const pollTimeout = 5 * time.Second

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

// PartitionCoverage reports how far a single partition's scan actually got,
// so callers can tell "we stopped because we ran out of patience, not
// because we ran out of data" apart from a deliberate, requested stop.
type PartitionCoverage struct {
	Partition  int
	Low, High  int64
	LastOffset int64 // next unread offset; equals High when the partition was fully read
	Reason     string
}

// Complete reports whether this partition's stop point should be trusted as
// "nothing left to read" rather than "gave up early." Only idle-timeout is
// untrustworthy: it means --idle-polls elapsed before reaching High, which
// can happen even though more (slower-arriving) data exists past that point.
// Every other reason is either a real end (reached-high, empty) or a stop
// the caller explicitly asked for (max-messages, to-time), so neither
// indicates a missed message.
func (p PartitionCoverage) Complete() bool {
	return p.Reason != "idle-timeout"
}

func (c *Consumer) Iterate(
	ctx context.Context,
	maxMessages int,
	emit func(core.DecodedMessage) error,
	reportBad func(core.BadRecord) error,
) ([]PartitionCoverage, error) {
	if len(c.settings.BootstrapServers) == 0 {
		return nil, fmt.Errorf("no Kafka bootstrap servers configured")
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
		return nil, fmt.Errorf("load topic %q metadata: %w", c.topic, errors.Join(lookupErrors...))
	}
	if len(partitions) == 0 {
		return nil, fmt.Errorf("topic %q not found or has no partitions", c.topic)
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
		fmt.Fprintf(c.diagnostics, "Topic %q partitions: %v\n", c.topic, partitionIDs)
	}

	// Partitions are independent streams, so they're scanned concurrently rather than
	// one-after-another: on a slow/flaky broker each partition can spend minutes sitting
	// in idle-poll waits, and running them sequentially made that wait time additive
	// across all partitions instead of overlapping.
	groupCtx, cancelGroup := context.WithCancel(ctx)
	defer cancelGroup()

	var (
		mu            sync.Mutex // serializes emit/reportBad calls and guards processed/retainedTotal/coverage
		processed     int
		retainedTotal int64
		firstErr      error
		coverage      []PartitionCoverage
	)
	recordErr := func(err error) {
		mu.Lock()
		if firstErr == nil {
			firstErr = err
		}
		mu.Unlock()
		cancelGroup()
	}

	var wg sync.WaitGroup
	for _, partition := range partitionIDs {
		wg.Add(1)
		go func(partition int) {
			defer wg.Done()
			partitionCoverage, err := c.iteratePartition(groupCtx, partition, maxMessages, &mu, &processed, cancelGroup, emit, reportBad)
			mu.Lock()
			retainedTotal += max(partitionCoverage.High-partitionCoverage.Low, 0)
			coverage = append(coverage, partitionCoverage)
			mu.Unlock()
			if err != nil {
				recordErr(err)
			}
		}(partition)
	}
	wg.Wait()

	sort.Slice(coverage, func(i, j int) bool { return coverage[i].Partition < coverage[j].Partition })

	if c.verbose {
		fmt.Fprintf(c.diagnostics, "Estimated retained messages across all partitions: %d\n", retainedTotal)
	}
	if firstErr != nil {
		return nil, firstErr
	}
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return coverage, nil
}

// iteratePartition scans a single partition end-to-end. emit/reportBad, the shared
// processed counter, and diagnostics writes are only ever touched with mu held, since
// this runs concurrently with the same call for every other partition in the topic.
func (c *Consumer) iteratePartition(
	ctx context.Context,
	partition int,
	maxMessages int,
	mu *sync.Mutex,
	processed *int,
	stopAll func(),
	emit func(core.DecodedMessage) error,
	reportBad func(core.BadRecord) error,
) (coverage PartitionCoverage, err error) {
	bounds, err := c.partitionBounds(ctx, partition)
	if err != nil {
		return PartitionCoverage{Partition: partition}, err
	}
	coverage = PartitionCoverage{Partition: partition, Low: bounds.low, High: bounds.high, LastOffset: bounds.low}
	if c.verbose {
		mu.Lock()
		fmt.Fprintf(c.diagnostics, "Partition %d: low=%d high=%d retained=%d\n", partition, bounds.low, bounds.high, max(bounds.high-bounds.low, 0))
		mu.Unlock()
	}
	if bounds.low >= bounds.high {
		coverage.Reason = "empty"
		return coverage, nil
	}

	reader := kafka.NewReader(kafka.ReaderConfig{Brokers: c.settings.BootstrapServers, Topic: c.topic, Partition: partition, MinBytes: 1, MaxBytes: 10e6, MaxWait: c.pollTimeout, Dialer: c.dialer})
	defer func() {
		if closeErr := reader.Close(); closeErr != nil && err == nil {
			err = fmt.Errorf("close partition %d reader: %w", partition, closeErr)
		}
	}()

	if c.fromMS != nil {
		if setErr := reader.SetOffsetAt(ctx, time.UnixMilli(*c.fromMS)); setErr != nil {
			return coverage, fmt.Errorf("find offset for partition %d at requested time: %w", partition, setErr)
		}
	} else if setErr := reader.SetOffset(bounds.low); setErr != nil {
		return coverage, fmt.Errorf("set partition %d start offset: %w", partition, setErr)
	}
	if c.verbose {
		mu.Lock()
		fmt.Fprintf(c.diagnostics, "Partition %d: scanning through offset %d\n", partition, bounds.high-1)
		mu.Unlock()
	}

	idle := 0
	for {
		pollContext, cancel := context.WithTimeout(ctx, c.pollTimeout)
		message, fetchErr := reader.FetchMessage(pollContext)
		cancel()
		if fetchErr != nil {
			if errors.Is(fetchErr, context.DeadlineExceeded) {
				idle++
				if idle >= c.idlePolls {
					coverage.Reason = "idle-timeout"
					return coverage, nil
				}
				continue
			}
			if errors.Is(fetchErr, context.Canceled) {
				// ctx was canceled by another partition's error, a global
				// maxMessages cap, or the caller (e.g. SIGINT) — not this
				// partition's own failure, so it reports no error itself.
				coverage.Reason = "canceled"
				return coverage, nil
			}
			return coverage, fmt.Errorf("read partition %d: %w", partition, fetchErr)
		}
		idle = 0
		if message.Offset >= bounds.high {
			coverage.Reason = "reached-high"
			return coverage, nil
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
			coverage.Reason = "to-time"
			return coverage, nil
		}
		decoded, bad := c.decodeMessage(message, timestamp)

		mu.Lock()
		// Re-check the cap fresh under the lock, not just after processing our
		// own message: another partition can have already pushed processed to
		// maxMessages while this message was in flight (fetched/decoded before
		// stopAll's cancellation reached this goroutine's next poll). Without
		// this check, every partition with a message already in hand at the
		// moment the cap is hit would still emit it, so --max-messages N could
		// overshoot by up to (partition count - 1).
		if maxMessages > 0 && *processed >= maxMessages {
			mu.Unlock()
			stopAll()
			coverage.Reason = "max-messages"
			return coverage, nil
		}
		var cbErr error
		if bad != nil {
			if reportBad != nil {
				cbErr = reportBad(*bad)
			}
		} else {
			cbErr = emit(decoded)
		}
		exceeded := false
		if cbErr == nil {
			*processed++
			exceeded = maxMessages > 0 && *processed >= maxMessages
		}
		mu.Unlock()

		if cbErr != nil {
			return coverage, cbErr
		}
		coverage.LastOffset = message.Offset + 1
		if exceeded {
			stopAll()
			coverage.Reason = "max-messages"
			return coverage, nil
		}
		if message.Offset >= bounds.high-1 {
			coverage.Reason = "reached-high"
			return coverage, nil
		}
	}
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
