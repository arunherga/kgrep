package app

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"runtime"
	"slices"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"kgrep/internal/config"
	"kgrep/internal/core"
	"kgrep/internal/csvutil"
	"kgrep/internal/decode"
	"kgrep/internal/filter"
	"kgrep/internal/kafkaclient"
	"kgrep/internal/selfupdate"
	"kgrep/internal/timeutil"
)

const deliveryIdentifiers = "delivery-identifiers"
const skipUpdateCheckEnv = "KGREP_SKIP_UPDATE_CHECK"

type consumeOptions struct {
	topic, allowedCSV, fromTime, toTime, outputCSV string
	matchMode, keyFormat, valueFormat              string
	timezone                                       string
	maxMessages, idlePolls                         int
	readAll, printMatchesOnly, verbose             bool
	keyFields, valueFields, shortcuts              stringList
}

type stringList []string

func (values *stringList) String() string { return strings.Join(*values, ",") }
func (values *stringList) Set(value string) error {
	*values = append(*values, value)
	return nil
}

func Run(version string, args []string, stdout, stderr io.Writer) int {
	started := time.Now()
	command, code := run(version, args, stdout, stderr)
	fmt.Fprintf(stdout, "Total runtime: %s\n", FormatDuration(time.Since(started)))
	if command != "update" {
		checkForUpdate(selfupdate.New(), version, stderr)
	}
	return code
}

func run(version string, args []string, stdout, stderr io.Writer) (string, int) {
	global := flag.NewFlagSet("kgrep", flag.ContinueOnError)
	global.SetOutput(stderr)
	envFile := global.String("env-file", ".env", "Path to .env file")
	profile := global.String("profile", "", "Environment profile name")
	global.Usage = func() { printUsage(stderr) }
	if err := global.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return "", 0
		}
		return "", 2
	}
	remaining := global.Args()
	if len(remaining) == 0 {
		printUsage(stderr)
		return "", 2
	}
	command, commandArgs := remaining[0], remaining[1:]
	if command == "update" {
		return command, runUpdate(selfupdate.New(), version, selfupdate.ReplaceExecutable, stdout, stderr)
	}
	loaded, err := config.LoadEnvironment(*envFile, *profile)
	if err != nil {
		fmt.Fprintf(stderr, "error: %v\n", err)
		return command, 1
	}
	switch command {
	case "consume", "dump", "print":
		options, parseCode := parseConsumeOptions(command, commandArgs, stderr)
		if parseCode >= 0 {
			return command, parseCode
		}
		if options.verbose && len(loaded) > 0 {
			fmt.Fprintf(stdout, "Loaded environment files: %s\n", strings.Join(loaded, ", "))
		}
		if err := runConsume(command, options, stdout, stderr); err != nil {
			fmt.Fprintf(stderr, "error: %v\n", err)
			return command, 1
		}
		return command, 0
	case "topics":
		return command, runTopics(stdout, stderr)
	case "describe-topic":
		topicFlag, parseCode := parseDescribeTopicOptions(commandArgs, stderr)
		if parseCode >= 0 {
			return command, parseCode
		}
		return command, runDescribeTopic(topicFlag, stdout, stderr)
	default:
		fmt.Fprintf(stderr, "error: unknown command %q\n", command)
		printUsage(stderr)
		return command, 2
	}
}

func printUsage(output io.Writer) {
	fmt.Fprintln(output, "Unified Kafka consumer CLI (Go)")
	fmt.Fprintln(output, "Usage: kgrep [--env-file PATH] [--profile NAME] <consume|dump|print|topics|describe-topic|update> [options]")
}

// runUpdate downloads and installs the newest published release in place of
// the currently running executable.
func runUpdate(client *selfupdate.Client, version string, install func(path string, content []byte) error, stdout, stderr io.Writer) int {
	if version == "dev" {
		fmt.Fprintln(stderr, "error: kgrep update is not available for development builds; download a release directly from https://github.com/"+selfupdate.Repo+"/releases/latest")
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fmt.Fprintln(stdout, "Checking for updates...")
	release, err := client.Latest(ctx)
	if err != nil {
		fmt.Fprintf(stderr, "error: check for the latest release: %v\n", err)
		return 1
	}
	if !selfupdate.IsNewer(version, release.Tag) {
		fmt.Fprintf(stdout, "kgrep is already up to date (%s).\n", version)
		return 0
	}

	assetName := selfupdate.AssetName(runtime.GOOS, runtime.GOARCH)
	asset, ok := selfupdate.FindAsset(release, assetName)
	if !ok {
		fmt.Fprintf(stderr, "error: %s has no release asset for %s/%s\n", release.Tag, runtime.GOOS, runtime.GOARCH)
		return 1
	}
	checksumsAsset, ok := selfupdate.FindAsset(release, "checksums.txt")
	if !ok {
		fmt.Fprintf(stderr, "error: %s is missing checksums.txt\n", release.Tag)
		return 1
	}

	fmt.Fprintf(stdout, "Downloading kgrep %s for %s/%s...\n", release.Tag, runtime.GOOS, runtime.GOARCH)
	binary, err := client.Download(ctx, asset.URL)
	if err != nil {
		fmt.Fprintf(stderr, "error: download %s: %v\n", assetName, err)
		return 1
	}
	checksums, err := client.Download(ctx, checksumsAsset.URL)
	if err != nil {
		fmt.Fprintf(stderr, "error: download checksums.txt: %v\n", err)
		return 1
	}
	if err := selfupdate.VerifyChecksum(binary, assetName, checksums); err != nil {
		fmt.Fprintf(stderr, "error: %v; aborting update\n", err)
		return 1
	}

	executablePath, err := os.Executable()
	if err != nil {
		fmt.Fprintf(stderr, "error: locate the running executable: %v\n", err)
		return 1
	}
	if err := install(executablePath, binary); err != nil {
		fmt.Fprintf(stderr, "error: install update: %v (you may need to re-run with elevated permissions, or download manually from https://github.com/%s/releases/latest)\n", err, selfupdate.Repo)
		return 1
	}
	fmt.Fprintf(stdout, "Updated kgrep %s -> %s.\n", version, release.Tag)
	return 0
}

// checkForUpdate silently checks for a newer release and prints a one-line
// notice if one is available. Any failure (offline, rate-limited, etc.) is
// swallowed so it never disrupts the command that was actually requested.
func checkForUpdate(client *selfupdate.Client, version string, stderr io.Writer) {
	if version == "dev" || os.Getenv(skipUpdateCheckEnv) != "" {
		return
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	release, err := client.Latest(ctx)
	if err != nil {
		return
	}
	if selfupdate.IsNewer(version, release.Tag) {
		fmt.Fprintf(stderr, "\nA newer version of kgrep is available: %s (you have %s). Run 'kgrep update' to upgrade.\n", release.Tag, version)
	}
}

func parseConsumeOptions(command string, args []string, stderr io.Writer) (consumeOptions, int) {
	options := consumeOptions{matchMode: "key-or-value", keyFormat: "auto", valueFormat: "auto", timezone: "UTC", idlePolls: 30}
	flags := flag.NewFlagSet(command, flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&options.topic, "topic", "", "Kafka topic; defaults to KAFKA_DEFAULT_TOPIC")
	flags.StringVar(&options.allowedCSV, "allowed-keys-csv", "", "CSV file containing allowed values")
	flags.StringVar(&options.fromTime, "from-time", "", "ISO-8601 time or epoch seconds/ms")
	flags.StringVar(&options.toTime, "to-time", "", "ISO-8601 time or epoch seconds/ms")
	flags.IntVar(&options.maxMessages, "max-messages", 0, "Maximum messages to scan (0 means unlimited)")
	flags.StringVar(&options.outputCSV, "output-csv", "", "Write emitted records to CSV")
	flags.BoolVar(&options.readAll, "read-all", false, "Emit every record without filtering")
	flags.StringVar(&options.matchMode, "match-mode", "key-or-value", "kafka-key|decoded-key-field|value-field|key-or-value|key-and-value")
	flags.Var(&options.keyFields, "key-field", "Decoded key field path; repeatable")
	flags.Var(&options.valueFields, "value-field", "Decoded value field path; repeatable")
	flags.Var(&options.shortcuts, "field-shortcut", "Field shortcut; repeatable (delivery-identifiers)")
	flags.StringVar(&options.keyFormat, "key-format", "auto", "auto|json|avro|string|bytes")
	flags.StringVar(&options.valueFormat, "value-format", "auto", "auto|json|avro|string|bytes")
	flags.StringVar(&options.timezone, "timezone", "UTC", "Timezone for timestamps")
	flags.BoolVar(&options.printMatchesOnly, "print-matches-only", false, "Print concise match summaries")
	flags.IntVar(&options.idlePolls, "idle-polls", 30, "Consecutive idle polls before stopping")
	flags.BoolVar(&options.verbose, "verbose", false, "Print metadata and format decisions")
	if err := flags.Parse(args); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return options, 0
		}
		return options, 2
	}
	if flags.NArg() != 0 {
		fmt.Fprintf(stderr, "error: unexpected arguments: %s\n", strings.Join(flags.Args(), " "))
		return options, 2
	}
	if !slices.Contains(filter.MatchModes, options.matchMode) {
		fmt.Fprintf(stderr, "error: invalid --match-mode %q\n", options.matchMode)
		return options, 2
	}
	if !slices.Contains(decode.Formats, options.keyFormat) || !slices.Contains(decode.Formats, options.valueFormat) {
		fmt.Fprintln(stderr, "error: --key-format and --value-format must be auto, json, avro, string, or bytes")
		return options, 2
	}
	if options.maxMessages < 0 || options.idlePolls < 1 {
		fmt.Fprintln(stderr, "error: --max-messages must be non-negative and --idle-polls must be positive")
		return options, 2
	}
	for _, shortcut := range options.shortcuts {
		if shortcut != deliveryIdentifiers {
			fmt.Fprintf(stderr, "error: invalid --field-shortcut %q\n", shortcut)
			return options, 2
		}
	}
	return options, -1
}

func runConsume(command string, options consumeOptions, stdout, stderr io.Writer) error {
	settings, err := config.KafkaFromEnv()
	if err != nil {
		return err
	}
	if options.topic == "" {
		options.topic = settings.DefaultTopic
	}
	if options.topic == "" {
		return fmt.Errorf("--topic or KAFKA_DEFAULT_TOPIC is required")
	}
	allowed, err := csvutil.LoadAllowedValues(options.allowedCSV)
	if err != nil {
		return err
	}
	readAll := options.readAll || ((command == "dump" || command == "print") && len(allowed) == 0)
	keyFields, valueFields := resolvePaths(options)
	fromMS, err := timeutil.ParseMilliseconds(options.fromTime)
	if err != nil {
		return err
	}
	toMS, err := timeutil.ParseMilliseconds(options.toTime)
	if err != nil {
		return err
	}
	if fromMS != nil && toMS != nil && *fromMS > *toMS {
		return fmt.Errorf("--from-time must not be after --to-time")
	}
	deserializer := decode.New(options.topic, options.keyFormat, options.valueFormat, config.SchemaRegistryFromEnv(), options.verbose, stderr)
	consumer, err := kafkaclient.New(settings, options.topic, deserializer, fromMS, toMS, options.idlePolls, options.verbose, stderr)
	if err != nil {
		return err
	}
	var writer *csvutil.ResultWriter
	if options.outputCSV != "" {
		writer, err = csvutil.NewResultWriter(options.outputCSV)
		if err != nil {
			return err
		}
	}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	goodRecords, badRecords, emitted := 0, 0, 0
	err = consumer.Iterate(ctx, options.maxMessages, func(message core.DecodedMessage) error {
		goodRecords++
		result, err := filter.Evaluate(message, allowed, options.matchMode, keyFields, valueFields)
		if err != nil {
			return err
		}
		if !readAll && !result.Matched {
			return nil
		}
		emitted++
		row, err := RowForMessage(message, result, options.timezone)
		if err != nil {
			return err
		}
		if writer != nil {
			if err := writer.Write(row); err != nil {
				return err
			}
		}
		if command != "dump" || writer == nil {
			if options.printMatchesOnly {
				fmt.Fprintln(stdout, formatMatchSummary(row))
			} else {
				fmt.Fprintln(stdout, formatRow(row))
			}
		}
		return nil
	}, func(record core.BadRecord) error {
		badRecords++
		fmt.Fprintln(stdout, FormatBadRecord(record))
		return nil
	})
	if writer != nil {
		closeErr := writer.Close()
		if err == nil {
			err = closeErr
		}
	}
	if err != nil {
		return err
	}
	fmt.Fprintf(
		stdout,
		"Scanned: %d\nGood records: %d\nBad records: %d\nEmitted: %d\n",
		goodRecords+badRecords,
		goodRecords,
		badRecords,
		emitted,
	)
	return nil
}

func FormatBadRecord(record core.BadRecord) string {
	parts := make([]string, 0, 2)
	if record.KeyError != "" {
		parts = append(parts, "key_error="+singleLine(record.KeyError))
	}
	if record.ValueError != "" {
		parts = append(parts, "value_error="+singleLine(record.ValueError))
	}
	return fmt.Sprintf(
		"BAD %s[%d]@%d %s",
		record.Topic,
		record.Partition,
		record.Offset,
		strings.Join(parts, " "),
	)
}

func singleLine(value string) string {
	return strings.Join(strings.Fields(value), " ")
}

func resolvePaths(options consumeOptions) ([]string, []string) {
	keys := append([]string(nil), options.keyFields...)
	values := append([]string(nil), options.valueFields...)
	for range options.shortcuts {
		values = append(values, "Manifest.Pallets.Deliveries.DeliveryDocument", "Manifest.Pallets.Deliveries.Order")
	}
	return deduplicate(keys), deduplicate(values)
}

func deduplicate(values []string) []string {
	seen := make(map[string]struct{}, len(values))
	result := make([]string, 0, len(values))
	for _, value := range values {
		if _, exists := seen[value]; !exists {
			seen[value] = struct{}{}
			result = append(result, value)
		}
	}
	return result
}

func RowForMessage(message core.DecodedMessage, result core.MatchResult, timezone string) (map[string]string, error) {
	timestamp, err := timeutil.FormatMilliseconds(message.TimestampMS, timezone)
	if err != nil {
		return nil, err
	}
	displayKey := filter.RawKeyToText(message.RawKey, message.Key)
	if (message.KeyFormat == "avro" || message.KeyFormat == "json") && message.Key != nil {
		displayKey = csvutil.JSONSnippet(message.Key, 1000)
	}
	return map[string]string{
		"topic": message.Topic, "partition": strconv.Itoa(message.Partition), "offset": strconv.FormatInt(message.Offset, 10), "timestamp": timestamp,
		"kafka_key": displayKey, "key_format": message.KeyFormat, "value_format": message.ValueFormat,
		"matched_values": strings.Join(csvutil.SortedSet(result.AllMatches()), ", "), "matched_fields": formatMatchedFields(result.MatchedFields),
		"decoded_key_json": csvutil.JSONSnippet(message.Key, 5000), "decoded_value_json": csvutil.JSONSnippet(message.Value, 5000),
	}, nil
}

func formatMatchedFields(fields map[string]map[string]struct{}) string {
	names := make([]string, 0, len(fields))
	for name := range fields {
		names = append(names, name)
	}
	sort.Strings(names)
	parts := make([]string, 0, len(names))
	for _, name := range names {
		parts = append(parts, name+"="+strings.Join(csvutil.SortedSet(fields[name]), ", "))
	}
	return strings.Join(parts, "; ")
}

func formatRow(row map[string]string) string {
	fields := ""
	if row["matched_fields"] != "" {
		fields = " matched_fields=" + row["matched_fields"]
	}
	return fmt.Sprintf("%s[%s]@%s ts=%s key=%s matches=%s%s value=%s", row["topic"], row["partition"], row["offset"], row["timestamp"], row["kafka_key"], row["matched_values"], fields, row["decoded_value_json"])
}

func formatMatchSummary(row map[string]string) string {
	matched := row["matched_fields"]
	if matched == "" {
		matched = row["matched_values"]
	}
	return fmt.Sprintf("MATCH %s[%s]@%s ts=%s key=%s matched=%s", row["topic"], row["partition"], row["offset"], row["timestamp"], row["kafka_key"], matched)
}

func FormatDuration(duration time.Duration) string {
	seconds := duration.Seconds()
	if seconds < 1 {
		return fmt.Sprintf("%.0f ms", seconds*1000)
	}
	if seconds < 60 {
		return fmt.Sprintf("%.2f seconds", seconds)
	}
	minutes := int(seconds / 60)
	remainder := seconds - float64(minutes*60)
	if minutes < 60 {
		return fmt.Sprintf("%dm %.2fs", minutes, remainder)
	}
	hours := minutes / 60
	return fmt.Sprintf("%dh %dm %.2fs", hours, minutes%60, remainder)
}
