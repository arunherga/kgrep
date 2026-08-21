package core

// DecodedMessage is the transport-neutral representation used by filtering and output.
type DecodedMessage struct {
	Topic       string
	Partition   int
	Offset      int64
	TimestampMS *int64
	RawKey      []byte
	RawValue    []byte
	Key         any
	Value       any
	KeyFormat   string
	ValueFormat string
}

// BadRecord identifies a Kafka record for which the key, value, or both could
// not be deserialized. Errors are stored separately so operators can identify
// which side of the record is malformed.
type BadRecord struct {
	Topic       string
	Partition   int
	Offset      int64
	TimestampMS *int64
	KeyError    string
	ValueError  string
}

type MatchResult struct {
	Matched           bool
	KafkaKeyMatches   map[string]struct{}
	DecodedKeyMatches map[string]struct{}
	ValueMatches      map[string]struct{}
	MatchedFields     map[string]map[string]struct{}
}

func (r MatchResult) AllMatches() map[string]struct{} {
	out := make(map[string]struct{})
	for _, values := range []map[string]struct{}{r.KafkaKeyMatches, r.DecodedKeyMatches, r.ValueMatches} {
		for value := range values {
			out[value] = struct{}{}
		}
	}
	return out
}
