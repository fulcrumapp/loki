package main

import (
	"context"
	"testing"
	"time"

	"github.com/grafana/loki/v3/pkg/logproto"
	"github.com/prometheus/common/model"
	"github.com/stretchr/testify/require"
)

func TestParseSESEvent_UsesIngestionTimestampForDelivery(t *testing.T) {
	ctx := context.Background()
	logger := NewLogger("debug")
	prevBatchSize := batchSize
	batchSize = 131072
	t.Cleanup(func() {
		batchSize = prevBatchSize
	})

	b := &batch{
		streams: map[string]*logproto.Stream{},
	}

	event := &SESNotification{
		NotificationType: "Delivery",
		Delivery: &Delivery{
			Timestamp: "2020-01-01T00:00:00Z",
		},
		Mail: Mail{
			Timestamp: "2020-01-01T00:00:00Z",
			Source:    "sender@example.com",
		},
	}

	before := time.Now().UTC()
	err := parseSESEvent(ctx, b, logger, event)
	after := time.Now().UTC()

	require.NoError(t, err)
	require.Len(t, b.streams, 1)

	var gotEntry logproto.Entry
	for _, stream := range b.streams {
		require.Len(t, stream.Entries, 1)
		gotEntry = stream.Entries[0]
	}

	require.False(t, gotEntry.Timestamp.Before(before), "timestamp should be at/after ingestion start")
	require.False(t, gotEntry.Timestamp.After(after), "timestamp should be at/before ingestion end")
	require.Contains(t, gotEntry.Line, "ses_event_timestamp='2020-01-01T00:00:00Z'")
}

func TestParseSESEvent_AddsSourceLabelFromBatch(t *testing.T) {
	ctx := context.Background()
	logger := NewLogger("debug")
	prevBatchSize := batchSize
	batchSize = 131072
	t.Cleanup(func() {
		batchSize = prevBatchSize
	})

	b := &batch{
		streams: map[string]*logproto.Stream{},
		source:  "sqs",
	}

	event := &SESNotification{
		NotificationType: "Delivery",
		Delivery: &Delivery{
			Timestamp: "2020-01-01T00:00:00Z",
		},
		Mail: Mail{
			Timestamp: "2020-01-01T00:00:00Z",
			Source:    "sender@example.com",
		},
	}

	err := parseSESEvent(ctx, b, logger, event)
	require.NoError(t, err)
	require.Len(t, b.streams, 1)

	for labels := range b.streams {
		require.Contains(t, labels, string(model.LabelName("source"))+"=\"sqs\"")
	}
}
