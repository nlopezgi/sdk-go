package event_test

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"

	"github.com/cloudevents/sdk-go/v2/event"

	"github.com/google/go-cmp/cmp"

	"github.com/cloudevents/sdk-go/v2/types"
)

//type DataExample struct {
//	AnInt   int        `json:"a,omitempty"`
//	AString string     `json:"b,omitempty"`
//	AnArray []string   `json:"c,omitempty"`
//	ATime   *time.Time `json:"e,omitempty"`
//}

func TestMarshal(t *testing.T) {
	now := types.Timestamp{Time: time.Now().UTC()}
	sourceUrl, _ := url.Parse("http://example.com/source")
	source := &types.URIRef{URL: *sourceUrl}
	sourceV1 := &types.URIRef{URL: *sourceUrl}

	schemaUrl, _ := url.Parse("http://example.com/schema")
	schema := &types.URIRef{URL: *schemaUrl}
	schemaV1 := &types.URI{URL: *schemaUrl}

	testCases := map[string]struct {
		event           event.Event
		eventExtensions map[string]interface{}
		want            []byte
		wantErr         *string
	}{
		"empty struct": {
			event:   event.Event{},
			wantErr: strptr("json: error calling MarshalJSON for type event.Event: every event conforming to the CloudEvents specification MUST include a context"),
		},
		"struct data v0.3": {
			event: func() event.Event {
				e := event.Event{
					Context: event.EventContextV03{
						Type:      "com.example.test",
						Source:    *source,
						SchemaURL: schema,
						ID:        "ABC-123",
						Time:      &now,
					}.AsV03(),
				}
				_ = e.SetData(event.ApplicationJSON, DataExample{
					AnInt:   42,
					AString: "testing",
				})
				return e
			}(),
			eventExtensions: map[string]interface{}{
				"exbool":   true,
				"exint":    int32(42),
				"exstring": "exstring",
				"exbinary": []byte{0, 1, 2, 3},
				"exurl":    source,
				"extime":   &now,
			},
			want: toBytes(map[string]interface{}{
				"specversion":     "0.3",
				"datacontenttype": "application/json",
				"data": map[string]interface{}{
					"a": 42,
					"b": "testing",
				},
				"id":        "ABC-123",
				"time":      now.Format(time.RFC3339Nano),
				"type":      "com.example.test",
				"exbool":    true,
				"exint":     42,
				"exstring":  "exstring",
				"exbinary":  "AAECAw==",
				"exurl":     "http://example.com/source",
				"extime":    now.Format(time.RFC3339Nano),
				"schemaurl": "http://example.com/schema",
				"source":    "http://example.com/source",
			}),
		},
		"nil data v0.3": {
			event: event.Event{
				Context: event.EventContextV03{
					Type:            "com.example.test",
					Source:          *source,
					SchemaURL:       schema,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
				}.AsV03(),
			},
			eventExtensions: map[string]interface{}{
				"exbool":   true,
				"exint":    int32(42),
				"exstring": "exstring",
				"exbinary": []byte{0, 1, 2, 3},
				"exurl":    source,
				"extime":   &now,
			},
			want: toBytes(map[string]interface{}{
				"specversion":     "0.3",
				"datacontenttype": "application/json",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"schemaurl":       "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
		},
		"string data v0.3": {
			event: func() event.Event {
				e := event.Event{
					Context: event.EventContextV03{
						Type:      "com.example.test",
						Source:    *source,
						SchemaURL: schema,
						ID:        "ABC-123",
						Time:      &now,
					}.AsV03(),
				}
				_ = e.SetData(event.ApplicationJSON, "This is a string.")
				return e
			}(),
			eventExtensions: map[string]interface{}{
				"exbool":   true,
				"exint":    int32(42),
				"exstring": "exstring",
				"exbinary": []byte{0, 1, 2, 3},
				"exurl":    source,
				"extime":   &now,
			},
			want: toBytes(map[string]interface{}{
				"specversion":     "0.3",
				"datacontenttype": "application/json",
				"data":            "This is a string.",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"schemaurl":       "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
		},
		"struct data v1.0": {
			event: func() event.Event {
				e := event.Event{
					Context: event.EventContextV1{
						Type:       "com.example.test",
						Source:     *sourceV1,
						DataSchema: schemaV1,
						ID:         "ABC-123",
						Time:       &now,
					}.AsV1(),
				}
				_ = e.SetData(event.ApplicationJSON, DataExample{
					AnInt:   42,
					AString: "testing",
				})
				return e
			}(),
			eventExtensions: map[string]interface{}{
				"exbool":   true,
				"exint":    int32(42),
				"exstring": "exstring",
				"exbinary": []byte{0, 1, 2, 3},
				"exurl":    sourceV1,
				"extime":   &now,
			},
			want: toBytes(map[string]interface{}{
				"specversion":     "1.0",
				"datacontenttype": "application/json",
				"data": map[string]interface{}{
					"a": 42,
					"b": "testing",
				},
				"id":         "ABC-123",
				"time":       now.Format(time.RFC3339Nano),
				"type":       "com.example.test",
				"exbool":     true,
				"exint":      42,
				"exstring":   "exstring",
				"exbinary":   "AAECAw==",
				"exurl":      "http://example.com/source",
				"extime":     now.Format(time.RFC3339Nano),
				"dataschema": "http://example.com/schema",
				"source":     "http://example.com/source",
			}),
		},
		"nil data v1.0": {
			event: event.Event{
				Context: event.EventContextV1{
					Type:            "com.example.test",
					Source:          *sourceV1,
					DataSchema:      schemaV1,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
				}.AsV1(),
			},
			eventExtensions: map[string]interface{}{
				"exbool":   true,
				"exint":    int32(42),
				"exstring": "exstring",
				"exbinary": []byte{0, 1, 2, 3},
				"exurl":    sourceV1,
				"extime":   &now,
			},
			want: toBytes(map[string]interface{}{
				"specversion":     "1.0",
				"datacontenttype": "application/json",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"dataschema":      "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
		},
		"string data v1.0": {
			event: func() event.Event {
				e := event.Event{
					Context: event.EventContextV1{
						Type:       "com.example.test",
						Source:     *sourceV1,
						DataSchema: schemaV1,
						ID:         "ABC-123",
						Time:       &now,
					}.AsV1(),
				}
				_ = e.SetData(event.ApplicationJSON, "This is a string.")
				return e
			}(),
			eventExtensions: map[string]interface{}{
				"exbool":   true,
				"exint":    int32(42),
				"exstring": "exstring",
				"exbinary": []byte{0, 1, 2, 3},
				"exurl":    sourceV1,
				"extime":   &now,
			},
			want: toBytes(map[string]interface{}{
				"specversion":     "1.0",
				"datacontenttype": "application/json",
				"data":            "This is a string.",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"dataschema":      "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {

			event := tc.event

			for k, v := range tc.eventExtensions {
				event.SetExtension(k, v)
			}

			gotBytes, err := json.Marshal(event)

			if tc.wantErr != nil || err != nil {
				if diff := cmp.Diff(*tc.wantErr, err.Error()); diff != "" {
					t.Errorf("unexpected error (-want, +got) = %v", diff)
				}
				return
			}

			// so we can understand the diff, turn bytes to strings
			want := string(tc.want)
			got := string(gotBytes)

			if diff := cmp.Diff(want, got); diff != "" {
				t.Errorf("unexpected event (-want, +got) = %v", diff)
			}
		})
	}
}

func TestUnmarshal(t *testing.T) {
	now := types.Timestamp{Time: time.Now().UTC()}
	sourceUrl, _ := url.Parse("http://example.com/source")
	source := &types.URIRef{URL: *sourceUrl}
	sourceV1 := &types.URIRef{URL: *sourceUrl}

	schemaUrl, _ := url.Parse("http://example.com/schema")
	schema := &types.URIRef{URL: *schemaUrl}
	schemaV1 := &types.URI{URL: *schemaUrl}

	testCases := map[string]struct {
		body    []byte
		want    *event.Event
		wantErr error
	}{
		"struct data v0.3": {
			body: toBytes(map[string]interface{}{
				"specversion":     "0.3",
				"datacontenttype": "application/json",
				"data": map[string]interface{}{
					"a": 42,
					"b": "testing",
				},
				"id":        "ABC-123",
				"time":      now.Format(time.RFC3339Nano),
				"type":      "com.example.test",
				"exbool":    true,
				"exint":     42,
				"exstring":  "exstring",
				"exbinary":  "AAECAw==",
				"exurl":     "http://example.com/source",
				"extime":    now.Format(time.RFC3339Nano),
				"schemaurl": "http://example.com/schema",
				"source":    "http://example.com/source",
			}),
			want: &event.Event{
				Context: event.EventContextV03{
					Type:            "com.example.test",
					Source:          *source,
					SchemaURL:       schema,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
					Extensions: map[string]interface{}{
						"exbool":   true, // Boolean should be preserved
						"exint":    int32(42),
						"exstring": "exstring",
						// Since byte, url and time are encoded as string, the unmarshal should just convert them to string
						"exbinary": "AAECAw==",
						"exurl":    "http://example.com/source",
						"extime":   now.Format(time.RFC3339Nano),
					},
				}.AsV03(),
				DataEncoded: toBytes(DataExample{
					AnInt:   42,
					AString: "testing",
				}),
			},
		},
		"string data v0.3": {
			body: toBytes(map[string]interface{}{
				"specversion":     "0.3",
				"datacontenttype": "application/json",
				"data":            "This is a string.",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"schemaurl":       "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
			want: &event.Event{
				Context: event.EventContextV03{
					Type:            "com.example.test",
					Source:          *source,
					SchemaURL:       schema,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
					Extensions: map[string]interface{}{
						"exbool":   true, // Boolean should be preserved
						"exint":    int32(42),
						"exstring": "exstring",
						// Since byte, url and time are encoded as string, the unmarshal should just convert them to string
						"exbinary": "AAECAw==",
						"exurl":    "http://example.com/source",
						"extime":   now.Format(time.RFC3339Nano),
					},
				}.AsV03(),
				DataEncoded: toBytes("This is a string."),
			},
		},
		"nil data v0.3": {
			body: toBytes(map[string]interface{}{
				"specversion":     "0.3",
				"datacontenttype": "application/json",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"schemaurl":       "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
			want: &event.Event{
				Context: event.EventContextV03{
					Type:            "com.example.test",
					Source:          *source,
					SchemaURL:       schema,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
					Extensions: map[string]interface{}{
						"exbool":   true, // Boolean should be preserved
						"exint":    int32(42),
						"exstring": "exstring",
						// Since byte, url and time are encoded as string, the unmarshal should just convert them to string
						"exbinary": "AAECAw==",
						"exurl":    "http://example.com/source",
						"extime":   now.Format(time.RFC3339Nano),
					},
				}.AsV03(),
			},
		},
		"struct data v1.0": {
			body: toBytes(map[string]interface{}{
				"specversion":     "1.0",
				"datacontenttype": "application/json",
				"data": map[string]interface{}{
					"a": 42,
					"b": "testing",
				},
				"id":         "ABC-123",
				"time":       now.Format(time.RFC3339Nano),
				"type":       "com.example.test",
				"exbool":     true,
				"exint":      42,
				"exstring":   "exstring",
				"exbinary":   "AAECAw==",
				"exurl":      "http://example.com/source",
				"extime":     now.Format(time.RFC3339Nano),
				"dataschema": "http://example.com/schema",
				"source":     "http://example.com/source",
			}),
			want: &event.Event{
				Context: event.EventContextV1{
					Type:            "com.example.test",
					Source:          *sourceV1,
					DataSchema:      schemaV1,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
					Extensions: map[string]interface{}{
						"exbool":   true, // Boolean should be preserved
						"exint":    int32(42),
						"exstring": "exstring",
						// Since byte, url and time are encoded as string, the unmarshal should just convert them to string
						"exbinary": "AAECAw==",
						"exurl":    "http://example.com/source",
						"extime":   now.Format(time.RFC3339Nano),
					},
				}.AsV1(),
				DataEncoded: toBytes(DataExample{
					AnInt:   42,
					AString: "testing",
				}),
			},
		},
		"string data v1.0": {
			body: toBytes(map[string]interface{}{
				"specversion":     "1.0",
				"datacontenttype": "application/json",
				"data":            "This is a string.",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"dataschema":      "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
			want: &event.Event{
				Context: event.EventContextV1{
					Type:            "com.example.test",
					Source:          *sourceV1,
					DataSchema:      schemaV1,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
					Extensions: map[string]interface{}{
						"exbool":   true, // Boolean should be preserved
						"exint":    int32(42),
						"exstring": "exstring",
						// Since byte, url and time are encoded as string, the unmarshal should just convert them to string
						"exbinary": "AAECAw==",
						"exurl":    "http://example.com/source",
						"extime":   now.Format(time.RFC3339Nano),
					},
				}.AsV1(),
				DataEncoded: toBytes("This is a string."),
			},
		},
		"nil data v1.0": {
			body: toBytes(map[string]interface{}{
				"specversion":     "1.0",
				"datacontenttype": "application/json",
				"id":              "ABC-123",
				"time":            now.Format(time.RFC3339Nano),
				"type":            "com.example.test",
				"exbool":          true,
				"exint":           42,
				"exstring":        "exstring",
				"exbinary":        "AAECAw==",
				"exurl":           "http://example.com/source",
				"extime":          now.Format(time.RFC3339Nano),
				"dataschema":      "http://example.com/schema",
				"source":          "http://example.com/source",
			}),
			want: &event.Event{
				Context: event.EventContextV1{
					Type:            "com.example.test",
					Source:          *sourceV1,
					DataSchema:      schemaV1,
					ID:              "ABC-123",
					Time:            &now,
					DataContentType: event.StringOfApplicationJSON(),
					Extensions: map[string]interface{}{
						"exbool":   true, // Boolean should be preserved
						"exint":    int32(42),
						"exstring": "exstring",
						// Since byte, url and time are encoded as string, the unmarshal should just convert them to string
						"exbinary": "AAECAw==",
						"exurl":    "http://example.com/source",
						"extime":   now.Format(time.RFC3339Nano),
					},
				}.AsV1(),
			},
		},
	}
	for n, tc := range testCases {
		t.Run(n, func(t *testing.T) {

			got := &event.Event{}
			err := json.Unmarshal(tc.body, got)

			if tc.wantErr != nil || err != nil {
				if diff := cmp.Diff(tc.wantErr, err); diff != "" {
					t.Errorf("unexpected error (-want, +got) = %v", diff)
				}
				return
			}

			if diff := cmp.Diff(tc.want, got); diff != "" {
				t.Errorf("unexpected event (-want, +got) = %v", diff)
			}
		})
	}
}

func toBytes(body interface{}) []byte {
	b, err := json.Marshal(body)
	if err != nil {
		return []byte(fmt.Sprintf(`{"error":%q}`, err.Error()))
	}
	return b
}
