package pubsub

import (
	"context"
	"io"
	"io/ioutil"

	"github.com/cloudevents/sdk-go/pkg/binding"
	"github.com/cloudevents/sdk-go/pkg/binding/format"
	"github.com/cloudevents/sdk-go/pkg/binding/spec"
	"github.com/cloudevents/sdk-go/pkg/types"
)

type pubsubMessagePublisher struct {
	Data       []byte
	attributes map[string]string
}

func (b *pubsubMessagePublisher) SetStructuredEvent(ctx context.Context, f format.Format, event io.Reader) error {
	val, err := ioutil.ReadAll(event)
	if err != nil {
		return err
	}
	b.Data = val
	return nil
}

func (b *pubsubMessagePublisher) Start(ctx context.Context) error {
	return nil
}

func (b *pubsubMessagePublisher) End(ctx context.Context) error {
	return nil
}

func (b *pubsubMessagePublisher) SetData(reader io.Reader) error {
	val, err := ioutil.ReadAll(reader)
	if err != nil {
		return err
	}
	b.Data = val
	return nil
}

func (b *pubsubMessagePublisher) SetAttribute(attribute spec.Attribute, value interface{}) error {
	// Everything is a string here
	s, err := types.Format(value)
	if err != nil {
		return err
	}

	if attribute.Kind() == spec.DataContentType {
		b.attributes[contentType] = s
	} else {
		b.attributes[prefix+attribute.Name()] = s
	}
	return nil
}

func (b *pubsubMessagePublisher) SetExtension(name string, value interface{}) error {
	s, err := types.Format(value)
	if err != nil {
		return err
	}
	b.attributes[prefix+name] = s
	return nil
}

var _ binding.StructuredWriter = (*pubsubMessagePublisher)(nil) // Test it conforms to the interface
var _ binding.BinaryWriter = (*pubsubMessagePublisher)(nil)     // Test it conforms to the interface
