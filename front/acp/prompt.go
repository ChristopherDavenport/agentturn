package acp

import (
	"errors"
	"fmt"

	"github.com/ChristopherDavenport/openresponses"
	"github.com/ironpark/acp-go/acp1"
	"github.com/ironpark/acp-go/acp2"
)

// ErrEmptyPrompt is returned for a prompt with no content the loop can
// read.
var ErrEmptyPrompt = errors.New("front/acp: the prompt has no text, resource or image")

// The prompt's blocks become one user message: text as input_text, an
// embedded text resource as input_text wrapped in a context element
// naming its URI, a resource link as a markdown link, and an image as
// input_image with a data URL. Audio and binary resources are refused,
// since the loop has nothing to put them in.

func linkPart(name, uri string) openresponses.Content {
	return &openresponses.InputText{Text: fmt.Sprintf("[%s](%s)", name, uri)}
}

func resourcePart(uri, text string) openresponses.Content {
	return &openresponses.InputText{Text: fmt.Sprintf("<context ref=%q>\n%s\n</context>", uri, text)}
}

func imagePart(mime, data string) openresponses.Content {
	return &openresponses.InputImage{ImageURL: "data:" + mime + ";base64," + data}
}

func userMessage(parts []openresponses.Content) (*openresponses.Message, error) {
	if len(parts) == 0 {
		return nil, ErrEmptyPrompt
	}
	return openresponses.UserMessage(parts...), nil
}

// messageV1 is the user message of a v1 prompt.
func messageV1(prompt []acp1.ContentBlock) (*openresponses.Message, error) {
	var parts []openresponses.Content
	for i, block := range prompt {
		switch b := block.Variant().(type) {
		case acp1.ContentBlockText:
			parts = append(parts, &openresponses.InputText{Text: b.Text})
		case acp1.ContentBlockResourceLink:
			parts = append(parts, linkPart(b.Name, b.URI))
		case acp1.ContentBlockResource:
			text, err := b.Resource.As[acp1.TextResourceContents]()
			if err != nil {
				return nil, fmt.Errorf("prompt block %d: only text resources are supported: %w", i, err)
			}
			parts = append(parts, resourcePart(text.URI, text.Text))
		case acp1.ContentBlockImage:
			parts = append(parts, imagePart(b.MIMEType, b.Data))
		default:
			return nil, fmt.Errorf("prompt block %d: %q content is not supported", i, block.Variant().Tag())
		}
	}
	return userMessage(parts)
}

// messageV2 is the user message of a v2 prompt.
func messageV2(prompt []acp2.ContentBlock) (*openresponses.Message, error) {
	var parts []openresponses.Content
	for i, block := range prompt {
		switch b := block.Variant().(type) {
		case acp2.ContentBlockText:
			parts = append(parts, &openresponses.InputText{Text: b.Text})
		case acp2.ContentBlockResourceLink:
			parts = append(parts, linkPart(b.Name, b.URI))
		case acp2.ContentBlockResource:
			text, err := b.Resource.As[acp2.TextResourceContents]()
			if err != nil {
				return nil, fmt.Errorf("prompt block %d: only text resources are supported: %w", i, err)
			}
			parts = append(parts, resourcePart(text.URI, text.Text))
		case acp2.ContentBlockImage:
			parts = append(parts, imagePart(string(b.MIMEType), b.Data))
		default:
			return nil, fmt.Errorf("prompt block %d: %q content is not supported", i, block.Variant().Tag())
		}
	}
	return userMessage(parts)
}
