package acp

import (
	"errors"
	"fmt"

	"github.com/ChristopherDavenport/openresponses"
	"github.com/ironpark/acp-go/acp1"
)

// ErrEmptyPrompt is returned for a prompt with no content the loop can
// read.
var ErrEmptyPrompt = errors.New("front/acp: the prompt has no text, resource or image")

// message turns an ACP prompt into one user message: text blocks as
// input_text, an embedded text resource as input_text wrapped in a
// context element naming its URI, a resource link as a markdown link,
// and an image as input_image with a data URL. Audio and binary
// resources are refused, since the loop has nothing to put them in.
func message(prompt []acp1.ContentBlock) (*openresponses.Message, error) {
	var parts []openresponses.Content
	for i, block := range prompt {
		switch b := block.Variant().(type) {
		case acp1.ContentBlockText:
			parts = append(parts, &openresponses.InputText{Text: b.Text})
		case acp1.ContentBlockResourceLink:
			parts = append(parts, &openresponses.InputText{Text: fmt.Sprintf("[%s](%s)", b.Name, b.URI)})
		case acp1.ContentBlockResource:
			text, err := b.Resource.As[acp1.TextResourceContents]()
			if err != nil {
				return nil, fmt.Errorf("prompt block %d: only text resources are supported: %w", i, err)
			}
			parts = append(parts, &openresponses.InputText{Text: fmt.Sprintf("<context ref=%q>\n%s\n</context>", text.URI, text.Text)})
		case acp1.ContentBlockImage:
			parts = append(parts, &openresponses.InputImage{ImageURL: "data:" + b.MIMEType + ";base64," + b.Data})
		default:
			return nil, fmt.Errorf("prompt block %d: %q content is not supported", i, block.Variant().Tag())
		}
	}
	if len(parts) == 0 {
		return nil, ErrEmptyPrompt
	}
	return openresponses.UserMessage(parts...), nil
}
