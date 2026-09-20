package a2a

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ChristopherDavenport/openresponses"
	"github.com/a2aproject/a2a-go/a2a"
)

// Metadata keys used on A2A messages.
const (
	// MetaCallerTools, on a user message, lists function tools the caller
	// executes itself: an array of Open Responses function tool objects.
	// When the model calls one, the task enters input-required with the
	// function_call items as data parts of the status message, and the
	// caller answers with a message whose data parts are the matching
	// function_call_output items.
	MetaCallerTools = "agentturn.caller_tools"
	// MetaPendingCalls, on an input-required status message, lists the
	// call IDs of the function calls the caller must answer: a JSON
	// array of strings on the wire, a []any of string values in process,
	// which is the form a JSON decode yields and the only one the SDK
	// stores.
	MetaPendingCalls = "agentturn.pending_calls"
)

// ItemsFromMessage converts an A2A message into Open Responses input.
// Text and file parts group into user messages; a data part carrying an
// object with a "type" key is decoded as an item through the registry,
// so a caller can send a function_call_output or any other item
// verbatim; other data parts become input_text carrying their JSON.
func ItemsFromMessage(msg *a2a.Message) (openresponses.Items, error) {
	var items openresponses.Items
	// buffered holds text and file parts until a data part or the end
	// closes the user message they form.
	var buffered openresponses.Contents
	flush := func() {
		if len(buffered) > 0 {
			items = append(items, &openresponses.Message{Role: openresponses.RoleUser, Content: buffered})
			buffered = nil
		}
	}
	for i, part := range msg.Parts {
		switch p := part.(type) {
		case a2a.TextPart:
			buffered = append(buffered, &openresponses.InputText{Text: p.Text})
		case a2a.FilePart:
			c, err := contentFromFile(p)
			if err != nil {
				return nil, fmt.Errorf("parts[%d]: %w", i, err)
			}
			buffered = append(buffered, c)
		case a2a.DataPart:
			raw, err := json.Marshal(p.Data)
			if err != nil {
				return nil, fmt.Errorf("parts[%d]: %w", i, err)
			}
			if _, ok := p.Data["type"].(string); ok {
				item, err := openresponses.UnmarshalItem(raw)
				if err != nil {
					return nil, fmt.Errorf("parts[%d]: %w", i, err)
				}
				flush()
				items = append(items, item)
				continue
			}
			buffered = append(buffered, &openresponses.InputText{Text: string(raw)})
		default:
			return nil, fmt.Errorf("parts[%d]: unsupported part %T", i, part)
		}
	}
	flush()
	return items, nil
}

// contentFromFile mirrors the file-part conversion in tools/a2a's
// fileParts; change both together.
func contentFromFile(p a2a.FilePart) (openresponses.Content, error) {
	switch f := p.File.(type) {
	case a2a.FileBytes:
		if strings.HasPrefix(f.MimeType, "image/") {
			return &openresponses.InputImage{ImageURL: "data:" + f.MimeType + ";base64," + f.Bytes}, nil
		}
		return &openresponses.InputFile{Filename: f.Name, FileData: f.Bytes}, nil
	case a2a.FileURI:
		if strings.HasPrefix(f.MimeType, "image/") {
			return &openresponses.InputImage{ImageURL: f.URI}, nil
		}
		return &openresponses.InputFile{Filename: f.Name, FileURL: f.URI}, nil
	}
	return nil, fmt.Errorf("unsupported file content %T", p.File)
}

// PartsFromItem converts an item into A2A parts for an agent message:
// the text of a message becomes a text part and any other item is sent
// verbatim as a data part.
func PartsFromItem(item openresponses.Item) ([]a2a.Part, error) {
	if m, ok := item.(*openresponses.Message); ok {
		var parts []a2a.Part
		for _, c := range m.Content {
			switch p := c.(type) {
			case *openresponses.OutputText:
				parts = append(parts, a2a.TextPart{Text: p.Text})
			case *openresponses.InputText:
				parts = append(parts, a2a.TextPart{Text: p.Text})
			case *openresponses.Refusal:
				parts = append(parts, a2a.TextPart{Text: p.Refusal})
			default:
				dp, err := dataPart(c)
				if err != nil {
					return nil, err
				}
				parts = append(parts, dp)
			}
		}
		return parts, nil
	}
	dp, err := dataPart(item)
	if err != nil {
		return nil, err
	}
	return []a2a.Part{dp}, nil
}

// dataPart encodes v, an item or a content part, as a data part.
func dataPart(v any) (a2a.DataPart, error) {
	raw, err := json.Marshal(v)
	if err != nil {
		return a2a.DataPart{}, fmt.Errorf("encode %T as a data part: %w", v, err)
	}
	var data map[string]any
	if err := json.Unmarshal(raw, &data); err != nil {
		return a2a.DataPart{}, fmt.Errorf("decode %T as a data part: %w", v, err)
	}
	return a2a.DataPart{Data: data}, nil
}

// callerTools reads the function tools a message declares under
// [MetaCallerTools].
func callerTools(msg *a2a.Message) ([]*openresponses.FunctionTool, error) {
	if msg == nil || msg.Metadata == nil {
		return nil, nil
	}
	v, ok := msg.Metadata[MetaCallerTools]
	if !ok || v == nil {
		return nil, nil
	}
	raw, err := json.Marshal(v)
	if err != nil {
		return nil, fmt.Errorf("%s: %w", MetaCallerTools, err)
	}
	var tools openresponses.Tools
	if err := json.Unmarshal(raw, &tools); err != nil {
		return nil, fmt.Errorf("%s: %w", MetaCallerTools, err)
	}
	out := make([]*openresponses.FunctionTool, 0, len(tools))
	for i, t := range tools {
		ft, ok := t.(*openresponses.FunctionTool)
		if !ok {
			return nil, fmt.Errorf("%s[%d]: only function tools are supported, got %q", MetaCallerTools, i, t.ToolType())
		}
		if ft.Name == "" {
			return nil, fmt.Errorf("%s[%d]: name is required", MetaCallerTools, i)
		}
		out = append(out, ft)
	}
	return out, nil
}
