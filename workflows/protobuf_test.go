package workflows

import (
	"encoding/json"
	"testing"

	"github.com/stretchr/testify/require"

	workflowstestpb "github.com/altipla-consulting/cloudtasks/workflows/testdata/gen/testdata/v1"
)

func newOpaqueMessage(name string) *workflowstestpb.OpaqueMessage {
	message := new(workflowstestpb.OpaqueMessage)
	message.SetName(name)
	return message
}

func TestNestedOpaqueProtobufReturn(t *testing.T) {
	type result struct {
		Messages []*workflowstestpb.OpaqueMessage
	}

	want := result{Messages: []*workflowstestpb.OpaqueMessage{
		newOpaqueMessage("one"),
		newOpaqueMessage("two"),
	}}
	raw, err := json.Marshal(want)
	require.NoError(t, err)
	require.NotContains(t, string(raw), "one")
	require.NotContains(t, string(raw), "two")

	protobufs, err := collectProtobufs(want)
	require.NoError(t, err)
	require.Len(t, protobufs, 2)
	require.Contains(t, protobufs, `["Messages","[0]"]`)
	require.Contains(t, protobufs, `["Messages","[1]"]`)

	var got result
	require.NoError(t, json.Unmarshal(raw, &got))
	require.Empty(t, got.Messages[0].GetName())
	require.Empty(t, got.Messages[1].GetName())
	require.NoError(t, restoreProtobufs(&got, protobufs))
	require.Equal(t, "one", got.Messages[0].GetName())
	require.Equal(t, "two", got.Messages[1].GetName())
}
