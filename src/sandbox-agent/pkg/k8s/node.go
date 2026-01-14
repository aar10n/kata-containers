package k8s

import (
	"context"
	"encoding/json"
	"fmt"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/types"
	"k8s.io/client-go/kubernetes"
)

// NodeLabeler handles adding and removing labels on nodes.
type NodeLabeler struct {
	clientset *kubernetes.Clientset
	nodeName  string
	labelKey  string
	labelVal  string
}

// NewNodeLabeler creates a new NodeLabeler for the given node.
func NewNodeLabeler(clientset *kubernetes.Clientset, nodeName, labelKey, labelVal string) *NodeLabeler {
	return &NodeLabeler{
		clientset: clientset,
		nodeName:  nodeName,
		labelKey:  labelKey,
		labelVal:  labelVal,
	}
}

// LabelNode adds the configured label to the node.
func (n *NodeLabeler) LabelNode(ctx context.Context) error {
	patch := map[string]interface{}{
		"metadata": map[string]interface{}{
			"labels": map[string]string{
				n.labelKey: n.labelVal,
			},
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	_, err = n.clientset.CoreV1().Nodes().Patch(
		ctx,
		n.nodeName,
		types.StrategicMergePatchType,
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch node %s: %w", n.nodeName, err)
	}

	return nil
}

// UnlabelNode removes the configured label from the node.
func (n *NodeLabeler) UnlabelNode(ctx context.Context) error {
	// Use a JSON patch to remove the label
	patch := []map[string]interface{}{
		{
			"op":   "remove",
			"path": fmt.Sprintf("/metadata/labels/%s", escapeJSONPointer(n.labelKey)),
		},
	}

	patchBytes, err := json.Marshal(patch)
	if err != nil {
		return fmt.Errorf("marshal patch: %w", err)
	}

	_, err = n.clientset.CoreV1().Nodes().Patch(
		ctx,
		n.nodeName,
		types.JSONPatchType,
		patchBytes,
		metav1.PatchOptions{},
	)
	if err != nil {
		return fmt.Errorf("patch node %s: %w", n.nodeName, err)
	}

	return nil
}

// escapeJSONPointer escapes special characters in JSON pointer paths.
// Per RFC 6901, ~ must be escaped as ~0 and / as ~1.
func escapeJSONPointer(s string) string {
	result := ""
	for _, c := range s {
		switch c {
		case '~':
			result += "~0"
		case '/':
			result += "~1"
		default:
			result += string(c)
		}
	}
	return result
}
