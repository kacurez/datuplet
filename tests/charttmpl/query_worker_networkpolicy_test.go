package charttmpl

import (
	"os/exec"
	"reflect"
	"strings"
	"testing"

	networkingv1 "k8s.io/api/networking/v1"
	"sigs.k8s.io/yaml"
)

const queryWorkerPolicyTemplate = "templates/query-worker/networkpolicy.yaml"

// Compare the whole spec so extra egress cannot replace existing rules or
// accidentally widen ingress or the worker selector.
func assertQueryWorkerPolicy(t *testing.T, rendered string, extraRules string) {
	t.Helper()
	var got, want networkingv1.NetworkPolicy
	if err := yaml.Unmarshal([]byte(rendered), &got); err != nil {
		t.Fatal(err)
	}
	expected := `spec:
  podSelector:
    matchLabels:
      app.kubernetes.io/instance: release-name
      app.kubernetes.io/part-of: datuplet
      app.kubernetes.io/name: query-worker
  policyTypes: [Ingress, Egress]
  ingress:
    - from:
        - podSelector:
            matchLabels:
              app.kubernetes.io/part-of: datuplet
              app.kubernetes.io/name: pipeline-api
      ports:
        - {protocol: TCP, port: 8090}
  egress:
    - to:
        - namespaceSelector:
            matchLabels:
              kubernetes.io/metadata.name: kube-system
          podSelector:
            matchLabels:
              k8s-app: kube-dns
      ports:
        - {protocol: UDP, port: 53}
        - {protocol: TCP, port: 53}
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/part-of: datuplet
              app.kubernetes.io/name: lakekeeper
      ports:
        - {protocol: TCP, port: 8181}
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/part-of: datuplet
              app.kubernetes.io/name: minio
      ports:
        - {protocol: TCP, port: 9000}
    - to:
        - podSelector:
            matchLabels:
              app.kubernetes.io/part-of: datuplet
              app.kubernetes.io/name: pipeline-api
      ports:
        - {protocol: TCP, port: 8081}
` + extraRules
	if err := yaml.Unmarshal([]byte(expected), &want); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(got.Spec, want.Spec) {
		t.Errorf("query-worker policy spec differs (-want +got):\nwant: %+v\ngot: %+v", want.Spec, got.Spec)
	}
}

func TestQueryWorkerChart_DefaultEgressUnchanged(t *testing.T) {
	for _, args := range [][]string{nil, {"--set-json", "queryWorker.extraEgress=[]"}} {
		assertQueryWorkerPolicy(t, renderShowOnly(t, queryWorkerPolicyTemplate, args...), "")
	}
}

func TestQueryWorkerChart_CustomEndpointExtraEgress(t *testing.T) {
	rules := `[{"to":[{"ipBlock":{"cidr":"203.0.113.10/32"}}],"ports":[{"protocol":"TCP","port":9443}]}]`
	assertQueryWorkerPolicy(t, renderShowOnly(t, queryWorkerPolicyTemplate,
		"--set-json", "queryWorker.extraEgress="+rules), `    - to:
        - ipBlock:
            cidr: 203.0.113.10/32
      ports:
        - {protocol: TCP, port: 9443}
`)
}

func TestQueryWorkerChart_DisabledRendersNoPolicy(t *testing.T) {
	out, err := exec.Command(helmOrSkip(t), "template", chartDir(t),
		"--set", "queryWorker.enabled=false", "--set", "appWorker.enabled=false",
		"--set-json", `queryWorker.extraEgress=[{"ports":[{"protocol":"TCP","port":443}]}]`,
	).CombinedOutput()
	if err != nil {
		t.Fatalf("helm template failed: %v\n%s", err, out)
	}
	if strings.Contains(string(out), "# Source: datuplet-app/"+queryWorkerPolicyTemplate) {
		t.Fatal("disabled query-worker still renders its NetworkPolicy")
	}
}

func TestQueryWorkerChart_PublicHTTPSExtraEgress(t *testing.T) {
	rules := `[{"to":[{"ipBlock":{"cidr":"0.0.0.0/0","except":["10.0.0.0/8","172.16.0.0/12","192.168.0.0/16"]}}],"ports":[{"protocol":"TCP","port":443}]}]`
	rendered := renderShowOnly(t, queryWorkerPolicyTemplate,
		"--set-json", "queryWorker.extraEgress="+rules)
	assertQueryWorkerPolicy(t, rendered, `    - to:
        - ipBlock:
            cidr: 0.0.0.0/0
            except: [10.0.0.0/8, 172.16.0.0/12, 192.168.0.0/16]
      ports:
        - {protocol: TCP, port: 443}
`)
}
