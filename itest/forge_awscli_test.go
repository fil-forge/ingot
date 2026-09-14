//go:build itest

package itest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/moby/moby/api/types/container"
	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/wait"
)

// awsCLIImage pins the unmodified AWS CLI v2 the round trip is driven with.
// Bump deliberately: the CLI's defaults are the surface under test — the
// 8 MiB multipart switchover and the request checksums it sends unasked
// (CRC64NVME declared on CreateMultipartUpload and carried per part, as of
// this version; earlier 2.23+ releases sent CRC32).
const awsCLIImage = "amazon/aws-cli:2.36.44"

// TestForgeAWSCLI drives a real, unmodified `aws s3 cp` against the
// forge-mode ingot: a 20 MiB object crosses the CLI's 8 MiB multipart
// switchover, so the CLI itself chooses multipart (three 8 MiB parts) and
// sends its default per-part checksums — the client behavior the Go SDK
// conformance tables cannot stand in for. The CLI runs in its official
// image, reaching ingot's host-mapped port through host.docker.internal;
// the uploaded bytes come back byte-exact, HEAD reports a 3-part ETag, and
// the object carries the full-object checksum the CLI declared.
func TestForgeAWSCLI(t *testing.T) {
	s, endpoint := forgeStack(t)
	ctx := t.Context()
	accessKey, secretKey := hiltProvisionTenant(t, ctx, s, "awscli")

	// The CLI container sees the host's mapped ingot port, not 127.0.0.1.
	ep, err := url.Parse(endpoint)
	if err != nil {
		t.Fatalf("parse ingot endpoint %q: %v", endpoint, err)
	}
	containerEndpoint := "http://host.docker.internal:" + ep.Port()

	const size = 20 << 20
	const bucket, key = "awscli", "big/obj.bin"
	want := patternBytes(size)
	work := t.TempDir()
	inPath := filepath.Join(work, "in.bin")
	if err := os.WriteFile(inPath, want, 0o644); err != nil {
		t.Fatalf("write input: %v", err)
	}

	// Path-style addressing: the CLI defaults to virtual-host style for a
	// hostname endpoint, and bucket.host.docker.internal does not resolve.
	// Everything else is the CLI's own default behavior.
	script := strings.Join([]string{
		"set -eu",
		"exec > /work/run.log 2>&1",
		"aws configure set default.s3.addressing_style path",
		fmt.Sprintf("aws --endpoint-url %q s3 mb s3://%s", containerEndpoint, bucket),
		fmt.Sprintf("aws --endpoint-url %q s3 cp /work/in.bin s3://%s/%s", containerEndpoint, bucket, key),
		fmt.Sprintf("aws --endpoint-url %q s3api head-object --bucket %s --key %s --checksum-mode ENABLED > /work/head.json", containerEndpoint, bucket, key),
		fmt.Sprintf("aws --endpoint-url %q s3 cp s3://%s/%s /work/out.bin", containerEndpoint, bucket, key),
		"md5sum /work/in.bin /work/out.bin",
	}, "\n")

	t.Logf("running %s against %s", awsCLIImage, containerEndpoint)
	cli, err := testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
		ContainerRequest: testcontainers.ContainerRequest{
			Image:      awsCLIImage,
			Entrypoint: []string{"/bin/sh"},
			Cmd:        []string{"-c", script},
			Env: map[string]string{
				"AWS_ACCESS_KEY_ID":         accessKey,
				"AWS_SECRET_ACCESS_KEY":     secretKey,
				"AWS_DEFAULT_REGION":        forgeRegion,
				"AWS_EC2_METADATA_DISABLED": "true",
				"AWS_PAGER":                 "",
			},
			Files: []testcontainers.ContainerFile{{
				HostFilePath:      inPath,
				ContainerFilePath: "/work/in.bin",
				FileMode:          0o644,
			}},
			HostConfigModifier: func(hc *container.HostConfig) {
				// Docker Desktop resolves host.docker.internal on its own;
				// Docker Engine (CI) needs the host-gateway alias.
				hc.ExtraHosts = append(hc.ExtraHosts, "host.docker.internal:host-gateway")
			},
			WaitingFor: wait.ForExit().WithExitTimeout(10 * time.Minute),
		},
		Started: true,
	})
	if cli != nil {
		t.Cleanup(func() { _ = cli.Terminate(context.Background()) })
	}
	if err != nil {
		t.Fatalf("run aws-cli container: %v", err)
	}
	state, err := cli.State(ctx)
	if err != nil {
		t.Fatalf("aws-cli container state: %v", err)
	}
	logs := string(fileFromContainer(t, ctx, cli, "/work/run.log"))
	if state.ExitCode != 0 {
		t.Fatalf("aws-cli script exited %d:\n%s", state.ExitCode, logs)
	}
	t.Logf("aws-cli output:\n%s", logs)

	got := fileFromContainer(t, ctx, cli, "/work/out.bin")
	if !bytes.Equal(got, want) {
		t.Fatalf("aws s3 cp round trip mismatch: got %d bytes, want %d", len(got), len(want))
	}

	var head struct {
		ContentLength     int64  `json:"ContentLength"`
		ETag              string `json:"ETag"`
		ChecksumCRC64NVME string `json:"ChecksumCRC64NVME"`
		ChecksumType      string `json:"ChecksumType"`
	}
	headJSON := fileFromContainer(t, ctx, cli, "/work/head.json")
	if err := json.Unmarshal(headJSON, &head); err != nil {
		t.Fatalf("parse head-object output: %v", err)
	}
	t.Logf("head-object:\n%s", headJSON)
	if head.ContentLength != size {
		t.Fatalf("head-object ContentLength = %d, want %d", head.ContentLength, size)
	}
	// 20 MiB at the CLI's default 8 MiB part size is three parts: the CLI
	// chose multipart on its own, and ingot completed it.
	if et := strings.Trim(head.ETag, `"`); !strings.HasSuffix(et, "-3") {
		t.Fatalf("head-object ETag = %q, want a 3-part multipart ETag", et)
	}
	// The CLI declared CRC64NVME on CreateMultipartUpload and sent a value
	// per part without being asked; ingot must have accepted each and
	// recorded the full-object checksum. A bare FULL_OBJECT type with no
	// value would mean ingot fell back to its own derived checksum.
	if head.ChecksumCRC64NVME == "" || head.ChecksumType != "FULL_OBJECT" {
		t.Fatalf("head-object checksum = %q/%q, want the CLI's default full-object CRC64NVME", head.ChecksumCRC64NVME, head.ChecksumType)
	}
}

// fileFromContainer copies one file out of a (possibly exited) container.
func fileFromContainer(t *testing.T, ctx context.Context, c testcontainers.Container, path string) []byte {
	t.Helper()
	rc, err := c.CopyFileFromContainer(ctx, path)
	if err != nil {
		t.Fatalf("copy %s from container: %v", path, err)
	}
	defer rc.Close()
	b, err := io.ReadAll(rc)
	if err != nil {
		t.Fatalf("read %s from container: %v", path, err)
	}
	return b
}
