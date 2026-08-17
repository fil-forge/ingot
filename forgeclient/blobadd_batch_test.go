package forgeclient

import (
	"context"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"

	assertcmds "github.com/fil-forge/libforge/commands/assert"
	blobcmds "github.com/fil-forge/libforge/commands/blob"
	"github.com/fil-forge/libforge/commands"
	ucancmds "github.com/fil-forge/libforge/commands/ucan"
	"github.com/fil-forge/ucantone/binding"
	"github.com/fil-forge/ucantone/ipld/codec/dagcbor"
	"github.com/fil-forge/ucantone/ipld/datamodel"
	"github.com/fil-forge/ucantone/multikey"
	"github.com/fil-forge/ucantone/multikey/ed25519"
	"github.com/fil-forge/ucantone/server"
	"github.com/fil-forge/ucantone/ucan/command"
	"github.com/fil-forge/ucantone/ucan/container"
	"github.com/fil-forge/ucantone/ucan/invocation"
	"github.com/fil-forge/ucantone/ucan/promise"
	"github.com/fil-forge/ucantone/ucan/receipt"
	"github.com/ipfs/go-cid"
	"github.com/multiformats/go-multihash"
)

// TestBlobConcludeBatchSingleRequest pins BlobConcludeBatch's core assumption
// against the real ucantone stack: N parked blobs conclude through ONE POST
// to the service — every conclude invocation and put receipt in one UCAN
// container, all executed by the server's container loop, all conclude
// receipts recovered from the one response — followed only by the per-blob
// accept-receipt fetches.
func TestBlobConcludeBatchSingleRequest(t *testing.T) {
	ctx := context.Background()
	agent, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate agent: %v", err)
	}
	svc, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate service: %v", err)
	}
	spaceIss, err := ed25519.GenerateIssuer()
	if err != nil {
		t.Fatalf("generate space: %v", err)
	}
	space := spaceIss.DID()

	// The service: a stock ucantone UCAN server with a conclude route that
	// records each executed invocation and checks its named put receipt is
	// present in the request container.
	var mu sync.Mutex
	var concluded []cid.Cid
	var missingReceipts int
	ucanSrv := server.NewHTTP(svc)
	route := ucancmds.Conclude.Route(func(req *binding.Request[*ucancmds.ConcludeArguments], res *binding.Response[*ucancmds.ConcludeOK]) error {
		found := false
		for _, r := range req.Metadata().Receipts() {
			if r.Link() == req.Task().Arguments().Receipt {
				found = true
				break
			}
		}
		mu.Lock()
		concluded = append(concluded, req.Task().Arguments().Receipt)
		if !found {
			missingReceipts++
		}
		mu.Unlock()
		return res.SetSuccess(&ucancmds.ConcludeOK{})
	})
	ucanSrv.Handle(route.Command, route.Handler)

	// Three parked blobs: a dummy put invocation per blob carrying the
	// derived signer key in its metadata (what synthesizePutReceipt reads),
	// and a pre-staged accept receipt + location commitment served by the
	// receipt endpoint (sprue stores these before answering the conclude,
	// so the first fetch always succeeds).
	receiptBodies := map[string][]byte{}
	var added []AddedBlob
	for _, name := range []string{"one", "two", "three"} {
		digest, err := multihash.Sum([]byte("blob-"+name), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("digest: %v", err)
		}
		derived, err := ed25519.Generate()
		if err != nil {
			t.Fatalf("generate derived key: %v", err)
		}
		keyID := multikey.KeyIssuer(derived).DID().String()
		putInv, err := invocation.Invoke(agent, agent.DID(), command.MustParse("/http/put"), nil,
			invocation.WithMetadata(datamodel.Map{
				"keys": datamodel.Map{"id": keyID, "keys": datamodel.Map{keyID: ed25519.Encode(derived)}},
			}))
		if err != nil {
			t.Fatalf("build put invocation: %v", err)
		}
		addTask := cid.NewCidV1(cid.Raw, digest)
		acceptDigest, err := multihash.Sum([]byte("accept-"+name), multihash.SHA2_256, -1)
		if err != nil {
			t.Fatalf("accept digest: %v", err)
		}
		acceptTask := cid.NewCidV1(cid.Raw, acceptDigest)
		added = append(added, AddedBlob{
			Digest:        digest,
			Size:          64,
			AddTask:       addTask,
			AcceptTask:    acceptTask,
			PutInvocation: putInv.Bytes(),
		})

		accRcpt, err := receipt.IssueOK(svc, acceptTask, &blobcmds.AcceptOK{
			Site: addTask,
			PDP:  promise.AwaitOK{Task: addTask},
		})
		if err != nil {
			t.Fatalf("issue accept receipt: %v", err)
		}
		locURL, err := url.Parse("http://piri.test/blob/" + name)
		if err != nil {
			t.Fatalf("parse loc url: %v", err)
		}
		locInv, err := assertcmds.Location.Invoke(svc, space, &assertcmds.LocationArguments{
			Space:    space,
			Content:  digest,
			Location: []commands.CborURL{commands.CborURL(*locURL)},
		})
		if err != nil {
			t.Fatalf("build location commitment: %v", err)
		}
		ct := container.New(container.WithReceipts(accRcpt), container.WithInvocations(locInv))
		var buf strings.Builder
		if err := ct.MarshalCBOR(&buf); err != nil {
			t.Fatalf("encode receipt container: %v", err)
		}
		receiptBodies[acceptTask.String()] = []byte(buf.String())
	}

	var posts int
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		posts++
		mu.Unlock()
		ucanSrv.ServeHTTP(w, r)
	})
	mux.HandleFunc("/receipt/", func(w http.ResponseWriter, r *http.Request) {
		task := strings.TrimPrefix(r.URL.Path, "/receipt/")
		body, ok := receiptBodies[task]
		if !ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", dagcbor.ContentType)
		_, _ = w.Write(body)
	})
	ts := httptest.NewServer(mux)
	t.Cleanup(ts.Close)

	tsURL, err := url.Parse(ts.URL)
	if err != nil {
		t.Fatalf("parse test server url: %v", err)
	}
	c, err := New(agent, svc.DID(), *tsURL)
	if err != nil {
		t.Fatalf("new client: %v", err)
	}

	results, err := c.BlobConcludeBatch(ctx, space, added)
	if err != nil {
		t.Fatalf("BlobConcludeBatch: %v", err)
	}
	for i, r := range results {
		if r.Err != nil {
			t.Fatalf("blob %d: %v", i, r.Err)
		}
		if r.Blob.Location == nil || r.Blob.Location.Command() != assertcmds.Location.Command {
			t.Fatalf("blob %d: location commitment missing from result", i)
		}
	}
	if posts != 1 {
		t.Fatalf("conclude POSTs = %d, want 1 (the whole batch in one request)", posts)
	}
	if len(concluded) != len(added) {
		t.Fatalf("server executed %d concludes, want %d", len(concluded), len(added))
	}
	if missingReceipts != 0 {
		t.Fatalf("%d concludes could not find their put receipt in the request container", missingReceipts)
	}
}
