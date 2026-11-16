package s3

import (
	"context"
	"encoding/xml"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"testing"

	"github.com/OpenListTeam/gofakes3"
)

func TestUploadPartCopyStoresSourceRange(t *testing.T) {
	ctx := context.Background()
	b, localRoot := setupMultipartBackend(t)

	src := "0123456789"
	if _, err := b.PutObject(ctx, "mp", "src.bin", map[string]string{}, strings.NewReader(src), int64(len(src))); err != nil {
		t.Fatalf("PutObject source: %+v", err)
	}
	uploadID, err := b.CreateMultipartUpload(ctx, "mp", "dst.bin", map[string]string{})
	if err != nil {
		t.Fatalf("CreateMultipartUpload: %+v", err)
	}

	next := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Errorf("request unexpectedly passed through: %s %s", r.Method, r.URL)
	})
	h := uploadPartCopyHandler(next, b, nil)

	// Part 1 copies bytes 2-5 of the source; part 2 copies the whole source.
	copyPart := func(partNumber int, rangeHdr string) string {
		t.Helper()
		req := httptest.NewRequest(http.MethodPut, "/mp/dst.bin?partNumber="+strconv.Itoa(partNumber)+"&uploadId="+string(uploadID), nil)
		req.Header.Set("X-Amz-Copy-Source", "/mp/src.bin")
		if rangeHdr != "" {
			req.Header.Set("X-Amz-Copy-Source-Range", rangeHdr)
		}
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)
		if rec.Code != http.StatusOK {
			t.Fatalf("UploadPartCopy %d status = %d, body %s", partNumber, rec.Code, rec.Body.String())
		}
		var res copyPartResult
		if err := xml.Unmarshal(rec.Body.Bytes(), &res); err != nil {
			t.Fatalf("decode CopyPartResult: %v", err)
		}
		return res.ETag
	}
	etag1 := copyPart(1, "bytes=2-5")
	etag2 := copyPart(2, "")

	_, _, err = b.CompleteMultipartUpload(ctx, "mp", "dst.bin", uploadID, &gofakes3.CompleteMultipartUploadRequest{
		Parts: []gofakes3.CompletedPart{
			{PartNumber: 1, ETag: etag1},
			{PartNumber: 2, ETag: etag2},
		},
	})
	if err != nil {
		t.Fatalf("CompleteMultipartUpload: %+v", err)
	}

	got, err := os.ReadFile(filepath.Join(localRoot, "dst.bin"))
	if err != nil {
		t.Fatalf("read destination: %v", err)
	}
	if want := "2345" + src; string(got) != want {
		t.Fatalf("destination = %q, want %q", got, want)
	}
}

func TestParseCopySource(t *testing.T) {
	cases := map[string]struct {
		bucket, object string
		ok             bool
	}{
		"/mp/dir/a.bin":           {"mp", "dir/a.bin", true},
		"mp/a%2Bb.bin":            {"mp", "a+b.bin", true},
		"/mp/a.bin?versionId=abc": {"mp", "a.bin", true},
		"/onlybucket":             {"", "", false},
	}
	for in, want := range cases {
		bucket, object, ok := parseCopySource(in)
		if bucket != want.bucket || object != want.object || ok != want.ok {
			t.Errorf("parseCopySource(%q) = (%q, %q, %v), want (%q, %q, %v)", in, bucket, object, ok, want.bucket, want.object, want.ok)
		}
	}
}

func TestParseCopySourceRange(t *testing.T) {
	r, err := parseCopySourceRange("bytes=2-5")
	if err != nil || r.Start != 2 || r.End != 5 {
		t.Fatalf("parseCopySourceRange = (%+v, %v), want start 2 end 5", r, err)
	}
	for _, bad := range []string{"bytes=5-2", "bytes=-5", "bytes=2-", "items=1-2", "bytes=a-b"} {
		if _, err := parseCopySourceRange(bad); err == nil {
			t.Errorf("parseCopySourceRange(%q) accepted an invalid range", bad)
		}
	}
}
