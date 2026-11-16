package s3

import (
	"encoding/xml"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/OpenListTeam/gofakes3"
)

// uploadPartCopyHandler serves UploadPartCopy (PUT ?partNumber&uploadId with
// x-amz-copy-source). gofakes3 treats such a request as an ordinary part upload
// and stores an empty body, which would silently truncate the final object, so
// the copy is performed here by streaming the source range into UploadPart.
// Every other request is passed through unchanged.
func uploadPartCopyHandler(next http.Handler, b *s3Backend, authPairs map[string]string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		query := r.URL.Query()
		if r.Method != http.MethodPut || r.Header.Get("X-Amz-Copy-Source") == "" ||
			query.Get("uploadId") == "" || query.Get("partNumber") == "" {
			next.ServeHTTP(w, r)
			return
		}
		// Unauthenticated requests are rejected by next's auth middleware.
		if !s3RequestAuthorized(r, authPairs) {
			next.ServeHTTP(w, r)
			return
		}
		serveUploadPartCopy(w, r, b)
	})
}

func serveUploadPartCopy(w http.ResponseWriter, r *http.Request, b *s3Backend) {
	dstBucket, dstObject, ok := parseObjectPath(r.URL.Path)
	if !ok {
		writeS3Error(w, gofakes3.ErrInvalidURI)
		return
	}
	srcBucket, srcObject, ok := parseCopySource(r.Header.Get("X-Amz-Copy-Source"))
	if !ok {
		writeS3Error(w, gofakes3.ErrInvalidArgument)
		return
	}
	partNumber, err := strconv.Atoi(r.URL.Query().Get("partNumber"))
	if err != nil {
		writeS3Error(w, gofakes3.ErrInvalidPart)
		return
	}

	var rangeReq *gofakes3.ObjectRangeRequest
	if hdr := r.Header.Get("X-Amz-Copy-Source-Range"); hdr != "" {
		rangeReq, err = parseCopySourceRange(hdr)
		if err != nil {
			writeS3Error(w, gofakes3.ErrInvalidRange)
			return
		}
	}

	src, err := b.GetObject(r.Context(), srcBucket, srcObject, rangeReq)
	if err != nil {
		writeS3Error(w, err)
		return
	}
	defer func() { _ = src.Contents.Close() }()

	length := src.Size
	if src.Range != nil {
		length = src.Range.Length
	}
	etag, err := b.UploadPart(r.Context(), dstBucket, dstObject, gofakes3.UploadID(r.URL.Query().Get("uploadId")), partNumber, length, src.Contents)
	if err != nil {
		writeS3Error(w, err)
		return
	}

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(copyPartResult{
		LastModified: time.Now().UTC().Format(time.RFC3339Nano),
		ETag:         etag,
	})
}

// copyPartResult is the UploadPartCopy response body.
type copyPartResult struct {
	XMLName      xml.Name `xml:"CopyPartResult"`
	LastModified string   `xml:"LastModified"`
	ETag         string   `xml:"ETag"`
}

// parseCopySource splits "/bucket/key" (optionally URL-encoded and carrying a
// ?versionId suffix) into its bucket and key.
func parseCopySource(src string) (bucket, object string, ok bool) {
	src, _, _ = strings.Cut(src, "?")
	src, err := url.PathUnescape(src)
	if err != nil {
		return "", "", false
	}
	return parseObjectPath(path.Clean("/" + src))
}

// parseCopySourceRange accepts the "bytes=first-last" form of
// x-amz-copy-source-range, which is the only form S3 defines for UploadPartCopy.
func parseCopySourceRange(v string) (*gofakes3.ObjectRangeRequest, error) {
	spec, ok := strings.CutPrefix(v, "bytes=")
	if !ok {
		return nil, fmt.Errorf("unsupported range %q", v)
	}
	first, last, ok := strings.Cut(spec, "-")
	if !ok || first == "" || last == "" {
		return nil, fmt.Errorf("unsupported range %q", v)
	}
	start, err1 := strconv.ParseInt(first, 10, 64)
	end, err2 := strconv.ParseInt(last, 10, 64)
	if err1 != nil || err2 != nil || start < 0 || end < start {
		return nil, fmt.Errorf("invalid range %q", v)
	}
	return &gofakes3.ObjectRangeRequest{Start: start, End: end}, nil
}

// writeS3Error renders err as an S3 XML error response. Non-S3 errors become
// InternalError so internal details are not exposed to clients.
func writeS3Error(w http.ResponseWriter, err error) {
	var s3err interface{ ErrorCode() gofakes3.ErrorCode }
	code := gofakes3.ErrInternal
	message := ""
	if errors.As(err, &s3err) {
		code = s3err.ErrorCode()
		message = err.Error()
	}
	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(code.Status())
	_, _ = io.WriteString(w, xml.Header)
	_ = xml.NewEncoder(w).Encode(gofakes3.ErrorResponse{Code: code, Message: message})
}
