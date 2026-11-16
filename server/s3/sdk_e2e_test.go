package s3

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strconv"
	"testing"

	"github.com/OpenListTeam/OpenList/v4/internal/conf"
	"github.com/OpenListTeam/OpenList/v4/internal/model"
	"github.com/OpenListTeam/OpenList/v4/internal/op"
	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	awss3 "github.com/aws/aws-sdk-go/service/s3"
	"github.com/aws/aws-sdk-go/service/s3/s3manager"
)

const (
	e2eAccessKey = "e2e-access"
	e2eSecretKey = "e2e-secret"
)

// newSDKTestClient mounts a local storage, exposes it as bucket "mp" through the
// real NewServer handler chain (with SigV4 auth enabled), and returns an AWS SDK
// client pointed at it.
func newSDKTestClient(t *testing.T) (*awss3.S3, string) {
	t.Helper()
	ctx := context.Background()
	_, localRoot := setupMultipartBackend(t)

	mount := "/e2e-" + sanitizeTestName(t.Name())
	addition, _ := json.Marshal(map[string]any{"root_folder_path": localRoot, "thumbnail": false})
	if _, err := op.CreateStorage(ctx, model.Storage{Driver: "Local", MountPath: mount, Addition: string(addition)}); err != nil {
		t.Fatalf("create local storage: %+v", err)
	}
	buckets, _ := json.Marshal([]Bucket{{Name: "mp", Path: mount}})
	if err := op.SaveSettingItem(&model.SettingItem{Key: conf.S3Buckets, Value: string(buckets)}); err != nil {
		t.Fatalf("save buckets: %+v", err)
	}
	if err := op.SaveSettingItem(&model.SettingItem{Key: conf.S3AccessKeyId, Value: e2eAccessKey}); err != nil {
		t.Fatalf("save access key: %+v", err)
	}
	if err := op.SaveSettingItem(&model.SettingItem{Key: conf.S3SecretAccessKey, Value: e2eSecretKey}); err != nil {
		t.Fatalf("save secret key: %+v", err)
	}

	h, err := NewServer(ctx)
	if err != nil {
		t.Fatalf("NewServer: %v", err)
	}
	srv := httptest.NewServer(h)
	t.Cleanup(srv.Close)

	sess, err := session.NewSession(&aws.Config{
		Credentials:      credentials.NewStaticCredentials(e2eAccessKey, e2eSecretKey, ""),
		Endpoint:         aws.String(srv.URL),
		Region:           aws.String("us-east-1"),
		S3ForcePathStyle: aws.Bool(true),
		MaxRetries:       aws.Int(0),
	})
	if err != nil {
		t.Fatalf("session: %v", err)
	}
	return awss3.New(sess), localRoot
}

func randomBytes(t *testing.T, n int) []byte {
	t.Helper()
	b := make([]byte, n)
	if _, err := rand.Read(b); err != nil {
		t.Fatal(err)
	}
	return b
}

func TestSDKMultipartUploadThroughGateway(t *testing.T) {
	client, localRoot := newSDKTestClient(t)
	// 12 MiB with 5 MiB parts gives 3 parts, the last one smaller than the rest.
	data := randomBytes(t, 12*1024*1024)

	up := s3manager.NewUploaderWithClient(client, func(u *s3manager.Uploader) {
		u.PartSize = 5 * 1024 * 1024
		u.Concurrency = 3
	})
	if _, err := up.Upload(&s3manager.UploadInput{
		Bucket: aws.String("mp"),
		Key:    aws.String("dir/big.bin"),
		Body:   bytes.NewReader(data),
	}); err != nil {
		t.Fatalf("multipart upload: %v", err)
	}
	assertFileEquals(t, filepath.Join(localRoot, "dir", "big.bin"), data)
}

func TestSDKUploadPartCopyThroughGateway(t *testing.T) {
	client, localRoot := newSDKTestClient(t)
	src := randomBytes(t, 6*1024*1024)
	if _, err := client.PutObject(&awss3.PutObjectInput{
		Bucket: aws.String("mp"), Key: aws.String("src.bin"), Body: bytes.NewReader(src),
	}); err != nil {
		t.Fatalf("put source: %v", err)
	}

	created, err := client.CreateMultipartUpload(&awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp"), Key: aws.String("copy.bin"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := created.UploadId

	half := len(src) / 2
	p1, err := client.UploadPartCopy(&awss3.UploadPartCopyInput{
		Bucket: aws.String("mp"), Key: aws.String("copy.bin"), UploadId: id, PartNumber: aws.Int64(1),
		CopySource: aws.String("mp/src.bin"), CopySourceRange: aws.String("bytes=0-" + strconv.Itoa(half-1)),
	})
	if err != nil {
		t.Fatalf("UploadPartCopy 1: %v", err)
	}
	p2, err := client.UploadPartCopy(&awss3.UploadPartCopyInput{
		Bucket: aws.String("mp"), Key: aws.String("copy.bin"), UploadId: id, PartNumber: aws.Int64(2),
		CopySource: aws.String("mp/src.bin"), CopySourceRange: aws.String("bytes=" + strconv.Itoa(half) + "-" + strconv.Itoa(len(src)-1)),
	})
	if err != nil {
		t.Fatalf("UploadPartCopy 2: %v", err)
	}
	if _, err := client.CompleteMultipartUpload(&awss3.CompleteMultipartUploadInput{
		Bucket: aws.String("mp"), Key: aws.String("copy.bin"), UploadId: id,
		MultipartUpload: &awss3.CompletedMultipartUpload{Parts: []*awss3.CompletedPart{
			{PartNumber: aws.Int64(1), ETag: p1.CopyPartResult.ETag},
			{PartNumber: aws.Int64(2), ETag: p2.CopyPartResult.ETag},
		}},
	}); err != nil {
		t.Fatalf("complete: %v", err)
	}
	assertFileEquals(t, filepath.Join(localRoot, "copy.bin"), src)
}

func TestSDKListAndAbortThroughGateway(t *testing.T) {
	client, _ := newSDKTestClient(t)
	created, err := client.CreateMultipartUpload(&awss3.CreateMultipartUploadInput{
		Bucket: aws.String("mp"), Key: aws.String("pending.bin"),
	})
	if err != nil {
		t.Fatalf("create: %v", err)
	}
	id := created.UploadId

	if _, err := client.UploadPart(&awss3.UploadPartInput{
		Bucket: aws.String("mp"), Key: aws.String("pending.bin"), UploadId: id, PartNumber: aws.Int64(1),
		Body: bytes.NewReader([]byte("part-one")),
	}); err != nil {
		t.Fatalf("upload part: %v", err)
	}

	parts, err := client.ListParts(&awss3.ListPartsInput{
		Bucket: aws.String("mp"), Key: aws.String("pending.bin"), UploadId: id,
	})
	if err != nil {
		t.Fatalf("ListParts: %v", err)
	}
	if len(parts.Parts) != 1 || aws.Int64Value(parts.Parts[0].Size) != int64(len("part-one")) {
		t.Fatalf("ListParts = %+v, want one part of 8 bytes", parts.Parts)
	}

	uploads, err := client.ListMultipartUploads(&awss3.ListMultipartUploadsInput{Bucket: aws.String("mp")})
	if err != nil {
		t.Fatalf("ListMultipartUploads: %v", err)
	}
	if len(uploads.Uploads) != 1 || aws.StringValue(uploads.Uploads[0].UploadId) != aws.StringValue(id) {
		t.Fatalf("ListMultipartUploads = %+v, want the pending upload", uploads.Uploads)
	}

	if _, err := client.AbortMultipartUpload(&awss3.AbortMultipartUploadInput{
		Bucket: aws.String("mp"), Key: aws.String("pending.bin"), UploadId: id,
	}); err != nil {
		t.Fatalf("abort: %v", err)
	}
	if _, err := client.ListParts(&awss3.ListPartsInput{
		Bucket: aws.String("mp"), Key: aws.String("pending.bin"), UploadId: id,
	}); err == nil {
		t.Fatal("ListParts succeeded after abort, want NoSuchUpload")
	}
}

func TestSDKCopyObjectThroughGateway(t *testing.T) {
	client, localRoot := newSDKTestClient(t)
	src := randomBytes(t, 64*1024)
	if _, err := client.PutObject(&awss3.PutObjectInput{
		Bucket: aws.String("mp"), Key: aws.String("a/src.bin"), Body: bytes.NewReader(src),
	}); err != nil {
		t.Fatalf("put source: %v", err)
	}
	if _, err := client.CopyObject(&awss3.CopyObjectInput{
		Bucket: aws.String("mp"), Key: aws.String("b/dst.bin"), CopySource: aws.String("mp/a/src.bin"),
	}); err != nil {
		t.Fatalf("CopyObject: %v", err)
	}
	assertFileEquals(t, filepath.Join(localRoot, "b", "dst.bin"), src)
}

func assertFileEquals(t *testing.T, path string, want []byte) {
	t.Helper()
	got, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read %s: %v", path, err)
	}
	if !bytes.Equal(got, want) {
		t.Fatalf("%s: got %d bytes, want %d; content differs", path, len(got), len(want))
	}
}
