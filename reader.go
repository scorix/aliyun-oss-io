package ossio

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type Reader struct {
	io.ReadSeekCloser

	ctx    context.Context
	bucket *oss.Bucket
	key    string

	// for implementing io.Seeker
	offset int64
	end    int64
}

func NewReader(ctx context.Context, bucket *oss.Bucket, objectKey string) (*Reader, error) {
	header, err := bucket.GetObjectDetailedMeta(objectKey, oss.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("get object metadata: %w", err)
	}

	contentLength, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse content length: %w", err)
	}

	return &Reader{ctx: ctx, bucket: bucket, key: objectKey, end: contentLength}, nil
}

// Implements io.Reader interface.
func (r *Reader) Read(p []byte) (int, error) {
	o, err := r.bucket.GetObject(
		r.key,
		oss.WithContext(r.ctx),
		oss.Range(r.offset, r.offset+int64(len(p))),
	)
	if err != nil {
		return 0, fmt.Errorf("get object: %w", err)
	}

	n, err := o.Read(p)
	r.offset += int64(n)

	return n, err
}

// Implements io.Closer interface.
func (r *Reader) Close() error {
	return nil
}

// Implements io.Seeker interface.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64
	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = r.offset + offset
	case io.SeekEnd:
		newOffset = r.end - offset
	default:
		return 0, fmt.Errorf("invalid whence value: %v", whence)
	}
	// validate seek position
	if newOffset < 0 || newOffset > r.end {
		return 0, fmt.Errorf("invalid seek position: %d", newOffset)
	}
	// set offset
	r.offset = newOffset

	return newOffset, nil
}
