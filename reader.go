package ossio

import (
	"context"
	"fmt"
	"io"
	"strconv"

	"github.com/aliyun/aliyun-oss-go-sdk/oss"
)

type object struct {
	object        io.ReadCloser
	contentLength int64

	// for implementing io.Seeker
	offset int64
}

func (o *object) Read(p []byte) (int, error) {
	n, err := o.object.Read(p)
	o.offset += int64(n)

	if n == len(p) {
		return n, nil
	}

	return n, err
}

func (o *object) Close() error {
	defer func() {
		o.offset = 0
		o.object = nil
	}()

	if o.object == nil {
		return nil
	}

	return o.object.Close()
}

func (o *object) Seek(offset int64, whence int) (int64, error) {
	var newOffset int64

	switch whence {
	case io.SeekStart:
		newOffset = offset
	case io.SeekCurrent:
		newOffset = o.offset + offset
	case io.SeekEnd:
		newOffset = o.contentLength - offset
	default:
		return 0, fmt.Errorf("invalid whence value: %v", whence)
	}
	// validate seek position
	if newOffset < 0 || newOffset > o.contentLength {
		return 0, fmt.Errorf("invalid seek position: %d", newOffset)
	}
	// set offset
	o.offset = newOffset

	return newOffset, nil
}

type Reader struct {
	io.ReadSeekCloser

	ctx       context.Context
	bucket    *oss.Bucket
	objectKey string

	object *object
}

func NewReader(ctx context.Context, bucket *oss.Bucket, objectKey string) (*Reader, error) {
	header, err := bucket.GetObjectMeta(objectKey, oss.WithContext(ctx))
	if err != nil {
		return nil, fmt.Errorf("get object %s metadata: %w", objectKey, err)
	}

	contentLength, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, fmt.Errorf("parse content length: %w", err)
	}

	r := Reader{
		ctx:       ctx,
		bucket:    bucket,
		objectKey: objectKey,
		object: &object{
			contentLength: contentLength,
		},
	}

	if _, err := r.Seek(0, io.SeekStart); err != nil {
		return nil, err
	}

	return &r, nil
}

// Implements io.Reader interface.
func (r *Reader) Read(p []byte) (int, error) {
	return r.object.Read(p)
}

// Implements io.Closer interface.
func (r *Reader) Close() error {
	return r.object.Close()
}

// Implements io.Seeker interface.
func (r *Reader) Seek(offset int64, whence int) (int64, error) {
	oldOffset := r.object.offset

	newOffset, err := r.object.Seek(offset, whence)
	if err != nil {
		return 0, err
	}

	if newOffset == oldOffset && r.object.object != nil {
		return newOffset, nil
	}

	// refresh object
	o, err := r.bucket.GetObject(
		r.objectKey,
		oss.WithContext(r.ctx),
		oss.NormalizedRange(fmt.Sprintf("%d-", newOffset)),
	)
	if err != nil {
		return 0, fmt.Errorf("get object: %w", err)
	}

	r.object.object = o

	return newOffset, nil
}

func (r *Reader) Name() string {
	return fmt.Sprintf("oss://%s/%s", r.bucket.BucketName, r.objectKey)
}

func (r *Reader) ReadAt(p []byte, offset int64) (int, error) {
	o, err := r.bucket.GetObject(
		r.objectKey,
		oss.WithContext(r.ctx),
		oss.Range(offset, offset+int64(len(p))-1),
	)
	if err != nil {
		return 0, fmt.Errorf("get object: %w", err)
	}

	n, err := o.Read(p)
	if n == len(p) {
		return n, nil
	}

	return n, err
}
