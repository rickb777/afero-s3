package s3

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gstruct"
	"github.com/spf13/afero"
)

// test s3.Fs is compatible with Afero.Fs
var _ afero.Fs = (*Fs)(nil)
var _ afero.File = (*File)(nil)

// test s3.FileInfo is compatible with os.FileInfo
var _ os.FileInfo = (*FileInfo)(nil)

func TestName(t *testing.T) {
	g := NewGomegaWithT(t)

	fs := NewFs("mybucket", nil).WithContext(context.Background())
	g.Expect(fs.Name()).To(Equal("S3/mybucket"))
}

func TestReadAFile(t *testing.T) {
	g := NewGomegaWithT(t)

	bigJunk := make([]byte, 10*1024*1024)
	stub := &s3stub{buf: bytes.NewBuffer(bigJunk)}
	fs := NewFs("mybucket", stub).WithContext(context.Background())

	f, err := fs.Open("/a/b/c.png")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(stub.headKey).To(gstruct.PointTo(Equal("/a/b/c.png")))

	_, err = io.Copy(ioutil.Discard, f)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(stub.getKey).To(gstruct.PointTo(Equal("/a/b/c.png")))

	err = f.Close()
	g.Expect(err).NotTo(HaveOccurred())
}

func TestWriteABigFile(t *testing.T) {
	g := NewGomegaWithT(t)

	bigJunk := make([]byte, 10*1024*1024)
	buf := bytes.NewBuffer(bigJunk)
	stub := &s3stub{buf: buf}
	fs := NewFs("mybucket", stub).WithContext(context.Background())

	f, err := fs.Create("/a/b/c.png")
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(stub.headKey).To(gstruct.PointTo(Equal("/a/b/c.png")))

	_, err = io.Copy(f, buf)
	g.Expect(err).NotTo(HaveOccurred())

	err = f.Close()
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(stub.putKey).To(gstruct.PointTo(Equal("/a/b/c.png")))
}

//-------------------------------------------------------------------------------------------------

type s3stub struct {
	buf     *bytes.Buffer
	headKey *string
	getKey  *string
	putKey  *string
}

func (*s3stub) CopyObjectWithContext(ctx aws.Context, req *s3.CopyObjectInput, opts ...request.Option) (*s3.CopyObjectOutput, error) {
	panic("implement me")
}

func (*s3stub) DeleteObjectWithContext(ctx aws.Context, req *s3.DeleteObjectInput, opts ...request.Option) (*s3.DeleteObjectOutput, error) {
	panic("implement me")
}

func (s *s3stub) HeadObjectWithContext(ctx aws.Context, req *s3.HeadObjectInput, opts ...request.Option) (*s3.HeadObjectOutput, error) {
	s.headKey = req.Key
	return &s3.HeadObjectOutput{
		ContentLength: aws.Int64(123),
		LastModified:  aws.Time(time.Now()),
	}, nil
}

func (s *s3stub) GetObjectWithContext(ctx aws.Context, req *s3.GetObjectInput, opts ...request.Option) (*s3.GetObjectOutput, error) {
	s.getKey = req.Key
	return &s3.GetObjectOutput{
		Body:          ioutil.NopCloser(s.buf),
		ContentLength: aws.Int64(123),
		LastModified:  aws.Time(time.Now()),
	}, nil
}

func (*s3stub) ListObjectsV2WithContext(ctx aws.Context, req *s3.ListObjectsV2Input, opts ...request.Option) (*s3.ListObjectsV2Output, error) {
	panic("implement me")
}

func (s *s3stub) PutObjectWithContext(ctx aws.Context, req *s3.PutObjectInput, opts ...request.Option) (*s3.PutObjectOutput, error) {
	s.putKey = req.Key
	return &s3.PutObjectOutput{
		ETag:                 nil,
		Expiration:           nil,
		RequestCharged:       nil,
		SSECustomerAlgorithm: nil,
		SSECustomerKeyMD5:    nil,
		SSEKMSKeyId:          nil,
		ServerSideEncryption: nil,
		VersionId:            nil,
	}, nil
}
