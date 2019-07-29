package s3

import (
	"bytes"
	"crypto/md5"
	"encoding/base64"
	"io"
	"os"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// File represents a file in S3.
// It is not safe to share File objects between goroutines.
type File struct {
	bucket string
	name   string
	s3Fs   Fs
	s3API  S3APISubset

	// state
	offset     int64
	closed     bool
	readCloser io.ReadCloser
	writeBuf   *bytes.Buffer

	// readdir state
	readdirContinuationToken *string
	readdirNotTruncated      bool

	ctx aws.Context
}

// NewFile initializes an File object.
func NewFile(bucket, name string, s3API S3APISubset, s3Fs Fs) *File {
	return &File{
		bucket: bucket,
		name:   name,
		s3API:  s3API,
		s3Fs:   s3Fs,
		offset: 0,
		closed: false,
		ctx:    s3Fs.ctx,
	}
}

// WithContext sets the context in a new instance of the file.
func (f File) WithContext(ctx aws.Context) *File {
	f.ctx = ctx
	return &f
}

// Name returns the filename, i.e. S3 path without the bucket name.
func (f *File) Name() string { return f.name }

// Readdir reads the contents of the directory associated with file and
// returns a slice of up to n FileInfo values, as would be returned
// by ListObjects, in directory order. Subsequent calls on the same file will yield further FileInfos.
//
// If n > 0, Readdir returns at most n FileInfo structures. In this case, if
// Readdir returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdir returns all the FileInfo from the directory in
// a single slice. In this case, if Readdir succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdir returns the FileInfo read until that point
// and a non-nil error.
func (f *File) Readdir(n int) ([]os.FileInfo, error) {
	lister := f.lister(aws.String(PathSeparator))
	list, err := lister.ListObjects(n, true)
	if err != nil {
		return nil, err
	}

	return list.ToSlice(), nil
}

// ReaddirAll provides list of file info.
func (f *File) ReaddirAll() ([]os.FileInfo, error) {
	lister := f.lister(aws.String(PathSeparator))
	list, err := lister.ListObjects(-1, true)
	if err != nil {
		return nil, err
	}

	return list.ToSlice(), nil
}

// Readdirnames reads and returns a slice of names from the directory f.
//
// If n > 0, Readdirnames returns at most n names. In this case, if
// Readdirnames returns an empty slice, it will return a non-nil error
// explaining why. At the end of a directory, the error is io.EOF.
//
// If n <= 0, Readdirnames returns all the names from the directory in
// a single slice. In this case, if Readdirnames succeeds (reads all
// the way to the end of the directory), it returns the slice and a
// nil error. If it encounters an error before the end of the
// directory, Readdirnames returns the names read until that point and
// a non-nil error.
func (f *File) Readdirnames(n int) ([]string, error) {
	fi, err := f.Readdir(n)
	names := make([]string, len(fi))
	for i, f := range fi {
		_, names[i] = path.Split(f.Name())
	}
	return names, err
}

func (f *File) lister(delimiter *string) Lister {
	return Lister{
		bucket:    f.bucket,
		name:      f.name,
		delimiter: delimiter,
		s3Fs:      f.s3Fs,
		s3API:     f.s3API,
		ctx:       f.ctx,
	}
}

// Stat returns the FileInfo structure describing file.
// If there is an error, it will be of type *PathError.
func (f *File) Stat() (os.FileInfo, error) {
	return f.s3Fs.Stat(f.Name())
}

// Sync is a noop.
func (f *File) Sync() error {
	return nil
}

// Truncate changes the size of the file.
// It does not change the I/O offset.
// If there is an error, it will be of type *PathError.
func (f *File) Truncate(size int64) error {
	panic("implement Truncate")
}

// WriteString is like Write, but writes the contents of string s rather than
// a slice of bytes.
func (f *File) WriteString(s string) (int, error) {
	return f.Write([]byte(s))
}

// Close closes the File, rendering it unusable for I/O.
// It returns an error, if any.
func (f *File) Close() error {
	var err error

	if f.readCloser != nil {
		err = f.readCloser.Close()
		f.readCloser = nil
	}

	if f.writeBuf != nil {
		err = f.finaliseWrite()
		f.writeBuf = nil
	}

	f.closed = true
	f.offset = 0
	return err
}

// Read reads up to len(b) bytes from the File.
// It returns the number of bytes read and an error, if any.
// EOF is signaled by a zero count with err set to io.EOF.
func (f *File) Read(p []byte) (int, error) {
	if f.closed {
		// mimic os.File's read after close behavior
		panic("read after close")
	}
	if len(p) == 0 {
		return 0, nil
	}

	if f.readCloser == nil {
		output, err := f.s3API.GetObjectWithContext(f.ctx, &s3.GetObjectInput{
			Bucket: aws.String(f.bucket),
			Key:    aws.String(f.name),
		})
		if err != nil {
			return 0, err
		}

		f.readCloser = output.Body

		err = f.skipBytes(f.offset)
		if err != nil {
			return 0, err
		}
	}

	n, err := f.readCloser.Read(p)
	f.offset += int64(n)
	return n, err
}

func (f *File) skipBytes(toSkip int64) error {
	if f.readCloser == nil {
		return nil
	}

	if toSkip > 1024 {
		junk := make([]byte, 1024)
		for ; toSkip > 1024; toSkip -= 1024 {
			_, err := f.readCloser.Read(junk)
			if err != nil {
				return err
			}
		}
	}

	if toSkip > 0 {
		junk := make([]byte, toSkip)
		_, err := f.readCloser.Read(junk)
		if err != nil {
			return err
		}
	}

	return nil
}

// ReadAt reads len(p) bytes from the file starting at byte offset off.
// It returns the number of bytes read and the error, if any.
// ReadAt always returns a non-nil error when n < len(b).
// At end of file, that error is io.EOF.
func (f *File) ReadAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Read(p)
	return
}

// Seek sets the offset for the next Read or Write on file to offset, interpreted
// according to whence: 0 means relative to the origin of the file, 1 means
// relative to the current offset, and 2 means relative to the end.
// It returns the new offset and an error, if any.
// The behavior of Seek on a file opened with O_APPEND is not specified.
func (f *File) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		if f.readCloser != nil {
			// already reading so force the file to re-open on next read
			err := f.readCloser.Close()
			f.readCloser = nil
			if err != nil {
				return 0, err
			}
		}

		if f.writeBuf != nil {
			panic("not implemented")
		}

		f.offset = offset

	case 1:
		err := f.skipBytes(offset)
		if err != nil {
			return 0, err
		}
		f.offset += offset

	case 2:
		// can probably do this if we had GetObjectOutput (ContentLength)
		panic("TODO: whence == 2 seek")
	}
	return f.offset, nil
}

// Write writes len(b) bytes to the File.
// It returns the number of bytes written and an error, if any.
// Write returns a non-nil error when n != len(b).
func (f *File) Write(p []byte) (int, error) {
	if f.closed {
		// mimic os.File's write after close behavior
		panic("write after close")
	}
	//if f.offset != 0 {
	//	panic("TODO: non-offset == 0 write")
	//}

	if f.writeBuf == nil {
		f.writeBuf = &bytes.Buffer{}
	}

	return f.writeBuf.Write(p)
}

// finaliseWrite upload the write buffer contents to the S3 object. It is not possible
// to alter S3 objects (or even write them incrementally) so this is the only way they
// can be written.
func (f *File) finaliseWrite() error {
	if f.closed {
		// mimic os.File's write after close behavior
		panic("write after close")
	}
	if f.offset != 0 {
		panic("TODO: non-offset == 0 write")
	}

	buf := f.writeBuf.Bytes()
	hasher := md5.New()
	_, err := hasher.Write(buf)
	if err != nil {
		return err
	}
	hashBytes := hasher.Sum(nil)
	hashB64 := base64.StdEncoding.EncodeToString(hashBytes)
	//fmt.Printf("%x\n", hashBytes)
	//fmt.Println(hashB64)

	readSeeker := bytes.NewReader(buf)
	if _, err := f.s3API.PutObjectWithContext(f.ctx, &s3.PutObjectInput{
		Bucket:      aws.String(f.bucket),
		Key:         aws.String(f.name),
		Body:        readSeeker,
		ContentType: f.lookupContentType(),
		ContentMD5:  aws.String(hashB64),
		//ServerSideEncryption: aws.String("AES256"),
	}); err != nil {
		return err
	}

	return nil
}

func (f *File) lookupContentType() *string {
	ext := path.Ext(f.name)
	if len(ext) > 1 {
		if ext[0] == '.' {
			ext = ext[1:]
		}
		typ, defined := f.s3Fs.mimeTypes[ext]
		if defined {
			return aws.String(typ)
		}
	}
	return nil
}

// WriteAt writes len(p) bytes to the file starting at byte offset off.
// It returns the number of bytes written and an error, if any.
// WriteAt returns a non-nil error when n != len(p).
func (f *File) WriteAt(p []byte, off int64) (n int, err error) {
	_, err = f.Seek(off, 0)
	if err != nil {
		return
	}
	n, err = f.Write(p)
	return
}
