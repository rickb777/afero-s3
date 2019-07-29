package s3

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path"
	"strings"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/spf13/afero"
)

// Fs is an FS object backed by S3. It is safe to share Fs objects between
// goroutines. Note that WithContext and AddMimeTypes modify and return a new
// version of the Fs object.
type Fs struct {
	bucket    string
	s3API     S3APISubset
	mimeTypes map[string]string
	ctx       aws.Context
}

// NewFs creates a new Fs object writing files to a given S3 bucket.
func NewFs(bucket string, s3API S3APISubset) *Fs {
	return &Fs{
		bucket:    bucket,
		s3API:     s3API,
		mimeTypes: make(map[string]string),
		ctx:       context.Background(),
	}
}

// WithContext sets the context in a new instance of the file system.
func (fs Fs) WithContext(ctx aws.Context) *Fs {
	fs.ctx = ctx
	return &fs
}

// AddMimeTypes adds MIME types to new instance of the file system.
// When uploading (i.e. writing) files, these are used to set the
// content type based on the file extension.
//
// Any file uploaded without its MIME type defined here will assume the default,
// application/octet-stream.
func (fs Fs) AddMimeTypes(mimeTypes map[string]string) *Fs {
	for k, v := range mimeTypes {
		if strings.HasPrefix(k, ".") {
			k = k[1:]
		}
		fs.mimeTypes[k] = v
	}
	return &fs
}

// Name returns the type of FS object this is: S3/bucket.
func (fs Fs) Name() string { return "S3/" + fs.bucket }

// Create a file.
func (fs Fs) Create(name string) (afero.File, error) {
	file, err := fs.Open(name)
	if err != nil {
		if os.IsNotExist(err) {
			return fs.OpenFile(name, os.O_CREATE, 0777)
		}
		lgr("Create %s %q > %+v\n", fs.bucket, name, err)
		return file, err
	}

	// Create(), like all of S3, is eventually consistent.
	// To protect against unexpected behavior, have this method
	// wait until S3 reports the object exists.
	//if s3Client, ok := fs.s3API.(*s3.S3); ok {
	//	return file, s3Client.WaitUntilObjectExists(&s3.HeadObjectInput{
	//		Bucket: aws.String(fs.bucket),
	//		Key:    aws.String(name),
	//	})
	//}

	// TODO improved performance under failure conditions can be achieved by
	// using a trial PUT operation with status code 100-Continue before
	// actually processing large amounts of data
	// (see https://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html)
	lgr("Create %s %q\n", fs.bucket, name)
	return file, err
}

// Mkdir makes a directory in S3.
func (fs Fs) Mkdir(name string, perm os.FileMode) error {
	file, err := fs.OpenFile(fmt.Sprintf("%s/", path.Clean(name)), os.O_CREATE, perm)
	if err != nil {
		lgr("Mkdir %s %q, %v > %+v\n", fs.bucket, name, perm, err)
		return err
	}
	defer file.Close()

	lgr("Mkdir %s %q, %v\n", fs.bucket, name, perm)
	return nil
}

// MkdirAll creates a directory and all parent directories if necessary.
func (fs Fs) MkdirAll(path string, perm os.FileMode) error {
	return fs.Mkdir(path, perm)
}

// Open a file for reading.
func (fs Fs) Open(name string) (afero.File, error) {
	if _, err := fs.Stat(name); err != nil {
		lgr("Open %s %q > %+v\n", fs.bucket, name, err)
		return (*File)(nil), err
	}

	lgr("Open %s %q\n", fs.bucket, name)
	return NewFile(fs.bucket, name, fs.s3API, fs), nil
}

// OpenFile opens a file.
func (fs Fs) OpenFile(name string, flag int, perm os.FileMode) (afero.File, error) {
	file := NewFile(fs.bucket, name, fs.s3API, fs)

	if flag&os.O_APPEND != 0 {
		lgr("OpenFile %s %q append disallowed\n", fs.bucket, name)
		return file, errors.New("S3 is eventually consistent. Appending files will lead to trouble")
	}

	if flag&os.O_CREATE != 0 {
		// write some empty content, forcing the file to
		// be created upon Close.
		if _, err := file.WriteString(""); err != nil {
			lgr("OpenFile %s %q > %+v\n", fs.bucket, name, err)
			return file, err
		}
	}

	lgr("OpenFile %s %q\n", fs.bucket, name)
	return file, nil
}

// Remove a file.
func (fs Fs) Remove(name string) error {
	if _, err := fs.Stat(name); err != nil {
		return err
	}
	_, err := fs.s3API.DeleteObjectWithContext(fs.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})

	if err != nil {
		lgr("Remove %s %q > %+v\n", fs.bucket, name, err)
		return err
	}

	lgr("Remove %s %q\n", fs.bucket, name)
	return nil
}

// ForceRemove doesn't error if a file does not exist.
func (fs Fs) ForceRemove(name string) error {
	_, err := fs.s3API.DeleteObjectWithContext(fs.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(name),
	})

	if err != nil {
		lgr("ForceRemove %s %q > %+v\n", fs.bucket, name, err)
		return err
	}

	lgr("ForceRemove %s %q\n", fs.bucket, name)
	return nil
}

// RemoveAll removes a path.
func (fs Fs) RemoveAll(name string) error {
	s3dir := NewFile(fs.bucket, name, fs.s3API, fs)
	fis, err := s3dir.Readdir(0)
	if err != nil {
		lgr("RemoveAll %s Readdir %q > %+v\n", fs.bucket, name, err)
		return err
	}

	for _, fi := range fis {
		fullpath := path.Join(s3dir.Name(), fi.Name())
		if fi.IsDir() {
			if err := fs.RemoveAll(fullpath); err != nil {
				lgr("RemoveAll %s %q > %+v\n", fs.bucket, name, err)
				return err
			}
		} else {
			if err := fs.ForceRemove(fullpath); err != nil {
				lgr("RemoveAll %s %q > %+v\n", fs.bucket, name, err)
				return err
			}
		}
	}

	// finally remove the "file" representing the directory
	if err := fs.ForceRemove(s3dir.Name() + "/"); err != nil {
		lgr("RemoveAll %s %q > %+v\n", fs.bucket, name, err)
		return err
	}

	lgr("RemoveAll %s %q\n", fs.bucket, name)
	return nil
}

// Rename a file.
// There is no method to directly rename an S3 object, so the Rename
// will copy the file to an object with the new name and then delete
// the original.
func (fs Fs) Rename(oldname, newname string) error {
	if oldname == newname {
		lgr("Rename %s %q %q (no-op)\n", fs.bucket, oldname, newname)
		return nil
	}

	_, err := fs.s3API.CopyObjectWithContext(fs.ctx, &s3.CopyObjectInput{
		Bucket:               aws.String(fs.bucket),
		CopySource:           aws.String(fs.bucket + oldname),
		Key:                  aws.String(newname),
		ServerSideEncryption: aws.String("AES256"),
	})
	if err != nil {
		lgr("Rename %s copy %q %q > %+v\n", fs.bucket, oldname, newname, err)
		return err
	}

	_, err = fs.s3API.DeleteObjectWithContext(fs.ctx, &s3.DeleteObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(oldname),
	})

	if err != nil {
		lgr("Rename %s %q %q > %+v\n", fs.bucket, oldname, newname, err)
		return err
	}

	lgr("Rename %s %q %q\n", fs.bucket, oldname, newname)
	return nil
}

// Stat returns a FileInfo describing the named file.
// If there is an error, it will be of type *os.PathError.
func (fs Fs) Stat(name string) (os.FileInfo, error) {
	nameClean := path.Clean(name)
	out, err := fs.s3API.HeadObjectWithContext(fs.ctx, &s3.HeadObjectInput{
		Bucket: aws.String(fs.bucket),
		Key:    aws.String(nameClean),
	})

	if err != nil {
		if re, ok := err.(awserr.RequestFailure); ok && re.StatusCode() == 404 {
			statDir, e2 := fs.statDirectory(name)
			return statDir, e2
		}
		if ae, ok := err.(awserr.Error); ok && ae.Code() == s3.ErrCodeNoSuchKey {
			statDir, e2 := fs.statDirectory(name)
			return statDir, e2
		}
		lgr("Stat %s %q > %+v\n", fs.bucket, name, err)
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}

	if hasTrailingSlash(name) {
		// user asked for a directory, but this is a file
		lgr("Stat %s %q is a file\n", fs.bucket, name)
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  os.ErrNotExist,
		}
	}

	lgr("Stat %s %q\n", fs.bucket, name)
	return NewFileInfo(name, *out.ContentLength, *out.LastModified), nil
}

func (fs Fs) statDirectory(name string) (os.FileInfo, error) {
	nameClean := path.Clean(name)
	out, err := fs.s3API.ListObjectsV2WithContext(fs.ctx, &s3.ListObjectsV2Input{
		Bucket:  aws.String(fs.bucket),
		Prefix:  aws.String(trimLeadingSlash(nameClean)),
		MaxKeys: aws.Int64(1),
	})

	if err != nil {
		lgr("Stat %s %q > os.PathError %+v\n", fs.bucket, name, err)
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  err,
		}
	}

	if *out.KeyCount == 0 && name != "" {
		lgr("Stat %s %q > os.PathError os.ErrNotExist\n", fs.bucket, name)
		return FileInfo{}, &os.PathError{
			Op:   "stat",
			Path: name,
			Err:  os.ErrNotExist,
		}
	}

	lgr("Stat %s %q is directory\n", fs.bucket, name)
	return NewDirectoryInfo(name), nil
}

// ListObjects gets a list of all the files in the bucket with a given prefix. No
// more than 'max' results are returned, however 'max' is ignored if it is negative.
//
// This is an extension to the Afero Fs API.
func (fs Fs) ListObjects(prefix string, max int) (FileInfoList, error) {
	lister := Lister{
		bucket:    fs.bucket,
		name:      prefix,
		delimiter: aws.String(PathSeparator),
		s3Fs:      fs,
		s3API:     fs.s3API,
		ctx:       fs.ctx,
	}

	list, err := lister.ListObjects(max)
	if err != nil {
		return nil, err
	}

	return list, nil
}

func (fs Fs) Chmod(name string, mode os.FileMode) error {
	return syscall.EPERM
}

func (fs Fs) Chtimes(name string, atime time.Time, mtime time.Time) error {
	return syscall.EPERM
}

// SetLogger sets a debug logger for observing S3 accesses. This is
// compatible with 'log.Printf'. The default value is a no-op function.
func SetLogger(fn func(format string, v ...interface{})) {
	lgr = fn
}

var lgr = func(format string, v ...interface{}) {}
