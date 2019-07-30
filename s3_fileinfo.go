package s3

import (
	"os"
	"path"
	"time"
)

//go:generate runtemplate -v -tpl simple/list.tpl Type=FileInfo MapTo:string Comparable:true

// PathSeparator is always a forward slash. This is consistent and not OS-specific.
const PathSeparator = "/"

// FileInfo implements os.FileInfo for a file in S3.
type FileInfo struct {
	parent      string
	name        string
	directory   bool
	sizeInBytes int64
	modTime     time.Time
	depth       int
}

// NewFileInfo creates file info.
func NewFileInfo(name string, sizeInBytes int64, modTime time.Time) FileInfo {
	parent, file := path.Split(trimTrailingSlash(name))
	return FileInfo{
		parent:      parent,
		name:        file,
		directory:   false,
		sizeInBytes: sizeInBytes,
		modTime:     modTime,
		depth:       depth(parent),
	}
}

// NewFileInfo creates directory info.
func NewDirectoryInfo(name string) FileInfo {
	parent, file := path.Split(trimTrailingSlash(name))
	return FileInfo{
		parent:    parent,
		name:      file,
		directory: true,
		depth:     depth(parent),
	}
}

// Name provides the base name of the file. This does not have a leading '/'.
func (fi FileInfo) Name() string {
	return fi.name
}

// Parent provides the name of the containing directory. This normally ends with
// '/' or is blank.
func (fi FileInfo) Parent() string {
	return fi.parent
}

// Path provides the full path of the file within the S3 bucket.
func (fi FileInfo) Path() string {
	return fi.parent + fi.name
}

// Size provides the length in bytes for a file.
func (fi FileInfo) Size() int64 {
	return fi.sizeInBytes
}

// Mode provides the file mode bits. For a file in S3 this defaults to
// 664 for files, 775 for directories.
// In the future this may return differently depending on the permissions
// available on the bucket.
func (fi FileInfo) Mode() os.FileMode {
	if fi.directory {
		return 0755
	}
	return 0664
}

// ModTime provides the last modification time.
func (fi FileInfo) ModTime() time.Time {
	return fi.modTime
}

// IsDir provides the abbreviation for Mode().IsDir()
func (fi FileInfo) IsDir() bool {
	return fi.directory
}

// Sys provides the underlying data source (can return nil)
func (fi FileInfo) Sys() interface{} {
	return nil
}
