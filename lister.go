package s3

import (
	"math"
	"path"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
	"github.com/rickb777/collection"
)

// File represents a file in S3.
// It is not safe to share File objects between goroutines.
type Lister struct {
	bucket    string
	name      string
	delimiter *string
	s3Fs      Fs
	s3API     S3APISubset
	ctx       aws.Context
}

func (f *Lister) doListObjects(n int, filesOnly bool, continuationToken *string) (FileInfoList, *string, bool, error) {
	// ListObjects treats leading slashes as part of the directory name
	// It also needs a trailing slash to list contents of a directory.
	// If n > 1000, AWS returns only the first 1000 keys.
	prefix := trimLeadingSlash(f.name) + PathSeparator
	input := &s3.ListObjectsV2Input{
		ContinuationToken: continuationToken,
		Bucket:            aws.String(f.bucket),
		Prefix:            aws.String(prefix),
		Delimiter:         f.delimiter,
		MaxKeys:           aws.Int64(int64(n)),
	}
	output, err := f.s3API.ListObjectsV2WithContext(f.ctx, input)

	if err != nil {
		return nil, nil, false, err
	}

	fis := make(FileInfoList, 0)
	for _, subfolder := range output.CommonPrefixes {
		fis = append(fis, NewDirectoryInfo(PathSeparator+*subfolder.Prefix))
	}

	var dirs collection.StringSet
	if !filesOnly {
		dirs = collection.NewStringSet()
	}

	for _, fileObject := range output.Contents {
		p := PathSeparator + *fileObject.Key
		if hasTrailingSlash(*fileObject.Key) {
			// S3 includes <name>/ in the Contents listing for <name>
			if !filesOnly {
				dir := NewDirectoryInfo(p)
				fis = append(fis, dir)
				parent := trimTrailingSlash(dir.parent)
				for len(parent) > len(f.name) {
					dirs.Add(parent)
					parent = trimTrailingSlash(path.Dir(parent))
				}
			}
		} else {
			fis = append(fis, NewFileInfo(p, *fileObject.Size, *fileObject.LastModified))
		}
	}

	if dirs.NonEmpty() {
		for _, d := range dirs.ToList() {
			fis = append(fis, NewDirectoryInfo(d))
		}
	}

	return fis, output.NextContinuationToken, *output.IsTruncated, nil
}

// ListObjects lists all objects in the bucket starting with the lister's name.
func (f *Lister) ListObjects(max int, filesOnly bool) (FileInfoList, error) {
	if max <= 0 {
		max = math.MaxInt64
	}

	hasMore := true
	var continuationToken *string
	fileInfos := make(FileInfoList, 0)
	for hasMore {
		n := maxObjectsPerRequest
		if n > max {
			n = max
		}

		var infos FileInfoList
		var err error
		infos, continuationToken, hasMore, err = f.doListObjects(n, filesOnly, continuationToken)
		fileInfos = append(fileInfos, infos...)

		if err != nil {
			return nil, err
		}

		max -= len(infos)
	}
	return fileInfos, nil
}

// maxObjectsPerRequest is the upper limit of objects returned per request to ListObjectsV2WithContext
const maxObjectsPerRequest = 1000
