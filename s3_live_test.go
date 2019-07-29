package s3

import (
	"fmt"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
	. "github.com/onsi/gomega"
	"github.com/spf13/afero"
)

// Compare local file system (core Afero) against S3 operations.
// This depends on ~/.aws/credentials
// and also uses AWS_PROFILE and AWS_SHARED_CREDENTIALS_FILE.
func TestS3Operations(t *testing.T) {
	region := os.Getenv("S3_REGION")
	bucket := os.Getenv("S3_BUCKET")

	g := NewGomegaWithT(t)

	wd, err := os.Getwd()
	g.Expect(err).NotTo(HaveOccurred())

	dir := "/test-" + time.Now().Format("20060102150405")

	t.Log("Testing against local file system")
	local := afero.NewBasePathFs(afero.NewOsFs(), wd)
	doTestFsOperations(t, wd, dir, local)
	doTestLargeNumberOfFiles(t, wd, dir, local)
	doCleanup(t, wd, dir, local)

	if region != "" && bucket != "" {
		SetLogger(func(format string, v ...interface{}) {
			t.Logf(format, v...)
		})
		defer SetLogger(func(format string, v ...interface{}) {})

		sess, err := session.NewSession(&aws.Config{
			Region:      aws.String(region),
			Credentials: credentials.NewSharedCredentials("", ""),
		})
		g.Expect(err).NotTo(HaveOccurred())

		remote := NewFs(bucket, s3.New(sess))
		// could populate this from /etc/mime.types
		remote.AddMimeTypes(map[string]string{
			"txt": "text/plain",
		})

		t.Logf("Testing against S3 bucket %s in %s", bucket, region)
		doTestFsOperations(t, wd, dir, remote)
		doTestLargeNumberOfFiles(t, wd, dir, remote)
		doCleanup(t, wd, dir, remote)
	}
}

func doTestFsOperations(t *testing.T, wd, d string, fs afero.Fs) {
	g := NewGomegaWithT(t)

	//----- text file fails with no-such-directory
	//TODO fix this
	//writeTextFile(t, fs, d+"/ab/cd/henryIVp1.txt", henryIVp1, &os.PathError{
	//	Op:   "open",
	//	Path: wd + d + "/ab/cd/henryIVp1.txt",
	//	Err:  syscall.ENOENT,
	//})

	err := fs.MkdirAll(d+"/ab/cd", 0755)
	g.Expect(err).NotTo(HaveOccurred())

	err = fs.MkdirAll(d+"/ab/cd", 0755)
	g.Expect(err).NotTo(HaveOccurred())

	//----- first text file
	writeTextFile(t, fs, d+"/ab/cd/henryIVp1.txt", henryIVp1, nil)

	//----- second text file
	writeTextFile(t, fs, d+"/ab/cd/henryIVp2.txt", henryIVp2, nil)

	//----- list the enclosing directory
	f, err := fs.Open(d + "/ab/cd")
	g.Expect(err).NotTo(HaveOccurred())

	fis, err := f.Readdir(-1)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(fis).To(HaveLen(2))
	g.Expect(fis[0].Name()).To(Equal("henryIVp1.txt"))
	g.Expect(fis[1].Name()).To(Equal("henryIVp2.txt"))

	err = f.Close()
	g.Expect(err).NotTo(HaveOccurred())

	f, err = fs.Open(d + "/ab/cd/henryIVp1.txt")
	g.Expect(err).NotTo(HaveOccurred())

	b := make([]byte, 20)
	n, err := f.Read(b)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(n).To(Equal(20))
	g.Expect(string(b)).To(Equal(henryIVp1[:20]))

	err = f.Close()
	g.Expect(err).NotTo(HaveOccurred())

	err = fs.Remove(d + "/ab/cd/henryIVp1.txt")
	g.Expect(err).NotTo(HaveOccurred())

	f, err = fs.Open(d + "/ab/cd/henryIVp1.txt")
	g.Expect(os.IsNotExist(err)).To(BeTrue())

	err = fs.Remove(d + "/ab/cd/henryIVp2.txt")
	g.Expect(err).NotTo(HaveOccurred())

	_, err = fs.Open(d)
	g.Expect(err).NotTo(HaveOccurred())
}

func doTestLargeNumberOfFiles(t *testing.T, wd, d string, fs afero.Fs) {
	g := NewGomegaWithT(t)

	const (
		nh = 1
		ni = 1
		nj = 5
		nk = 11
		n  = nh * ni * nj * nk
	)

	t.Logf("creating large number of (small) files...\n")
	for h := 0; h < nh; h++ {
		for i := 0; i < ni; i++ {
			t.Logf("files %d, %d, ...\n", h, i)
			for j := 0; j < nj; j++ {
				for k := 0; k < nk; k++ {
					err := fs.MkdirAll(fmt.Sprintf("%s/%02d/%02d/%02d", d, h, i, j), 0755)
					g.Expect(err).NotTo(HaveOccurred())
					writeTextFile(t, fs, fmt.Sprintf("%s/%02d/%02d/%02d/%02d.txt", d, h, i, j, k), henryIVp2, nil)
				}
			}
		}
	}
	t.Logf("done creating files.\n")

	countFiles(t, d, fs, nh+1)
	for h := 0; h < nh; h++ {
		countFiles(t, fmt.Sprintf("%s/%02d", d, h), fs, ni)
		for i := 0; i < ni; i++ {
			countFiles(t, fmt.Sprintf("%s/%02d/%02d", d, h, i), fs, nj)
			for j := 0; j < nj; j++ {
				countFiles(t, fmt.Sprintf("%s/%02d/%02d/%02d", d, h, i, j), fs, nk)
			}
		}
	}
}

func doCleanup(t *testing.T, wd, d string, fs afero.Fs) {
	g := NewGomegaWithT(t)

	err := fs.RemoveAll(d)
	g.Expect(err).NotTo(HaveOccurred())

	_, err = fs.Open(d)
	g.Expect(os.IsNotExist(err)).To(BeTrue())
}

func countFiles(t *testing.T, d string, fs afero.Fs, expected int) {
	g := NewGomegaWithT(t)

	file, err := fs.Open(d)
	g.Expect(err).NotTo(HaveOccurred())
	defer file.Close()

	list, err := file.Readdir(-1)
	g.Expect(err).NotTo(HaveOccurred())
	g.Expect(list).To(HaveLen(expected))
}

func writeTextFile(t *testing.T, fs afero.Fs, name, content string, expected error) {
	t.Helper()
	g := NewGomegaWithT(t)

	f, err := fs.Create(name)
	if expected != nil {
		g.Expect(err).To(Equal(expected))
		return
	}

	g.Expect(err).NotTo(HaveOccurred())

	for _, s := range strings.Split(content, "\n") {
		_, err = f.WriteString(s + "\n")
		g.Expect(err).NotTo(HaveOccurred())
	}

	err = f.Close()
	g.Expect(err).NotTo(HaveOccurred())
}

var henryIVp1 = `So shaken as we are,
So wan with care!
Find we a time for frighted peace to pant
And breath short-winded accents of new broils
To be commenced in strands afar remote.`

var henryIVp2 = `Open your ears; for which of you will stop
The vent of hearing when loud Rumour speaks?`
