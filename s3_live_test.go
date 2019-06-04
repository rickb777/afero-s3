package s3

import (
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

	t.Log("Testing against local file system")
	doTestFsOperations(g, afero.NewBasePathFs(afero.NewOsFs(), wd))

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

		fs := NewFs(bucket, s3.New(sess))
		// could populate this from /etc/mime.types
		fs.AddMimeTypes(map[string]string{
			"txt": "text/plain",
		})

		t.Logf("Testing against S3 bucket %s in %s", bucket, region)
		doTestFsOperations(g, fs)
	}
}

func doTestFsOperations(g *GomegaWithT, fs afero.Fs) {
	d := "/test-" + time.Now().Format("20060102150405")
	err := fs.MkdirAll(d+"/ab/cd", 0755)
	g.Expect(err).NotTo(HaveOccurred())

	err = fs.MkdirAll(d+"/ab/cd", 0755)
	g.Expect(err).NotTo(HaveOccurred())

	//----- first text file
	writeTextFile(g, fs, d+"/ab/cd/henryIVp1.txt", henryIVp1)

	//----- second text file
	writeTextFile(g, fs, d+"/ab/cd/henryIVp2.txt", henryIVp2)

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

	err = fs.RemoveAll(d)
	g.Expect(err).NotTo(HaveOccurred())

	_, err = fs.Open(d)
	g.Expect(os.IsNotExist(err)).To(BeTrue())
}

func writeTextFile(g *GomegaWithT, fs afero.Fs, name, content string) {
	f, err := fs.Create(name)
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
