// Copyright 2016 Ka-Hing Cheung
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package internal

import (
	"os"

	. "github.com/voyvodov/goofys/api/common"
	. "gopkg.in/check.v1"

	"context"

	"github.com/aws/aws-sdk-go/service/s3"
)

func rustfsTestConfig(flags *FlagStorage) *S3Config {
	conf := (&S3Config{
		Region:    "us-east-1",
		AccessKey: os.Getenv("RUSTFS_ACCESS_KEY"),
		SecretKey: os.Getenv("RUSTFS_SECRET_KEY"),
	}).Init()
	if conf.AccessKey == "" {
		conf.AccessKey = "rustfsadmin"
	}
	if conf.SecretKey == "" {
		conf.SecretKey = "rustfsadmin"
	}
	flags.Endpoint = os.Getenv("TEST_ENDPOINT_URL")
	if flags.Endpoint == "" {
		flags.Endpoint = "http://127.0.0.1:9000"
	}
	return conf
}

type RustFSTest struct {
	fs     *Goofys
	flags  FlagStorage
	s3     *S3Backend
	bucket string
}

var _ = Suite(&RustFSTest{})

func (s *RustFSTest) SetUpSuite(t *C) {
	if !hasEnv("RUSTFS") || os.Getenv("CLOUD") != "s3" || hasEnv("AWS") || hasEnv("GCS") {
		t.Skip("RustFS is not selected")
	}
	s.fs = &Goofys{}

	conf := rustfsTestConfig(&s.flags)
	s.flags.Backend = conf

	var err error
	s.s3, err = NewS3("", &s.flags, conf)
	t.Assert(err, IsNil)

	_, err = s.s3.ListBuckets(nil)
	t.Assert(err, IsNil)
}

func (s *RustFSTest) SetUpTest(t *C) {
	bucket := RandStringBytesMaskImprSrc(32)
	s.bucket = bucket

	_, err := s.s3.CreateBucket(&s3.CreateBucketInput{
		Bucket: &bucket,
	})
	t.Assert(err, IsNil)

	s.fs = NewGoofys(context.Background(), bucket, &s.flags)
	t.Assert(s.fs, NotNil)
}

func (s *RustFSTest) TestListBuckets(t *C) {
	result, err := s.s3.ListBuckets(nil)
	t.Assert(err, IsNil)
	found := false
	for _, bucket := range result.Buckets {
		if *bucket.Name == s.bucket {
			found = true
		}
	}
	t.Assert(found, Equals, true)
}

func (s *RustFSTest) TearDownTest(t *C) {
	if s.fs != nil {
		s.fs.Destroy()
	}
	_, err := s.s3.DeleteBucket(&s3.DeleteBucketInput{Bucket: &s.bucket})
	t.Assert(err, IsNil)
}
