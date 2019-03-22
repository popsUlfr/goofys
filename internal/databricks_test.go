// Copyright 2019 - 2019 Databricks

package internal

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	. "gopkg.in/check.v1"
)

type DatabricksConfTest struct{}

var _ = Suite(&DatabricksConfTest{})

type DatabricksTest struct {
	server  *httptest.Server
	session *DatabricksSession
}

var _ = Suite(&DatabricksTest{})

func (s *DatabricksTest) SetUpSuite(t *C) {
	s.server = httptest.NewServer(&DatabricksTestServer{t: t})
	t.Assert(s.server, NotNil)
}

func (s *DatabricksTest) TearDownSuite(t *C) {
	if s.server != nil {
		s.server.Close()
	}
}

func (s *DatabricksTest) SetUpTest(t *C) {
	client := s.server.Client()
	t.Assert(client, NotNil)

	uri, err := url.Parse(s.server.URL)
	t.Assert(err, IsNil)

	port, err := strconv.Atoi(uri.Port())
	t.Assert(err, IsNil)

	session, err := NewDatabricksSession(client, uri.Hostname(), uint16(port))
	t.Assert(err, IsNil)
	t.Assert(session, NotNil)
	t.Assert(session.headers.Get("sessionid"), Equals, "1234567890")

	s.session = session
}

func (s *DatabricksTest) TestDatabricksFindMounts(t *C) {
	m, err := s.session.FindMount("/")
	t.Assert(err, IsNil)
	t.Assert(m, NotNil)

	bucket, prefix, err := m.Bucket()
	t.Assert(err, IsNil)
	t.Assert(bucket, Equals, "databricks-staging-storage-oregon")
	t.Assert(prefix, Equals, "shard-dogfood/0")
}

func (s *DatabricksTest) TestDatabricksGetMountsV2(t *C) {
	res, err := s.session.GetMountsV2()
	t.Assert(err, IsNil)
	t.Assert(len(res), Equals, 2)
	t.Assert(res[0].MountPointString, Equals, "/")
	t.Assert(res[0].SourceString, Equals, "s3a://databricks-staging-storage-oregon/shard-dogfood/0")
	t.Assert(res[0].Configurations.Sse, Equals, "AES256")
	t.Assert(res[0].Configurations.SessionTokenId, Equals, "/")
	t.Assert(res[0].Configurations.CredentialsType, Equals, "SessionToken")

	bucket, prefix, err := res[0].Bucket()
	t.Assert(err, IsNil)
	t.Assert(bucket, Equals, "databricks-staging-storage-oregon")
	t.Assert(prefix, Equals, "shard-dogfood/0")
}

func (s *DatabricksTest) TestDatabricksGetSessionCredentials(t *C) {
	p := NewDatabricksCredentialsProvider(s.session, "/")
	t.Assert(p.IsExpired(), Equals, true)

	cred, err := p.Retrieve()
	t.Assert(err, IsNil)
	t.Assert(cred.AccessKeyID, Equals, "KEY")
	t.Assert(cred.SecretAccessKey, Equals, "SECRET")
	t.Assert(cred.SessionToken, Equals, "TOKEN")
	t.Assert(p.IsExpired(), Equals, false)
}

func (s *DatabricksTest) TestConfigureDatabricks(t *C) {
	flags := FlagStorage{}
	awsConfig := aws.Config{}
	conf := DatabricksConf{AutoDetectEndpoint: true}

	bucketSpec, err := s.session.configureDatabricksMount(&conf, "/", "ml", &flags, &awsConfig)
	t.Assert(err, IsNil)
	t.Assert(bucketSpec, Equals, "databricks-staging-storage-oregon:shard-dogfood/0/ml")
	t.Assert(flags.UseSSE, Equals, true)
	t.Assert(flags.Endpoint, Equals, "")
	t.Assert(awsConfig.Credentials, NotNil)

	flags = FlagStorage{}
	awsConfig = aws.Config{}
	conf = DatabricksConf{
		AutoDetectEndpoint: false,
		Endpoint:           "s3.us-iso-east-1.c2s.ic.gov",
	}

	bucketSpec, err = s.session.configureDatabricksMount(&conf, "/mnt/c2s", "ml", &flags, &awsConfig)
	t.Assert(err, IsNil)
	t.Assert(bucketSpec, Equals, "databricks-c2s:shard-dogfood/0/ml")
	t.Assert(flags.UseSSE, Equals, true)
	t.Assert(flags.Endpoint, Equals, "https://s3.us-iso-east-1.c2s.ic.gov")
	t.Assert(awsConfig.Credentials, IsNil)
}

func (s *DatabricksTest) TestParseDatabricksDeployConf(t *C) {
	conf, err := NewDatabricksConf()
	t.Assert(err, IsNil)

	err = conf.parse("", "../test/databricks_deploy.conf")
	t.Assert(err, IsNil)
	t.Assert(conf.SessionTokenAllowed, Equals, false)
	t.Assert(conf.AutoDetectEndpoint, Equals, false)
	t.Assert(conf.Endpoint, Equals, "s3.us-iso-east-1.c2s.ic.gov")
}

type DatabricksTestServer struct {
	t *C
}

func (s *DatabricksTestServer) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	t := s.t

	q := r.URL.Query()
	api := q.Get("type")
	apiPrefix := "\"com.databricks.backend.daemon.data.common.DataMessages$"

	t.Assert(strings.HasPrefix(api, apiPrefix), Equals, true)
	t.Assert(strings.HasSuffix(api, "\""), Equals, true)

	// services throws NPE if auth is not passed
	t.Assert(r.Header.Get("auth"), Not(Equals), "")
	t.Assert(r.Header.Get("authType"), Equals, "com.databricks.backend.daemon.data.common.DbfsAuth")

	body, err := ioutil.ReadAll(r.Body)
	t.Assert(err, IsNil)
	// body is required
	t.Assert(len(body), Not(Equals), 0)

	// randomly fail to test retry
	rand := rand.New(rand.NewSource(int64(time.Now().Nanosecond())))
	if rand.Intn(4) == 0 {
		w.WriteHeader(500)
	}

	api = api[len(apiPrefix) : len(api)-1]
	switch api {
	case "SessionCreate":
		_, err := w.Write([]byte("{\"id\":1234567890}"))
		t.Assert(err, IsNil)
	case "GetMountsV2":
		t.Assert(r.Header.Get("sessionid"), Equals, "1234567890")

		_, err = w.Write([]byte("["))
		t.Assert(err, IsNil)

		sourceString := "s3a://databricks-staging-storage-oregon/shard-dogfood/0"
		mountPointString := "/"
		class := "com.databricks.backend.daemon.data.server.util.HadoopMountEntryV2"
		configurations := map[string]string{
			"fs.s3a.server-side-encryption-algorithm": "AES256",
			"fs.s3a.sessionToken.id":                  "/",
			"fs.s3a.credentialsType":                  "SessionToken",
		}
		configBlob, err := json.Marshal(configurations)
		t.Assert(err, IsNil)

		_, err = w.Write([]byte(fmt.Sprintf("{%q:%q,%q:%q,%q:%q,%q:%v}",
			"mountPointString", mountPointString,
			"sourceString", sourceString,
			"@class", class,
			"configurations", string(configBlob))))
		t.Assert(err, IsNil)

		_, err = w.Write([]byte(","))
		t.Assert(err, IsNil)

		sourceString = "s3a://databricks-c2s/shard-dogfood/0"
		mountPointString = "/mnt/c2s"
		class = "com.databricks.backend.daemon.data.server.util.HadoopMountEntryV2"
		configurations = map[string]string{
			"fs.s3a.server-side-encryption-algorithm": "AES256",
			"fs.s3a.sessionToken.id":                  "/",
		}
		configBlob, err = json.Marshal(configurations)
		t.Assert(err, IsNil)

		_, err = w.Write([]byte(fmt.Sprintf("{%q:%q,%q:%q,%q:%q,%q:%v}",
			"mountPointString", mountPointString,
			"sourceString", sourceString,
			"@class", class,
			"configurations", string(configBlob))))
		t.Assert(err, IsNil)

		_, err = w.Write([]byte("]"))
		t.Assert(err, IsNil)

	case "GetSessionCredentials":
		t.Assert(r.Header.Get("sessionid"), Equals, "1234567890")

		params := make(map[string]string)
		err = json.Unmarshal(body, &params)
		t.Assert(err, IsNil)

		t.Assert(params["mountPoint"], Equals, "/")

		_, err := w.Write([]byte(fmt.Sprintf("{\"key\":\"KEY\",\"secret\":\"SECRET\",\"token\":\"TOKEN\",\"expiration\":%v}", (time.Now().Unix()+500)*1000)))
		t.Assert(err, IsNil)

	default:
		s.t.Fatal("unknown API")
	}
}
