package internal

const DATABRICKS_DATA_DAEMON_CONTROL_PORT = 7070

// https://github.com/databricks/universe/blob/8cd62117cb401cbe61def79cbd8231b5a1daef57/daemon/data/data-common/src/main/scala/com/databricks/backend/daemon/data/common/SessionId.scala#L5
type SessionCreateResponse struct {
	Id uint64
}

// https://github.com/databricks/universe/blob/8cd62117cb401cbe61def79cbd8231b5a1daef57/daemon/data/data-common/src/main/scala/com/databricks/backend/daemon/data/common/MountEntry.scala#L101
type GetMountsV2Response struct {
	MountPointString string
	SourceString     string
	Configurations   struct {
		Sse              string `json:"fs.s3a.server-side-encryption-algorithm"`
		SessionTokenId   string `json:"fs.s3a.sessionToken.id"`
		CredentialsType  string `json:"fs.s3a.credentialsType"`
		Endpoint         string `json:"fs.s3a.endpoint"`
		FallbackEndpoint string `json:"fs.s3a.fallback-endpoint"`
	}
}

// https://github.com/databricks/universe/blob/8cd62117cb401cbe61def79cbd8231b5a1daef57/daemon/data/data-common/src/main/scala/com/databricks/backend/daemon/data/common/DataMessages.scala#L355
type GetSessionCredentialsResponse struct {
	Secret     string
	Token      string
	Key        string
	Expiration int64 // milliseconds
}
