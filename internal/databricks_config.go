package internal

import (
	"fmt"
	"io/ioutil"

	hocon "github.com/go-akka/configuration"
)

type DatabricksConf struct {
	DaemonHost          string
	SessionTokenAllowed bool
	AutoDetectEndpoint  bool
	Endpoint            string
}

func NewDatabricksConf() (conf *DatabricksConf, err error) {
	conf = &DatabricksConf{
		SessionTokenAllowed: true,
		AutoDetectEndpoint:  true,
	}
	return
}

func (conf *DatabricksConf) Load() (err error) {
	// data-client.conf is generated from
	// https://github.com/databricks/universe/blob/master/daemon/node/src/main/scala/com/databricks/backend/daemon/node/container/LxcContainerManager.scala#L630
	err = conf.parse("/databricks/data/conf/data-client.conf", "/databricks/common/conf/deploy.conf")
	return
}

func (conf *DatabricksConf) parse(clientConf string, deployConf string) (err error) {
	var data *hocon.Config

	if clientConf != "" {
		data, err = loadHocon(clientConf)
		if err != nil {
			return
		}

		conf.DaemonHost = data.GetString("databricks.daemon.data.serverHost")
		if conf.DaemonHost == "" {
			err = fmt.Errorf("\"databricks.daemon.data.serverHost\" not found in %v", clientConf)
			return
		}
	}

	if deployConf != "" {
		data, err = loadHocon(deployConf)
		if err != nil {
			return
		}

		// https://github.com/go-akka/configuration/issues/8
		// go-akka/configuration's path splitting is wrong wrt quotes, manually get the quoted part
		driver := data.GetNode("driver.spark.hadoop")
		if driver == nil {
			return
		}

		node := driver.GetChildObject("databricks.dbfs.client.aws.sessionTokenAllowed")
		if node != nil {
			conf.SessionTokenAllowed = node.GetBoolean()
		}

		node = driver.GetChildObject("databricks.s3.auto-detect-endpoint")
		if node != nil {
			conf.AutoDetectEndpoint = node.GetBoolean()
		}

		if !conf.AutoDetectEndpoint {
			node = driver.GetChildObject("databricks.s3.endpoint")
			if node != nil {
				conf.Endpoint = node.GetString()
			}
		}
	}

	return
}

func loadHocon(file string) (conf *hocon.Config, err error) {
	data, err := ioutil.ReadFile(file)
	if err != nil {
		return
	}

	conf = hocon.ParseString(string(data))
	if conf == nil {
		// https://github.com/go-akka/configuration/issues/9
		// this is a lie, go-akka/configuration currently panics if it cannot parse the file
		err = fmt.Errorf("cannot parse config: %v", file)
		return
	}

	return
}
