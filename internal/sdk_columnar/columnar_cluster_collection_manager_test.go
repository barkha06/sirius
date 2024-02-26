package sdk_columnar

import (
	"log"
	"testing"
)

func TestConfigConnectionManager(t *testing.T) {
	cConfig := &ClusterConfig{}

	cmObj := ConfigConnectionManager()

	//var accessKey, secretKey string
	cbUsername := ""
	cbPassword := ""
	cbConnStr := ""

	if _, err := cmObj.GetCluster(cbConnStr, cbUsername, cbPassword, cConfig); err != nil {
		log.Println(err)
		t.Fail()
	}
	log.Println("Connection Manager Object:\n", cmObj)
}
