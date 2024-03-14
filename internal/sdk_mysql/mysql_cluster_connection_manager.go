package sdk_mysql

import (
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

// SqlClusterConfig,ValidateClusterConfig
// SqlConnectionManager contains different cluster information and connections to them.
type SqlConnectionManager struct {
	Clusters map[string]*sql.DB
	lock     sync.Mutex
}

// ConfigSqlConnectionManager returns an instance of SqlConnectionManager.
func ConfigSqlConnectionManager() *SqlConnectionManager {

	return &SqlConnectionManager{
		Clusters: make(map[string]*sql.DB),
		lock:     sync.Mutex{},
	}
}

// setSqlClusterObject maps the SqlClusterObject using connection string to *SqlConnectionManager
func (cm *SqlConnectionManager) setSqlClusterObject(clusterIdentifier string, c *sql.DB) {
	cm.Clusters[clusterIdentifier] = c
}

// getSqlClusterObject returns *Sql.DB if cluster is already setup.
// If not, then set up a *Sql.DB
func (cm *SqlConnectionManager) getSqlClusterObject(connStr, username, password string,
	clusterConfig *SqlClusterConfig) (*sql.DB, error) {

	clusterIdentifier := connStr + "/" + clusterConfig.Database

	_, ok := cm.Clusters[clusterIdentifier]
	if !ok {
		if err := ValidateClusterConfig(connStr, username, password, clusterConfig); err != nil {
			return nil, err
		}
		if clusterConfig.Port == 0 {
			clusterConfig.Port = 3306
		}
		dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s", username, password, connStr, clusterConfig.Port, clusterConfig.Database)
		cluster, err := sql.Open("mysql", dsn)
		if err != nil {
			fmt.Println("Unable to connect to SqlDB!")
			log.Fatal(err)
			return nil, err
		}
		if clusterConfig.MaxIdleTime == 0 {
			clusterConfig.MaxIdleTime = 5
		}
		if clusterConfig.MaxIdleConnections == 0 {
			clusterConfig.MaxIdleTime = 2
		}
		if clusterConfig.MaxOpenConnections == 0 {
			clusterConfig.MaxIdleTime = 250
		}
		cluster.SetConnMaxIdleTime(time.Duration(clusterConfig.MaxIdleTime) * time.Second)
		cluster.SetConnMaxLifetime(time.Duration(clusterConfig.MaxLifeTime) * time.Second)
		cluster.SetMaxIdleConns(clusterConfig.MaxIdleConnections)
		cluster.SetMaxOpenConns(clusterConfig.MaxOpenConnections)

		err = cluster.Ping()
		if err != nil {
			fmt.Println("Unable to connect to SqlDB!")
			log.Fatal(err)
			return nil, err
		}
		cm.setSqlClusterObject(clusterIdentifier, cluster)
	}

	return cm.Clusters[clusterIdentifier], nil
}

// Disconnect disconnects a particular SqlDB Cluster
func (cm *SqlConnectionManager) Disconnect(connStr string, clusterConfig *SqlClusterConfig) error {
	clusterIdentifier := connStr + "/" + clusterConfig.Database
	cluster, ok := cm.Clusters[clusterIdentifier]
	if ok {
		if err := cluster.Close(); err != nil {
			fmt.Println("Disconnect failed!")
			log.Fatal(err)
			return err
		}
	}
	return nil
}

// DisconnectAll disconnect all the SqlDB Clusters used in a tasks.Request
func (cm *SqlConnectionManager) DisconnectAll() {
	defer cm.lock.Unlock()
	cm.lock.Lock()
	for cS, v := range cm.Clusters {
		if v != nil {
			_ = v.Close()
			delete(cm.Clusters, cS)
		}
		v = nil
	}
}
