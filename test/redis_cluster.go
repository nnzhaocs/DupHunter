package main

import (
    //"errors"
    "fmt"
    //"sync"
    "time"
    //"log"
    //redissingle "github.com/go-redis/redis"
    "github.com/gomodule/redigo/redis"
    //rediscluster "github.com/chasex/redis-go-cluster"
    rejson "github.com/secondspass/go-rejson"
    //"encoding/json"
    redisc "github.com/mna/redisc"
)

//var redisdb *redissingle.Client
//var cluster *rediscluster.Cluster

//func Newclusterclient() *redissingle.ClusterClient{
//	redisdb := redissingle.NewClusterClient(&redissingle.ClusterOptions{
//    		Addrs: []string{"172.19.0.2:6379", "172.19.0.3:6379", "172.19.0.4:6379", "172.19.0.5:6379", "172.19.0.6:6379", "172.19.0.7:6379"},
//	})
//	redisdb.Ping()
//	return redisdb
//}
//
//func Newclustercli() *rediscluster.Cluster{
//   cluster, _ := rediscluster.NewCluster(
//    	&rediscluster.Options{
//    	StartNodes: []string{"172.19.0.2:6379", "172.19.0.3:6379", "172.19.0.4:6379", "172.19.0.5:6379", "172.19.0.6:6379", "172.19.0.7:6379"},
//    	ConnTimeout: 50 * time.Millisecond,
//    	ReadTimeout: 50 * time.Millisecond,
//    	WriteTimeout: 50 * time.Millisecond,
//    	KeepAlive: 16,
//    	AliveTime: 60 * time.Second,
//    })
//
//    return cluster
//}

func createPool(addr string, opts ...redis.DialOption) (*redis.Pool, error) {
    return &redis.Pool{
        MaxIdle:     5,
        MaxActive:   10,
        IdleTimeout: time.Minute,
        Dial: func() (redis.Conn, error) {
            return redis.Dial("tcp", addr, opts...)
        },
        TestOnBorrow: func(c redis.Conn, t time.Time) error {
            _, err := c.Do("PING")
            return err
        },
    }, nil
}


func Newclusterredisc() redisc.Cluster{
	cluster := redisc.Cluster{
        	StartupNodes: []string{"192.168.0.213:7000", "192.168.0.213:7001", "192.168.0.213:7002", "192.168.0.213:7003", "192.168.0.213:7004", "192.168.0.213:7005"},
        	DialOptions:  []redis.DialOption{redis.DialConnectTimeout(5 * time.Second)},
        	CreatePool:   createPool,
    	}
	return cluster 
}

type jsonvalue struct {
    Page   int      `json:"page"`
    Fruits []string `json:"fruits"`
}

func main() {
	//data := &jsonvalue{Page: 1, Fruits: []string{"apple", "peach"}}

	redisdb := Newclusterredisc()
	defer redisdb.Close()

	if err := redisdb.Refresh(); err != nil {
        	panic(err)
    	}

	fmt.Println("hello")

	conn := redisdb.Get()
	defer conn.Close()

	//_, err := rejson.JSONSet(conn, "key1", ".", data, false, false)
    	//if err != nil {
        //	panic(err)
    	//}
	val, err := redis.Bytes(rejson.JSONGet(conn, "key1", ""))
	if err != nil{
		panic(err)
	}		
	//fmt.Println("key", string(json.Marshal(val)))
	fmt.Println("key", string(val))
	
}
