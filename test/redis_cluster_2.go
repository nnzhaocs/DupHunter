package main

import (
	//"errors"
	"fmt"
	//"flag"
	"encoding/json"
	// "sync"
	//"time"

	"github.com/go-redis/redis"
)

var redisdb *redis.Client

type jsonvalue struct {
	Page   int      `json:"page"`
	Fruits []string `json:"fruits"`
}

type fileDescriptor struct {
	Digest   string
	ServerIp string
	Slices   map[string][]sliceDescriptor
	FilePath string
}

type sliceDescriptor struct {
	Digest string
	Tp     string
}

func (m *sliceDescriptor) MarshalBinary() ([]byte, error) {
	str, err := json.Marshal(m)
	fmt.Println(str, err)
	return str, err
}

func (m *sliceDescriptor) UnmarshalBinary(data []byte) error {
	return json.Unmarshal(data, m)
}

func (m *fileDescriptor) MarshalBinary() ([]byte, error) {
	//str, err := json.Marshal(m.slicemap)
	//fmt.Println(str, err)
	return json.Marshal(m)
}

func (m *fileDescriptor) UnmarshalBinary(data []byte) error {
	// convert data to yours, let's assume its json data
	err := json.Unmarshal(data, m)
	return err
}

func (m *jsonvalue) MarshalBinary() ([]byte, error) {
	return json.Marshal(m)
}

func (m *jsonvalue) UnmarshalBinary(data []byte) error {
	// convert data to yours, let's assume its json data
	return json.Unmarshal(data, m)
}

/*
func newPool() *redisgo.Pool {
	return &redisgo.Pool{
		// Maximum number of idle connections in the pool.
		MaxIdle: 80,
		// max number of connections
		MaxActive: 12000,
		// Dial is an application supplied function for creating and
		// configuring a connection.
		Dial: func() (redisgo.Conn, error) {
			c, err := redisgo.Dial("tcp", ":6379")
			if err != nil {
				panic(err.Error())

			}
			return c, err

		},
	}

}
*/
func main() {
	/*redisdb := redis.NewClient(&redis.Options{
		Addr: "192.168.0.174:6379",
		//Password: "", // no password set
		//        DB:       0,  // use default DB

	})*/

	/*pong, err := redisdb.Ping().Result()
	fmt.Println(pong, err)
	ok, err := redisdb.FlushAll().Result()
	fmt.Println(ok, err)*/
	redisdb := redis.NewClusterClient(&redis.ClusterOptions{
		Addrs: []string{
			"192.168.0.170:7000", "192.168.0.170:7001",
			"192.168.0.171:7000", "192.168.0.171:7001",
			"192.168.0.172:7000", "192.168.0.172:7001",
			"192.168.0.174:7000", "192.168.0.174:7001",
			"192.168.0.176:7000", "192.168.0.176:7001",
			"192.168.0.177:7000", "192.168.0.177:7001",
			"192.168.0.178:7000", "192.168.0.178:7001",
			"192.168.0.179:7000", "192.168.0.179:7001",
			"192.168.0.180:7000", "192.168.0.180:7001",
		}})
	//redisdb.Ping()

	err := redisdb.Set("key", "value", 0).Err()
	if err != nil {
		panic(err)
	}

	val, err := redisdb.Get("key").Result()
	if err != nil {
		panic(err)
	}
	fmt.Println("key", val)

	data := &jsonvalue{Page: 1, Fruits: []string{"apple1", "peach1"}}
	err = redisdb.Set("key2", data, 0).Err()
	if err != nil {
		panic(err)
	}
	value, err := redisdb.Get("key2").Result()
	var newdata jsonvalue
	if err := newdata.UnmarshalBinary([]byte(value)); err != nil {
		panic(err)
	}
	fmt.Printf("value: %v\n", newdata)

	/*pool := newPool()
	conn := pool.Get()
	//err := ping(conn)
	_, err = conn.Do("PING")
	if err != nil {
		fmt.Println(err)
	}
	*/
	slicemap := make(map[string][]sliceDescriptor)
	var slices []sliceDescriptor
	sd := sliceDescriptor{
		Digest: "sha256:122222",
		Tp:     "slice",
	}

	slices = append(slices, sd)
	fmt.Println("slices", slices)
	/*rh := rejson.NewReJSONHandler()
	rh.SetRedigoClient(conn)*/
	slicemap["192.168.0.1"] = slices
	fmt.Println("slicemap", slicemap)
	/*
		_, err = rejson.JSONSet(conn, "key2", ".", slicemap, false, false)
		if err != nil {
			panic(err)
			return

		}
		res, err := redisgo.Bytes(rejson.JSONGet(conn, "key2", "."))
		if err != nil {
			panic(err)
			return

		}
		val2 := make(map[string][]sliceDescriptor)
		err = json.Unmarshal(res, &val2)
		fmt.Println("key2", val2)
	*/
	fd := &fileDescriptor{
		Digest:   "sha256:123333333",
		ServerIp: "192.168.0.123",
		Slices:   slicemap,
		FilePath: "/home/nannan/distribution",
	}
	err = redisdb.Set("keyfd", fd, 0).Err()
	//_, err = rejson.JSONSet(conn, "key3", ".", fd, false, false)
	if err != nil {
		panic(err)
		return
	}
	fmt.Println("keyfd", fd)
	valfd, err := redisdb.Get("keyfd").Result()
	//res3, err := redisgo.Bytes(rejson.JSONGet(conn, "key3", "."))
	if err != nil {
		panic(err)
		return
	}
	fmt.Println(string(valfd))
	/*
		val2 := make(map[string][]sliceDescriptor)
		err = json.Unmarshal(res3, &val2)
		fmt.Println("key2", val2)
	*/
	val3 := fileDescriptor{}
	err = val3.UnmarshalBinary([]byte(valfd))
	fmt.Println("keys3", val3)

	//var m []sliceDescriptor
	//json.Unmarshal(slices)
	/*
		m, _ := json.Marshal(slices)
		fmt.Println(string(m))
		err = redisdb.Set("key2", m, 0).Err()
		if err != nil {
			panic(err)

		}
		value, err = redisdb.Get("key2").Result()
		var newdata2 []sliceDescriptor
		if err := json.Unmarshal([]byte(value), &newdata2); err != nil {
			panic(err)

		}
		fmt.Printf("original value: %v\n", string(m))
		fmt.Printf("value: %v\n", newdata2)
	*/
	/*if _, err := redisdb.HMSet("sha:11111", map[string]interface{}{
		"digest":             "sha256:123333333",
		"serverIp":           "192.168.0.213", //NANNAN SET TO the first registry ip address for global dedup; even for local dedup, it is correct?!
		"requestedServerIps": rqslices,        // NANNAN: set to none initially.
		"filePath":           "/home/nannan/distribution"}).Result(); err != nil {
		panic(err)
	}
	reply, err := redisdb.Do("HMGET", "sha:11111", "digest", "filePath", "serverIp").Result()
	fmt.Printf("%v\n", reply)
	//var desc fileDescriptor
	//err = redisdb.HVals("sha:11111").ScanSlice(&desc)
	//fmt.Printf("%d", desc)
	//json.Unmarshal(redisdb.HVals("sha:11111"), &desc)
	//fmt.Printf("%d", desc)
	/*
	   var desc fileDescriptor
	       if _, err = redisgo.Scan(reply, &desc.digest, &desc.filePath, &desc.serverIp); err != nil {

	           panic( err)
	       }
	   	fmt.Printf("des %v\n", desc)*/
	//	fmt.Printf("value: %v\n", newdata)

}
