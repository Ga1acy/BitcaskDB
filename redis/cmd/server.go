package main

import (
	"bitcaskGo"
	"bitcaskGo/redis"
	"github.com/tidwall/redcon"
	"log"
	"sync"
)

const addr = "127.0.0.1:6380"

type BitcaskServer struct {
	dbs    map[int]*redis.RedisDataStructure
	server *redcon.Server
	mu     sync.RWMutex
}

func main() {
	//open redis data structure server
	redisDataStructure, err := redis.NewRedisDataStructure(bitcaskGo.DefaultOptions)
	if err != nil {
		panic(err)
	}

	//initial BitcaskServer
	bitcaskServer := &BitcaskServer{
		dbs: make(map[int]*redis.RedisDataStructure),
	}
	bitcaskServer.dbs[0] = redisDataStructure

	//initial a Redis server
	bitcaskServer.server = redcon.NewServer(addr, execClientCommand, bitcaskServer.accept, bitcaskServer.close)
	bitcaskServer.listen()

}

func (svr *BitcaskServer) listen() {
	log.Println("bitcask server running, ready to accept connection")
	_ = svr.server.ListenAndServe()
}

func (svr *BitcaskServer) accept(conn redcon.Conn) bool {
	cli := new(BitcaskClient)
	svr.mu.Lock()
	defer svr.mu.Unlock()
	cli.server = svr
	cli.db = svr.dbs[0]
	conn.SetContext(cli)
	return true
}

func (svr *BitcaskServer) close(conn redcon.Conn, err error) {
	for _, db := range svr.dbs {
		_ = db.Close()
	}
	_ = svr.server.Close()
}

//func main() {
//	conn, err := net.Dial("tcp", "localhost:6379")
//	if err != nil {
//		panic(err)
//	}
//
//	//send a request to redis
//	cmd := "set name2 zjq\r\n"
//	conn.Write([]byte(cmd))
//
//	//parse the response of redis
//	reader := bufio.NewReader(conn)
//	res, err := reader.ReadString('\n')
//	if err != nil {
//		panic(err)
//	}
//	fmt.Printf(res)
//}
