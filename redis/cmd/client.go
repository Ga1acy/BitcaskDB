package main

import (
	"bitcaskGo"
	"bitcaskGo/redis"
	"bitcaskGo/utils"
	"fmt"
	"github.com/tidwall/redcon"
	"strings"
)

func newWrongNumberOfArgsError(cmd string) error {
	return fmt.Errorf("ERR wrong number of arguments for '%s' command", cmd)
}

type cmdHandler func(cli *BitcaskClient, args [][]byte) (interface{}, error)

var supportedCommands = map[string]cmdHandler{
	"set":       set,
	"get":       get,
	"hset":      hset,
	"hget":      hget,
	"hdel":      hdel,
	"sadd":      sadd,
	"sismember": sismember,
	"srem":      srem,
	"lpush":     lpush,
	"rpush":     rpush,
	"lpop":      lpop,
	"rpop":      rpop,
	"zadd":      zadd,
	"zscore":    zscore,
}

type BitcaskClient struct {
	server *BitcaskServer
	db     *redis.RedisDataStructure
}

func execClientCommand(conn redcon.Conn, cmd redcon.Command) {
	command := strings.ToLower(string(cmd.Args[0]))
	cmdFunc, ok := supportedCommands[command]
	if !ok {
		conn.WriteError("Err unsupported command'" + command + "'")
		return
	}

	client, _ := conn.Context().(*BitcaskClient)

	switch command {
	case "quit":
		_ = conn.Close()
	case "ping":
		conn.WriteString("pong")
	default:
		res, err := cmdFunc(client, cmd.Args[1:])
		if err != nil {
			if err == bitcaskGo.ErrKeyNotFound {
				conn.WriteNull()
			} else {
				conn.WriteError(err.Error())
			}
			return
		}
		conn.WriteAny(res)
	}
}

// ---------------------String method--------------------------

func set(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("set")
	}

	key, value := args[0], args[1]
	if err := cli.db.Set(key, 0, value); err != nil {
		return nil, err
	}
	return redcon.SimpleString("OK"), nil
}

func get(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("get")
	}
	key := args[0]
	value, err := cli.db.Get(key)
	if err != nil {
		return nil, err
	}
	return value, nil
}

// ---------------------Hash method--------------------------

func hset(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("hset")
	}
	var ok = 0
	key, field, value := args[0], args[1], args[2]
	res, err := cli.db.HSet(key, field, value)

	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}
	return redcon.SimpleInt(ok), nil
}

func hget(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hget")
	}
	key, field := args[0], args[1]
	value, err := cli.db.HGet(key, field)
	if err != nil {
		return nil, err
	}
	return value, nil
}

func hdel(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("hdel")
	}
	var ok = 0
	key, field := args[0], args[1]
	res, err := cli.db.HDel(key, field)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

// ---------------------Set method--------------------------

func sadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sadd")
	}
	key, member := args[0], args[1]
	var ok = 0
	res, err := cli.db.SAdd(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func sismember(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("sismember")
	}
	key, member := args[0], args[1]
	var ok = 0
	res, err := cli.db.SIsMember(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func srem(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("srem")
	}
	key, member := args[0], args[1]
	var ok = 0
	res, err := cli.db.SRem(key, member)
	if err != nil {
		return nil, err
	}
	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

// ---------------------List method--------------------------

func lpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("lpush")
	}

	key, element := args[0], args[1]
	size, err := cli.db.LPush(key, element)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(size), nil
}

func rpush(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("rpush")
	}

	key, element := args[0], args[1]
	size, err := cli.db.RPush(key, element)
	if err != nil {
		return nil, err
	}

	return redcon.SimpleInt(size), nil
}

func lpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("lpop")
	}
	key := args[0]
	element, err := cli.db.LPop(key)
	if err != nil {
		return nil, err
	}

	return element, nil
}

func rpop(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 1 {
		return nil, newWrongNumberOfArgsError("rpop")
	}
	key := args[0]
	element, err := cli.db.RPop(key)
	if err != nil {
		return nil, err
	}

	return element, nil
}

// ---------------------ZSet method--------------------------

func zadd(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 3 {
		return nil, newWrongNumberOfArgsError("zadd")
	}

	var ok = 0
	key, score, member := args[0], args[1], args[2]
	res, err := cli.db.ZAdd(key, utils.BytesToFloat64(score), member)
	if err != nil {
		return nil, err
	}

	if res {
		ok = 1
	}

	return redcon.SimpleInt(ok), nil
}

func zscore(cli *BitcaskClient, args [][]byte) (interface{}, error) {
	if len(args) != 2 {
		return nil, newWrongNumberOfArgsError("zscore")
	}
	key, member := args[0], args[1]
	score, err := cli.db.ZScore(key, member)
	if err != nil {
		return nil, err
	}

	return score, nil
}
