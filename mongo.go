// Copyright (c) 2018 Copyright Holder All Rights Reserved.
package main

import (
	"log"
	"os"
	"strconv"
	"strings"

	"bytes"
	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const (
	url            = "114.112.74.30:60013"
	dbName         = "live_struct_db"
	collectionName = "live_data"

	equalSign     = "="
	logPathPrefix = "logpath="
	startTime     = "startTime="
	endTime       = "endTime="
	contentP      = "content="
	logSeq        = "!#log#!"
)

func main() {
	args := os.Args
	programName := args[0]
	logPath := "."
	if strings.HasPrefix(args[1], logPathPrefix) {
		logPath = strings.Split(args[1], equalSign)[1]
	}
	var startT uint64
	if strings.HasPrefix(args[2], startTime) {
		var err error
		startT, err = strconv.ParseUint(strings.Split(args[2], equalSign)[1], 10, 64)
		if err != nil {
			log.Fatalf("开始时间【%s】赋值异常", startTime)
		}
	}
	var endT uint64
	if strings.HasPrefix(args[3], endTime) {
		var err error
		endT, err = strconv.ParseUint(strings.Split(args[3], equalSign)[1], 10, 64)
		if err != nil {
			log.Fatalf("结束时间【%s】赋值异常", endTime)
		}
	}
	content := ""
	if strings.HasPrefix(args[4], contentP) {
		content = strings.Split(args[4], equalSign)[1]
	}
	if strings.HasSuffix(content, ".flv") || strings.HasSuffix(content, ".m3u8") {

	} else {
		log.Fatalf("导出原始频道信息参数【%s】赋值异常", contentP)
	}
	suffix := string([]rune(content)[len(content)-8:])
	logFile, _ := os.Create(logPath + "/redcdn_original_log_" + suffix + ".log")
	// logger := log.New(logFile, "// DEBUG: ", log.LstdFlags|log.Lshortfile)
	logger := log.New(logFile, "", log.LstdFlags)
	logger.Printf("工具【%s】开始启动", programName)
	defer logFile.Close()
	defer logger.Printf("工具【%s】结束", programName)

	session, err := mgo.Dial(url)
	if err != nil {
		logger.Fatalln("连接MongoDB服务异常")
	}
	defer session.Close()
	session.SetMode(mgo.SecondaryPreferred, true)
	conn := session.DB(dbName).C(collectionName)
	filter := bson.M{"playtime": bson.M{"$gt": startT, "$lt": endT}, "content": content}
	display := bson.M{"_id": 0, "uid": 1, "log": 1}
	count, err := conn.Find(filter).Count()
	if err != nil {
		logger.Fatalln("查询异常:", err)
	}
	logger.Printf("%s集合中, 符合过滤条件的文档总数：%d", collectionName, count)
	pageCount := 10000
	totalPage := 0
	if count%pageCount == 0 {
		totalPage = count / pageCount
	} else {
		totalPage = count/pageCount + 1
	}
	uids := make(map[string]string, count)
	for skip := 0; skip < totalPage; {
		inter := conn.Find(filter).Select(display).Limit(pageCount).Iter()
		result := bson.M{}
		for inter.Next(result) {
			//logger.Println(result["log"])
			uid := result["uid"].(string)
			log, exist := uids[uid]
			if exist {
				b := bytes.Buffer{}
				b.WriteString(log)
				b.WriteString(logSeq)
				b.WriteString(result["log"].(string))
				uids[uid] = b.String()
			} else {
				uids[uid] = result["log"].(string)
			}
		}
		skip += pageCount
	}

	loadPlayerFilter := bson.M{"playtime": bson.M{"$gt": startT, "$lt": endT}, "event_type": "LoadPlayer"}
	uidFilter := make([]bson.M, len(uids))
	cnt := 0
	for key := range uids {
		uidFilter[cnt] = bson.M{"uid":key}
		cnt++
	}
	loadPlayerFilter["$or"] = uidFilter

	loadPlayLogs := make([]bson.M, len(uids))
	errSel := conn.Find(loadPlayerFilter).Select(display).Limit(pageCount).All(&loadPlayLogs)
	if errSel != nil {
		logger.Fatal(errSel)
	}
	for i := 0; i < len(loadPlayLogs); i++ {
		curr := loadPlayLogs[i]
		uid := curr["uid"].(string)
		log, exist := uids[uid]
		if exist {
			b := bytes.Buffer{}
			b.WriteString(log)
			b.WriteString(logSeq)
			b.WriteString(curr["log"].(string))
			uids[uid] = b.String()
		} else {
			uids[uid] = curr["log"].(string)
		}
	}

	for key, value := range uids {
		logger.Printf("用户【%s】在观看视频【%s】的过程中, 产生的日志如下：", key, content)
		logs := strings.Split(value, logSeq)
		for i := 0; i < len(logs); i++ {
			logger.Println(logs[i])
		}
	}
}
