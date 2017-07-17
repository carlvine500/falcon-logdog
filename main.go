package main

import (
	"encoding/json"
	"github.com/fsnotify/fsnotify"
	"github.com/hpcloud/tail"
	"github.com/sdvdxl/falcon-logdog/config"
	"github.com/sdvdxl/falcon-logdog/log"
	"github.com/streamrail/concurrent-map"
	"io/ioutil"
	"net/http"
	"os"
	"path"
	"path/filepath"
	"runtime"
	"strings"
	"time"
	"io"
)

var (
	workers  chan bool
	keywords cmap.ConcurrentMap
)

func main() {

	workers = make(chan bool, runtime.NumCPU()*2)
	keywords = cmap.New()
	runtime.GOMAXPROCS(runtime.NumCPU())

	go func() {
		ticker := time.NewTicker(time.Second * time.Duration(int64(config.Cfg.Timer)))
		for range ticker.C {
			fillData()

			postData()
		}
	}()

	go func() {
		setLogFile()

		log.Info("watch file", config.Cfg.WatchFiles)

		for i := 0; i < len(config.Cfg.WatchFiles); i++ {
			readFileAndSetTail(&(config.Cfg.WatchFiles[i]))
			go logFileWatcher(&(config.Cfg.WatchFiles[i]))

		}

	}()

	select {}
}
func logFileWatcher(file *config.WatchFile) {
	logTail := file.ResultFile.LogTail
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		log.Fatal(err)
	}
	defer watcher.Close()

	done := make(chan bool)

	go func() {
		for {
			select {
			case event := <-watcher.Events:
				log.Debug("event:", event)

				if file.PathIsFile && event.Op == fsnotify.Create && event.Name == file.Path {
					log.Info("continue to watch file:", event.Name)
					if file.ResultFile.LogTail != nil {
						logTail.Stop()
					}

					readFileAndSetTail(file)
				} else {

					if file.ResultFile.FileName == event.Name && (event.Op == fsnotify.Remove || event.Op == fsnotify.Rename) {
						log.Warn(event, "stop to tail")
					} else if event.Op == fsnotify.Create {
						log.Infof("created file %v, basePath:%v", event.Name, path.Base(event.Name))
						if strings.HasSuffix(event.Name, file.Suffix) && strings.HasPrefix(path.Base(event.Name), file.Prefix) {
							if logTail != nil {
								logTail.Stop()
							}
							file.ResultFile.FileName = event.Name
							readFileAndSetTail(file)

						}
					}
				}

			case err := <-watcher.Errors:
				log.Error(err)
			}
		}
	}()

	watchPath := file.Path
	if file.PathIsFile {
		watchPath = filepath.Dir(file.Path)
	}
	err = watcher.Add(watchPath)
	if err != nil {
		log.Fatal(err)

	}
	<-done
}

func readFileAndSetTail(file *config.WatchFile) {
	if file.ResultFile.FileName == "" {
		return
	}
	_, err := os.Stat(file.ResultFile.FileName)
	if err != nil {
		log.Error(file.ResultFile.FileName, err)
		return
	}

	log.Info("read file", file.ResultFile.FileName)
	tail, err := tail.TailFile(file.ResultFile.FileName, tail.Config{Follow: true,Location: &tail.SeekInfo{Offset: 0, Whence: io.SeekEnd}})
	if err != nil {
		log.Fatal(err)
	}

	file.ResultFile.LogTail = tail

	go func() {
		for line := range tail.Lines {
			log.Debug("log line: ", line.Text)
			handleKeywords(*file, line.Text)
		}
	}()

}

func setLogFile() {
	c := config.Cfg
	for i, v := range c.WatchFiles {
		if v.PathIsFile {
			c.WatchFiles[i].ResultFile.FileName = v.Path
			continue
		}

		filepath.Walk(v.Path, func(path string, info os.FileInfo, err error) error {
			cfgPath := v.Path
			if strings.HasSuffix(cfgPath, "/") {
				cfgPath = string([]rune(cfgPath)[:len(cfgPath)-1])
			}
			log.Debug(path)

			//只读取root目录的log
			if filepath.Dir(path) != cfgPath && info.IsDir() {
				log.Debug(path, "not in root path, ignoring , Dir:", path, "cofig path:", cfgPath)
				return err
			}

			log.Debug("path", path, "prefix:", v.Prefix, "suffix:", v.Suffix, "base:", filepath.Base(path), "isFile", !info.IsDir())
			if strings.HasPrefix(filepath.Base(path), v.Prefix) && strings.HasSuffix(path, v.Suffix) && !info.IsDir() {

				if c.WatchFiles[i].ResultFile.FileName == "" || info.ModTime().After(c.WatchFiles[i].ResultFile.ModTime) {
					c.WatchFiles[i].ResultFile.FileName = path
					c.WatchFiles[i].ResultFile.ModTime = info.ModTime()
				}
				return err
			}

			return err
		})

	}
}

// 查找关键词
func handleKeywords(file config.WatchFile, line string) {
	for _, p := range file.Keywords {
		value := 0.0
		if p.Regex.MatchString(line) {
			log.Infof("exp:%v match ===> line: %v ", p.Regex.String(), line)
			value = 1.0
		}

		var data config.PushData


		if v, ok := keywords.Get(p.Tag + "=" + p.Exp); ok {
			d := v.(config.PushData)
			d.Value += value
			data = d
		} else {
			data = config.PushData{Metric: config.Cfg.Metric,
				Endpoint:    config.Cfg.Host,
				Timestamp: time.Now().Unix(),
				Value:       value,
				Step:        config.Cfg.Timer,
				CounterType: "GAUGE",
				Tags:        /*"prefix=" + file.Prefix + ",suffix=" + file.Suffix + "," +*/ p.Tag + "=" + p.FixedExp,
			}
		}

		keywords.Set(p.Tag + "=" + p.Exp, data)

		//rlt,_:=json.Marshal(keywords)
		//log.Debug("==x>"+string(rlt))

	}
}

func postData() {
	c := config.Cfg
	workers <- true
	currentTimestamp:=time.Now().Unix()

	go func() {
		if len(keywords.Items()) != 0 {

			data := make([]config.PushData, 0, 20)
			//rlt,_:=json.Marshal(data)
			//log.Debug("==1>"+string(rlt))
			for k, v := range keywords.Items() {
				// data timestamp bug desc:
				// server record data at 00:30:00,00:31:00
				// client sent two data: {00:30:29=1} send at 00:30:30, {00:30:31=0} at 00:31:30
				// server merge two data as {00:30:00=0},{00:30:29=1} was lost
				pushData := v.(config.PushData)
				pushData.Timestamp = currentTimestamp
				data = append(data, pushData)
				keywords.Remove(k)
			}

			bytes, err := json.Marshal(data)
			//log.Debug("==2>"+string(bytes))
			if err != nil {
				log.Error("marshal push data", data, err)
				return
			}

			log.Info("pushing data:", string(bytes))

			resp, err := http.Post(c.Agent, "plain/text", strings.NewReader(string(bytes)))
			if err != nil {
				log.Error(" post data ", string(bytes), " to agent ", err)
			} else {
				defer resp.Body.Close()
				bytes, _ = ioutil.ReadAll(resp.Body)
				log.Debug(string(bytes))
			}
		}

		<-workers
	}()

}

func fillData() {
	c := config.Cfg
	for _, v := range c.WatchFiles {
		for _, p := range v.Keywords {

			key := p.Tag + "=" + p.Exp
			//log.Debug("_______", key)
			//rlt,_:=json.Marshal(keywords)
			//log.Debug("==3>"+string(rlt))
			if _, ok := keywords.Get(key); ok {
				continue
			}
			//log.Debug("==3.1>"+string(rlt))

			//不存在要插入一个补全
			data := config.PushData{Metric: c.Metric,
				Endpoint:    c.Host,
				Timestamp:   time.Now().Unix(),
				Value:       0.0,
				Step:        c.Timer,
				CounterType: "GAUGE",
				Tags:        /*"prefix=" + v.Prefix + ",suffix=" + v.Suffix + "," +*/ p.Tag + "=" + p.FixedExp,
			}

			keywords.Set(p.Tag + "=" + p.Exp, data)
		}
	}

}
