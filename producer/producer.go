package main

import (
	"flag"
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

import (
	"bytes"
	_ "github.com/nfnt/resize"
	"image"
	"image/jpeg"
)

var (
	ImgPath *string
	logger  = log.New(os.Stderr, "[srama]", log.LstdFlags)
	// kafka broker list config
	brokerList = "gxm-k1:9092,gxm-k2:9092,gxm-k3:9092,gxm-k4:9092,gxm-k5:9092"
)

const (
	// 货柜数量，模拟并发数
	SHELF_NUM = 6
)

func init() {
	ImgPath = flag.String("path", "../image", "a string")
}

func getImageDimension(imagePath string) (int, int) {
	file, err := os.Open(imagePath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
	}

	image, _, err := image.DecodeConfig(file)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%s: %v\n", imagePath, err)
	}
	return image.Width, image.Height
}

func GetImgDataByFile(imgPath string, rsz int) (imgByte []byte, err error) {
	// open image file
	file, err := os.Open(imgPath)
	if err != nil {
		log.Fatal("open image err:", err)
	}
	defer file.Close()
	// decode jpeg into image.Image
	img, err := jpeg.Decode(file)
	if err != nil {
		log.Fatal(err)
	}
	// width, _ := getImageDimension(imgPath)
	// new_width := uint(width / rsz)
	// // resize to width using Lanczos resampling
	// // and preserve aspect ratio
	// m := resize.Resize(new_width, 0, img, resize.Lanczos3)
	// convert image to []byte
	buf := new(bytes.Buffer)
	err = jpeg.Encode(buf, img, nil)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes(), err
}

func ListDir(dirPth string, suffix string) (files, fileName []string, err error) {
	dir, err := ioutil.ReadDir(dirPth)
	if err != nil {
		log.Fatal("readdir err:", err)
	}
	PthSep := string(os.PathSeparator)
	suffix = strings.ToUpper(suffix) //忽略后缀匹配的大小写
	for _, fi := range dir {
		if fi.IsDir() { // 忽略目录
			continue
		}
		if strings.HasSuffix(strings.ToUpper(fi.Name()), suffix) { //匹配文件
			files = append(files, dirPth+PthSep+fi.Name())
			fileName = append(fileName, fi.Name())
		}
	}
	return files, fileName, nil
}

func produceMSG(imgPath string) {
	sarama.Logger = logger
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(strings.Split(brokerList, ","), config)
	if err != nil {
		logger.Println("Failed to produce message:", err)
		os.Exit(500)
	}
	defer producer.Close()
	files, fileName, err := ListDir(imgPath, "jpg")
	if err != nil {
		logger.Println("Failed to list dir:", err)
		os.Exit(500)
	}
	var wg sync.WaitGroup
	ch := make(chan struct{}, SHELF_NUM)
	for i := 0; i < len(files); i++ {
		wg.Add(1)
		ch <- struct{}{}
		go func(index int) {
			defer func() {
				wg.Done()
				<-ch
			}()
			imgBytes, err := GetImgDataByFile(files[i], 1)
			if err != nil {
				log.Fatal("getImgDataByFile error:", err)
			}
			msg := &sarama.ProducerMessage{}
			msg.Topic = "testbench"
			msg.Partition = int32(-1)
			msg.Key = sarama.StringEncoder(fileName[i])
			msg.Value = sarama.ByteEncoder(imgBytes)
			partition, offset, err := producer.SendMessage(msg)
			if err != nil {
				logger.Println("Failed to produce message: ", err)
			}
			logger.Printf("partition=%d, offset=%d\n, filename:%s", partition, offset, msg.Key)
		}(i)
		wg.Wait()
	}
}

func main() {
	flag.Parse()
	logger.Println("input path is:", *ImgPath)
	t1 := time.Now() // get current time
	produceMSG(*ImgPath)
	elapsed := time.Since(t1)
	logger.Println("time elapsed: ", elapsed)
}
