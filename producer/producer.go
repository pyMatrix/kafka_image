package main

import (
	"fmt"
	"github.com/Shopify/sarama"
	"io/ioutil"
	"log"
	"os"
	"strings"
)

import (
	"bytes"
	"github.com/nfnt/resize"
	"image"
	"image/jpeg"
)

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
	width, _ := getImageDimension(imgPath)
	new_width := uint(width / rsz)
	// resize to width using Lanczos resampling
	// and preserve aspect ratio
	m := resize.Resize(new_width, 0, img, resize.Lanczos3)
	// convert image to []byte
	buf := new(bytes.Buffer)
	err = jpeg.Encode(buf, m, nil)
	if err != nil {
		log.Fatal(err)
	}
	return buf.Bytes(), err
}

var (
	logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
)

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

func produceMSG() {
	sarama.Logger = logger
	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Partitioner = sarama.NewRandomPartitioner
	config.Producer.Return.Successes = true
	producer, err := sarama.NewSyncProducer(strings.Split("gxm-k1:9092,gxm-k2:9092,gxm-k3:9092", ","), config)
	if err != nil {
		logger.Println("Failed to produce message:", err)
		os.Exit(500)
	}
	defer producer.Close()
	files, fileName, err := ListDir("../image", "jpg")
	if err != nil {
		logger.Println("Failed to list dir:", err)
		os.Exit(500)
	}
	for i := 0; i < len(files); i++ {
		imgBytes, err := GetImgDataByFile(files[i], 6)
		if err != nil {
			log.Fatal("getImgDataByFile error:", err)
		}
		msg := &sarama.ProducerMessage{}
		msg.Topic = "k1testnew"
		msg.Partition = int32(-1)
		msg.Key = sarama.StringEncoder(fileName[i])
		msg.Value = sarama.ByteEncoder(imgBytes)
		partition, offset, err := producer.SendMessage(msg)
		if err != nil {
			logger.Println("Failed to produce message: ", err)
		}
		logger.Printf("partition=%d, offset=%d\n", partition, offset)
	}
}

func main() {
	produceMSG()
	// fmt.Println(ListDir("./image", "jpg"))
}