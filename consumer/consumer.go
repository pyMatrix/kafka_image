package main

import (
	"bytes"
	"fmt"
	"github.com/Shopify/sarama"
	"image"
	"image/jpeg"
	"log"
	"os"
	"strings"
	"sync"
)

var (
	wg     sync.WaitGroup
	logger = log.New(os.Stderr, "[srama]", log.LstdFlags)
)

func main() {
	sarama.Logger = logger
	consumer, err := sarama.NewConsumer(strings.Split("gxm-k1:9092,gxm-k2:9092,gxm-k3:9092,", ","), nil)

	if err != nil {
		logger.Println("Failed to start consumer: %s", err)
	}
	partitionList, err := consumer.Partitions("k1testnew")

	if err != nil {
		logger.Println("Failed to get the list of partitions: ", err)
	}
	for partition := range partitionList {
		pc, err := consumer.ConsumePartition("k1testnew", int32(partition), sarama.OffsetNewest)
		if err != nil {
			logger.Printf("Failed to start consumer for partition %d: %s\n", partition, err)
		}
		defer pc.AsyncClose()
		wg.Add(1)
		go func(sarama.PartitionConsumer) {
			defer wg.Done()
			for msg := range pc.Messages() {
				img, _, _ := image.Decode(bytes.NewReader(msg.Value))
				//save the imgByte to file
				out, err := os.Create(string(msg.Key))
				if err != nil {
					log.Fatal("create file fail:", err)
				}
				err = jpeg.Encode(out, img, nil)
				if err != nil {
					log.Fatal("jpeg encode err:", err)
				}
				fmt.Printf("Partition:%d, Offset:%d, Key:%s\n", msg.Partition, msg.Offset, string(msg.Key))
			}
		}(pc)
	}
	wg.Wait()
	logger.Println("Done consuming topic")
	consumer.Close()
}
