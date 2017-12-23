package kafka_benchtest

import (
	"bytes"
	"fmt"
	"github.com/nfnt/resize"
	"image"
	"image/jpeg"
	"log"
	"os"
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

// func main() {
// 	// open "test.jpg"
// 	file, err := os.Open("test.jpg")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer file.Close()
// 	// decode jpeg into image.Image
// 	img, err := jpeg.Decode(file)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	width, _ := getImageDimension("test.jpg")
// 	new_width := uint(width / 2)
// 	// resize to width 1000 using Lanczos resampling
// 	// and preserve aspect ratio
// 	m := resize.Resize(new_width, 0, img, resize.Lanczos3)

// 	out, err := os.Create("test_resized.jpg")
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	defer out.Close()
// 	// write new image to file
// 	jpeg.Encode(out, m, nil)
// 	// convert image to []byte
// 	buf := new(bytes.Buffer)
// 	err = jpeg.Encode(buf, m, nil)
// 	if err != nil {
// 		log.Fatal(err)
// 	}
// 	// convert []byte to image for saving to file
// 	newimg, _, _ := image.Decode(bytes.NewReader(buf.Bytes()))

// 	//save the imgByte to file
// 	newout, err := os.Create("new_resize.jpg")

// 	if err != nil {
// 		fmt.Println(err)
// 		os.Exit(1)
// 	}

// 	err = jpeg.Encode(newout, newimg, nil)

// 	if err != nil {
// 		fmt.Println(err)
// 		os.Exit(1)
// 	}
// }
