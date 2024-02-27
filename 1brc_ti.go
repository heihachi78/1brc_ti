package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"sync"
	"time"
)

type fileChunkLimits struct {
	readFrom     int64
	readTo       int64
	displacement int64
	bytesToRead  int64
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func getChunkSizes(numberOfFileChunks int, fileName string) ([]fileChunkLimits, int64) {
	var startTime = time.Now()

	file, fileOpenError := os.Open(fileName)
	check(fileOpenError)
	defer file.Close()
	stat, fileStatError := file.Stat()
	check(fileStatError)

	initialChunkSize := int64(math.Ceil(float64(stat.Size()) / float64(numberOfFileChunks)))
	var chunkLimitsData []fileChunkLimits = make([]fileChunkLimits, numberOfFileChunks)
	chunkLimitsData[0].readFrom = 0
	chunkLimitsData[0].readTo = initialChunkSize
	chunkLimitsData[0].displacement = 0
	chunkLimitsData[0].bytesToRead = initialChunkSize
	for i := 1; i < numberOfFileChunks; i++ {
		chunkLimitsData[i].readFrom = chunkLimitsData[i-1].readTo
		chunkLimitsData[i].readTo = chunkLimitsData[i].readFrom + initialChunkSize
		chunkLimitsData[i].displacement = 0
	}

	waitGroup := sync.WaitGroup{}
	for idx := 1; idx < numberOfFileChunks; idx++ {
		waitGroup.Add(1)
		go func(id int, chunkLimitsData []fileChunkLimits, file *os.File) {
			defer waitGroup.Done()
			var readBuffer []byte = make([]byte, 1024)
			n, fileReadError := file.ReadAt(readBuffer, chunkLimitsData[id].readFrom)
			if fileReadError != nil && fileReadError != io.EOF {
				check(fileReadError)
			}
			var i int = 0
			for readBuffer[i] != '\n' && readBuffer[i] != '\r' && i < n {
				i++
				if i+1 < n && (readBuffer[i+1] == '\n' || readBuffer[i+1] == '\r') {
					i++
				}
			}
			chunkLimitsData[id].displacement = int64(i + 1)
		}(idx, chunkLimitsData, file)
	}
	waitGroup.Wait()

	for i := 1; i < numberOfFileChunks; i++ {
		chunkLimitsData[i-1].readTo += chunkLimitsData[i].displacement
		chunkLimitsData[i].readFrom += chunkLimitsData[i].displacement
	}
	chunkLimitsData[numberOfFileChunks-1].readTo = stat.Size()
	for i := 0; i < numberOfFileChunks; i++ {
		chunkLimitsData[i].bytesToRead = chunkLimitsData[i].readTo - chunkLimitsData[i].readFrom
	}

	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to get chunk limits: %v\n", runTime)
	return chunkLimitsData, stat.Size()
}

func readFileInChunks(fileName string, chunkLimitsData []fileChunkLimits, readBufferLength int) int64 {
	var startTime = time.Now()

	numberOfFileChunks := len(chunkLimitsData)
	totalBytesReadByAllGoRoutines := int64(0)

	waitGroup := sync.WaitGroup{}
	for idx := 0; idx < numberOfFileChunks; idx++ {
		waitGroup.Add(1)
		go func(id int, fileName string, readBufferLength int) {
			var readStartTime = time.Now()
			defer waitGroup.Done()

			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()

			var readBuffer []byte = make([]byte, readBufferLength)
			totalBytesReadByThisGoRoutine := int64(0)
			for totalBytesReadByThisGoRoutine < chunkLimitsData[id].bytesToRead {
				n, fileReadErr := file.ReadAt(readBuffer, chunkLimitsData[id].readFrom)
				if fileReadErr != nil && fileReadErr != io.EOF {
					check(fileReadErr)
				}
				if n > int(chunkLimitsData[id].bytesToRead-totalBytesReadByThisGoRoutine) {
					n = int(chunkLimitsData[id].bytesToRead - totalBytesReadByThisGoRoutine)
				}
				totalBytesReadByThisGoRoutine += int64(n)
			}
			totalBytesReadByAllGoRoutines += totalBytesReadByThisGoRoutine
			fmt.Printf("%d read %d bytes from %d bytes starting at %d\n", id, totalBytesReadByThisGoRoutine, chunkLimitsData[id].bytesToRead, chunkLimitsData[id].readFrom)
			var readRunTime = time.Since(readStartTime)
			fmt.Printf("Time taken to read file %d chunk: %v\n", id, readRunTime)
		}(idx, fileName, readBufferLength)
	}
	waitGroup.Wait()

	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to read file in chunks: %v\n", runTime)
	return totalBytesReadByAllGoRoutines
}

func main() {
	var inputFile = flag.String("inputfile", "measurements.txt", "name of the file to process with the temperatures data")
	var numberOfChunks = flag.Int("filechunks", 4, "number of chunk to process the file")
	var readBufferLength = flag.Int("readbuffer", 524288, "length of the read buffer, the amount we read at a time")
	flag.Parse()
	chunkLimitsData, fileSize := getChunkSizes(*numberOfChunks, *inputFile)
	totalBytesReadByAllGoRoutines := readFileInChunks(*inputFile, chunkLimitsData, *readBufferLength)
	fmt.Println(fileSize, totalBytesReadByAllGoRoutines, chunkLimitsData)
}
