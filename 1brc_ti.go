package main

import (
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"strconv"
	"sync"
	"time"
)

type fileChunkLimits struct {
	readFrom     int64
	readTo       int64
	displacement int64
	bytesToRead  int64
}

type temperatureData struct {
	minTemp   float64
	maxTemp   float64
	sumTemp   float64
	dataCount int64
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
			for i < n {
				if readBuffer[i] == 13 && readBuffer[i+1] == 10 {
					i++
					break
				}
				if readBuffer[i] == 10 && readBuffer[i+1] == 13 {
					i++
					break
				}
				i++
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

func readFileInChunks(fileName string, chunkLimitsData []fileChunkLimits, readBufferLength int) (int64, int64) {
	var startTime = time.Now()

	numberOfFileChunks := len(chunkLimitsData)
	totalBytesReadByAllGoRoutines := int64(0)
	totalLinesReadByAllGoRoutines := int64(0)

	waitGroup := sync.WaitGroup{}
	for idx := 0; idx < numberOfFileChunks; idx++ {
		waitGroup.Add(1)
		go func(id int, fileName string, readBufferLength int) {
			var readStartTime = time.Now()
			defer waitGroup.Done()

			var cityData map[string]temperatureData = make(map[string]temperatureData)

			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()

			var readBuffer []byte = make([]byte, readBufferLength)
			var unprocessedBuffer []byte
			totalBytesReadByThisGoRoutine := int64(0)
			totalLinesReadByThisGoRoutine := int64(0)
			for totalBytesReadByThisGoRoutine < chunkLimitsData[id].bytesToRead {
				n, fileReadErr := file.ReadAt(readBuffer, chunkLimitsData[id].readFrom+totalBytesReadByThisGoRoutine)
				if fileReadErr != nil && fileReadErr != io.EOF {
					check(fileReadErr)
				}
				if n > int(chunkLimitsData[id].bytesToRead-totalBytesReadByThisGoRoutine) {
					n = int(chunkLimitsData[id].bytesToRead - totalBytesReadByThisGoRoutine)
				}
				totalBytesReadByThisGoRoutine += int64(n)
				bytesToProcess := append(unprocessedBuffer, readBuffer[:n]...)
				notProcessedFrom := 0
				lastSeparatorIndex := 0
				for l := 0; l < len(bytesToProcess)-1; l++ {
					if bytesToProcess[l] == 59 {
						lastSeparatorIndex = l
					}
					if (bytesToProcess[l] == 13 && bytesToProcess[l+1] == 10) || (bytesToProcess[l] == 10 && bytesToProcess[l+1] == 13) {
						totalLinesReadByThisGoRoutine++
						//lineBytes := bytesToProcess[notProcessedFrom:l]
						/*NEW GO ROUTINE FROM HERE?*/
						city := string(bytesToProcess[notProcessedFrom:lastSeparatorIndex])
						temp := string(bytesToProcess[lastSeparatorIndex+1 : l])
						tempFloat64, conversionError := strconv.ParseFloat(temp, 64)
						check(conversionError)
						entry, ok := cityData[city]
						if !ok {
							entry = temperatureData{
								minTemp:   tempFloat64,
								maxTemp:   tempFloat64,
								sumTemp:   tempFloat64,
								dataCount: 1,
							}
							cityData[city] = entry
						} else {
							entry.dataCount++
							entry.sumTemp += tempFloat64
							if entry.minTemp > tempFloat64 {
								entry.minTemp = tempFloat64
							}
							if entry.maxTemp < tempFloat64 {
								entry.maxTemp = tempFloat64
							}
						}
						/*NEW GO ROUTINE UNTILL HERE?*/
						notProcessedFrom = l + 2
					}
				}
				unprocessedBuffer = nil
				unprocessedBuffer = bytesToProcess[notProcessedFrom:]
			}
			totalBytesReadByAllGoRoutines += totalBytesReadByThisGoRoutine
			totalLinesReadByAllGoRoutines += totalLinesReadByThisGoRoutine
			//fmt.Printf("%d read %d bytes from %d bytes starting at %d\n", id, totalBytesReadByThisGoRoutine, chunkLimitsData[id].bytesToRead, chunkLimitsData[id].readFrom)
			//fmt.Printf("%d read %d lines\n", id, totalLinesReadByThisGoRoutine)
			fmt.Printf("%d collected %d city data\n", id, len(cityData))
			var readRunTime = time.Since(readStartTime)
			fmt.Printf("Time taken to read chunk %d: %v\n", id, readRunTime)
		}(idx, fileName, readBufferLength)
	}
	waitGroup.Wait()

	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to read the entire file content: %v\n", runTime)
	return totalBytesReadByAllGoRoutines, totalLinesReadByAllGoRoutines
}

func main() {
	var inputFile = flag.String("inputfile", "measurements.txt", "name of the file to process with the temperatures data")
	var numberOfChunks = flag.Int("filechunks", 16, "number of chunk to process the file")
	var readBufferLength = flag.Int("readbuffer", 2097152, "length of the read buffer, the amount we read at a time")
	flag.Parse()
	var startTime = time.Now()
	chunkLimitsData, fileSize := getChunkSizes(*numberOfChunks, *inputFile)
	totalBytesReadByAllGoRoutines, totalLinesReadByAllGoRoutines := readFileInChunks(*inputFile, chunkLimitsData, *readBufferLength)
	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to solve the 1brc: %v\n", runTime)
	fmt.Println(fileSize, totalBytesReadByAllGoRoutines, totalLinesReadByAllGoRoutines, chunkLimitsData)
}
