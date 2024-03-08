package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"math"
	"os"
	"runtime"
	"slices"
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
	minTemp   int32
	maxTemp   int32
	sumTemp   int32
	dataCount int32
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
			var readBuffer []byte = make([]byte, 128)
			n, fileReadError := file.ReadAt(readBuffer, chunkLimitsData[id].readFrom)
			if fileReadError != nil && fileReadError != io.EOF {
				check(fileReadError)
			}
			var i int = 0
			for i < n {
				if readBuffer[i] == 10 {
					break
				}
				i++
			}
			i++
			chunkLimitsData[id].displacement = int64(i)
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

	var chunkLimitsDataFinal []fileChunkLimits = make([]fileChunkLimits, 0)
	for i := 0; i < numberOfFileChunks; i++ {
		if chunkLimitsData[i].bytesToRead > 4 {
			chunkLimitsDataFinal = append(chunkLimitsDataFinal, chunkLimitsData[i])
		}
	}

	fmt.Printf("Time taken to get chunk limits: %v\n", time.Since(startTime))
	return chunkLimitsDataFinal, stat.Size()
}

func loadFileIntoMemory(fileName string, cpuc int) []map[string]temperatureData {
	cs, _ := getChunkSizes(cpuc, fileName)

	var fileContent [][]byte = make([][]byte, cpuc)
	for i, data := range cs {
		fileContent[i] = make([]byte, data.bytesToRead)
	}

	var tempMap []map[string]temperatureData = make([]map[string]temperatureData, cpuc)

	waitGroup := sync.WaitGroup{}
	for i := 0; i < cpuc; i++ {
		waitGroup.Add(1)
		go func(id int, content *[]byte) {
			defer waitGroup.Done()

			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()

			var readBuffer []byte = make([]byte, 1024*1024*2)
			totalBytesReadByThisGoRoutine := int64(0)
			for totalBytesReadByThisGoRoutine < cs[id].bytesToRead {
				n, fileReadErr := file.ReadAt(readBuffer, cs[id].readFrom+totalBytesReadByThisGoRoutine)
				if fileReadErr != nil {
					if fileReadErr != io.EOF {
						check(fileReadErr)
					}
				}
				if n > int(cs[id].bytesToRead-totalBytesReadByThisGoRoutine) {
					n = int(cs[id].bytesToRead - totalBytesReadByThisGoRoutine)
				}
				for l := 0; l < n; l++ {
					(*content)[totalBytesReadByThisGoRoutine+int64(l)] = readBuffer[l]
				}
				totalBytesReadByThisGoRoutine += int64(n)
			}

			waitGroup.Add(1)
			go func(id int, content *[]byte, tempMap *[]map[string]temperatureData) {
				defer waitGroup.Done()

				tm := make(map[string]temperatureData)
				var city string
				var temp int32
				var l int = 64
				for i := 0; i < len(*content); i++ {
					l = 64
					if l > len((*content)[i:]) {
						l = len((*content)[i:])
					}
					sep2 := i + bytes.IndexByte((*content)[i:i+l], 59)
					nl2 := i + bytes.IndexByte((*content)[i:i+l], 10)
					if sep2 != -1 && nl2 != -1 && nl2 > sep2 {
						city = string((*content)[i:sep2])
						temp = toInt((*content)[sep2+1 : nl2])
					}
					i = nl2
					ed, ok := tm[city]
					if !ok {
						ed = temperatureData{
							minTemp:   temp,
							maxTemp:   temp,
							sumTemp:   temp,
							dataCount: 1,
						}
					} else {
						ed.dataCount++
						ed.sumTemp += temp
						if temp < ed.minTemp {
							ed.minTemp = temp
						}
						if temp > ed.maxTemp {
							ed.maxTemp = temp
						}
					}
					tm[city] = ed
				}
				(*tempMap)[id] = tm
			}(id, &fileContent[id], &tempMap)

		}(i, &fileContent[i])
	}
	waitGroup.Wait()

	return tempMap
}

func toInt(b []byte) int32 {
	r := int32(0)
	s := int32(1)
	k := int32(1)
	if b[0] == '-' {
		s = -1
	}
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] != '.' && b[i] != '-' {
			r += (int32(b[i]) - 48) * k
			k *= 10
		}
	}
	return s * r
}

func mergeTemperatureData(cityDatas *[]map[string]temperatureData) *map[string]temperatureData {
	var startTime = time.Now()

	var mergedCityTemperatureData map[string]temperatureData = make(map[string]temperatureData)
	for idx := 0; idx < len(*cityDatas); idx++ {
		cityData := (*cityDatas)[idx]
		for city, entry := range cityData {
			mergedEntry, ok := mergedCityTemperatureData[city]
			if !ok {
				mergedCityTemperatureData[city] = entry
			} else {
				mergedEntry.dataCount += entry.dataCount
				mergedEntry.sumTemp += entry.sumTemp
				if entry.minTemp < mergedEntry.minTemp {
					mergedEntry.minTemp = entry.minTemp
				}
				if entry.maxTemp > mergedEntry.maxTemp {
					mergedEntry.maxTemp = entry.maxTemp
				}
				mergedCityTemperatureData[city] = mergedEntry
			}
		}
	}

	fmt.Printf("Time taken to merge data: %v\n", time.Since(startTime))
	return &mergedCityTemperatureData
}

func printDataSorted(cityDatas *map[string]temperatureData, noprint bool) {
	var startTime = time.Now()
	keys := make([]string, 0, len(*cityDatas))
	for k := range *cityDatas {
		keys = append(keys, k)
	}
	slices.Sort(keys)

	if !noprint {
		fmt.Print("{")
		for i, k := range keys {
			fmt.Printf("%v=%.1f/%.1f/%.1f", k, float64((*cityDatas)[k].minTemp)/10.0, float64((*cityDatas)[k].sumTemp)/float64((*cityDatas)[k].dataCount)/10.0, float64((*cityDatas)[k].maxTemp)/10.0)
			if i != len(keys)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Print("}\n")
	}

	fmt.Printf("Time taken to sort and print data: %v\n", time.Since(startTime))
}

func main() {
	var cycle = flag.Int("cycle", 1, "number of run cycles")
	var input = flag.String("input", "measurements.txt", "input file name")
	flag.Parse()
	minTime := time.Duration(0)
	for i := 0; i < *cycle; i++ {
		var startTime = time.Now()
		td := loadFileIntoMemory(*input, runtime.NumCPU())
		md := mergeTemperatureData(&td)
		if i == *cycle-1 {
			printDataSorted(md, false)
		} else {
			printDataSorted(md, true)
		}
		fmt.Println("Total time taken to process: ", time.Since(startTime))
		if i == 0 {
			minTime = time.Since(startTime)
		} else {
			if time.Since(startTime) < minTime {
				minTime = time.Since(startTime)
			}
		}
	}
	fmt.Println("Fastest processing time: ", minTime)
}
