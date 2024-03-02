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

type rowData struct {
	city []byte
	temp []byte
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}

func processFileSimpleGoRoutine(fileName string, chunkLimitsData []fileChunkLimits, readBufferLength int) []map[string]temperatureData {
	var startTime = time.Now()

	numberOfFileChunks := len(chunkLimitsData)
	var cityDatas []map[string]temperatureData = make([]map[string]temperatureData, numberOfFileChunks)
	for i := 0; i < numberOfFileChunks; i++ {
		cityDatas[i] = make(map[string]temperatureData)
	}

	fileReadWaitGroup := sync.WaitGroup{}
	for idx := 0; idx < numberOfFileChunks; idx++ {
		fileReadWaitGroup.Add(1)
		go func(id int, fileName string, readBufferLength int) {
			defer fileReadWaitGroup.Done()

			var cityData map[string]temperatureData = make(map[string]temperatureData)

			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()

			var readBuffer []byte = make([]byte, readBufferLength)
			var unprocessedBuffer []byte
			totalBytesReadByThisGoRoutine := int64(0)
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
						cityData[city] = entry
						notProcessedFrom = l + 2
					}
				}
				unprocessedBuffer = bytesToProcess[notProcessedFrom:]
			}
			file.Close()
			cityDatas[id] = cityData
		}(idx, fileName, readBufferLength)
	}
	fileReadWaitGroup.Wait()

	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to read the entire file content: %v\n", runTime)
	return cityDatas
}

func processFileMultipleGoRoutines(fileName string, chunkLimitsData []fileChunkLimits, readBufferLength int, channelBufferLength int) []map[string]temperatureData {
	var startTime = time.Now()

	numberOfFileChunks := len(chunkLimitsData)
	var lineChannels []chan []byte = make([]chan []byte, numberOfFileChunks)
	for i := 0; i < numberOfFileChunks; i++ {
		lineChannels[i] = make(chan []byte, channelBufferLength)
	}
	var cityDatas []map[string]temperatureData = make([]map[string]temperatureData, numberOfFileChunks)
	for i := 0; i < numberOfFileChunks; i++ {
		cityDatas[i] = make(map[string]temperatureData)
	}

	fileReadWaitGroup := sync.WaitGroup{}
	lineProcessWaitGroup := sync.WaitGroup{}
	for idx := 0; idx < numberOfFileChunks; idx++ {

		fileReadWaitGroup.Add(1)
		go func(id int, fileName string, readBufferLength int, lineChannel chan []byte) {
			defer fileReadWaitGroup.Done()

			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()

			var readBuffer []byte = make([]byte, readBufferLength)
			var unprocessedBuffer []byte
			totalBytesReadByThisGoRoutine := int64(0)
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
				for l := 0; l < len(bytesToProcess)-1; l++ {
					if (bytesToProcess[l] == 13 && bytesToProcess[l+1] == 10) || (bytesToProcess[l] == 10 && bytesToProcess[l+1] == 13) {
						lineChannel <- bytesToProcess[notProcessedFrom:l]
						notProcessedFrom = l + 2
					}
				}
				unprocessedBuffer = bytesToProcess[notProcessedFrom:]
			}
			file.Close()
			close(lineChannel)
		}(idx, fileName, readBufferLength, lineChannels[idx])

		lineProcessWaitGroup.Add(1)
		go func(id int, lineChannel chan []byte) {
			defer lineProcessWaitGroup.Done()

			var cityData map[string]temperatureData = make(map[string]temperatureData)

			for lineBytes := range lineChannel {
				separatorIndex := 0
				for l := 0; l < len(lineBytes)-1; l++ {
					if lineBytes[l] == 59 {
						separatorIndex = l
					}
				}
				city := string(lineBytes[:separatorIndex])
				temp := string(lineBytes[separatorIndex+1:])
				tempFloat64, conversionError := strconv.ParseFloat(temp, channelBufferLength)
				check(conversionError)
				entry, ok := cityData[city]
				if !ok {
					entry = temperatureData{
						minTemp:   tempFloat64,
						maxTemp:   tempFloat64,
						sumTemp:   tempFloat64,
						dataCount: 1,
					}
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
				cityData[city] = entry
			}

			cityDatas[id] = cityData
		}(idx, lineChannels[idx])
	}
	fileReadWaitGroup.Wait()
	lineProcessWaitGroup.Wait()

	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to read the entire file content: %v\n", runTime)
	return cityDatas
}

func loadFileIntoMemory(fileName string, chunkLimitsData []fileChunkLimits, readBufferLength int) []map[string]temperatureData {
	var startTime = time.Now()

	numberOfFileChunks := len(chunkLimitsData)

	var fileContent [][]byte = make([][]byte, numberOfFileChunks)
	for i, data := range chunkLimitsData {
		fileContent[i] = make([]byte, data.bytesToRead)
	}

	var readDone []chan int64 = make([]chan int64, numberOfFileChunks)
	for i := 0; i < numberOfFileChunks; i++ {
		readDone[i] = make(chan int64)
	}

	var returnCityData map[string]temperatureData = make(map[string]temperatureData)
	var mapChannel chan map[string]temperatureData = make(chan map[string]temperatureData)

	syncGroup := sync.WaitGroup{}

	for idx := 0; idx < numberOfFileChunks; idx++ {

		syncGroup.Add(1)
		go func(id int, fileName string, readBufferLength int, content *[]byte, readDone chan int64) {
			defer syncGroup.Done()
			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()
			defer close(readDone)

			var readBuffer []byte = make([]byte, readBufferLength)
			totalBytesReadByThisGoRoutine := int64(0)
			for totalBytesReadByThisGoRoutine < chunkLimitsData[id].bytesToRead {
				n, fileReadErr := file.ReadAt(readBuffer, chunkLimitsData[id].readFrom+totalBytesReadByThisGoRoutine)
				if fileReadErr != nil {
					if fileReadErr != io.EOF {
						check(fileReadErr)
					} else {
						break
					}
				}
				if n > int(chunkLimitsData[id].bytesToRead-totalBytesReadByThisGoRoutine) {
					n = int(chunkLimitsData[id].bytesToRead - totalBytesReadByThisGoRoutine)
				}
				for l := 0; l < n; l++ {
					(*content)[totalBytesReadByThisGoRoutine+int64(l)] = readBuffer[l]
				}
				totalBytesReadByThisGoRoutine += int64(n)
				readDone <- totalBytesReadByThisGoRoutine
			}
		}(idx, fileName, readBufferLength, &fileContent[idx], readDone[idx])

		syncGroup.Add(1)
		go func(id int, bytesToProcess *[]byte, readDone chan int64, mapChannel chan map[string]temperatureData) {
			defer syncGroup.Done()
			var lastSeparatorIndex int
			var lastNewLineIndex int
			var city string
			var totalBytesprocessedThisGoRoutine int64 = 0
			var collectedRows []rowData = make([]rowData, 2048)
			var collectedRowsIndex int = 0
			for totalBytesReadByThisGoRoutine := range readDone {
				for byteIdx := totalBytesprocessedThisGoRoutine; byteIdx < totalBytesReadByThisGoRoutine; byteIdx++ {
					if (*bytesToProcess)[byteIdx] == byte(13) {
						lastSeparatorIndex = bytes.LastIndex((*bytesToProcess)[:byteIdx], []byte{';'})
						collectedRows[collectedRowsIndex].city = (*bytesToProcess)[lastNewLineIndex+1 : lastSeparatorIndex]
						lastNewLineIndex = bytes.LastIndex((*bytesToProcess)[:byteIdx], []byte{'\n'})
						collectedRows[collectedRowsIndex].temp = (*bytesToProcess)[lastSeparatorIndex+1 : byteIdx-1]
						collectedRowsIndex++
						if collectedRowsIndex == len(collectedRows) {
							syncGroup.Add(1)
							go func(collectedRows []rowData, mapChannel chan map[string]temperatureData) {
								defer syncGroup.Done()
								var cd map[string]temperatureData = make(map[string]temperatureData)
								for x := 0; x < len(collectedRows); x++ {
									entry, ok := cd[string(collectedRows[x].city)]

									tb := collectedRows[x].temp
									ct := bytes.LastIndex(tb, []byte{'\r'})
									if ct >= 0 {
										collectedRows[x].temp = tb[:ct]
									}
									tempFloat64, conversionError := strconv.ParseFloat(string(collectedRows[x].temp), 64)
									if conversionError != nil {
										tempFloat64, conversionError := strconv.ParseFloat(string(collectedRows[x].temp[:len(collectedRows[x].temp)-1]), 64)
										check(conversionError)
										_ = tempFloat64
									}

									if !ok {
										entry = temperatureData{
											minTemp:   tempFloat64,
											maxTemp:   tempFloat64,
											sumTemp:   tempFloat64,
											dataCount: 1,
										}
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
									cd[city] = entry
								}
								mapChannel <- cd
							}(collectedRows, mapChannel)

							collectedRowsIndex = 0
						}
					}
				}
				totalBytesprocessedThisGoRoutine = totalBytesReadByThisGoRoutine
			}

			syncGroup.Add(1)
			go func(collectedRows []rowData, mapChannel chan map[string]temperatureData) {
				defer syncGroup.Done()
				var cd map[string]temperatureData = make(map[string]temperatureData)
				for x := 0; x < len(collectedRows); x++ {
					entry, ok := cd[string(collectedRows[x].city)]
					tempFloat64, conversionError := strconv.ParseFloat(string(collectedRows[x].temp), 64)
					check(conversionError)
					if !ok {
						entry = temperatureData{
							minTemp:   tempFloat64,
							maxTemp:   tempFloat64,
							sumTemp:   tempFloat64,
							dataCount: 1,
						}
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
					cd[city] = entry
				}
				mapChannel <- cd
			}(collectedRows[:collectedRowsIndex], mapChannel)

		}(idx, &fileContent[idx], readDone[idx], mapChannel)
	}

	sg := sync.WaitGroup{}
	sg.Add(1)
	go func() {
		defer sg.Done()
		for mcd := range mapChannel {
			for city, entry := range mcd {
				mergedEntry, ok := returnCityData[city]
				if !ok {
					returnCityData[city] = entry
				} else {
					mergedEntry.dataCount += entry.dataCount
					mergedEntry.sumTemp += entry.sumTemp
					if entry.minTemp < mergedEntry.minTemp {
						mergedEntry.minTemp = entry.minTemp
					}
					if entry.maxTemp > mergedEntry.maxTemp {
						mergedEntry.maxTemp = entry.maxTemp
					}
					returnCityData[city] = mergedEntry
				}
			}
		}
	}()

	syncGroup.Wait()
	close(mapChannel)
	sg.Wait()

	var r []map[string]temperatureData = make([]map[string]temperatureData, 1)
	r[0] = returnCityData

	fmt.Printf("Time taken to read the entire file content into memory: %v\n", time.Since(startTime))
	return r
}

func mergeCityDatas(cityDatas *[]map[string]temperatureData) map[string]temperatureData {
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
	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to merge data: %v\n", runTime)
	return mergedCityTemperatureData
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
			fmt.Printf("%v=%.1f/%.1f/%.1f", k, (*cityDatas)[k].minTemp, (*cityDatas)[k].sumTemp/float64((*cityDatas)[k].dataCount), (*cityDatas)[k].maxTemp)
			if i != len(keys)-1 {
				fmt.Print(", ")
			}
		}
		fmt.Print("}\n")
	}
	var runTime = time.Since(startTime)
	fmt.Printf("Time taken to sort and print data: %v\n", runTime)
}

func getChunkSizes2(numberOfFileChunks int, fileName string) ([]fileChunkLimits, int64) {
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

	var chunkLimitsDataFinal []fileChunkLimits = make([]fileChunkLimits, 0)
	for i := 0; i < numberOfFileChunks; i++ {
		if chunkLimitsData[i].bytesToRead > 4 {
			chunkLimitsDataFinal = append(chunkLimitsDataFinal, chunkLimitsData[i])
		}
	}

	fmt.Printf("Time taken to get chunk limits: %v\n", time.Since(startTime))
	return chunkLimitsDataFinal, stat.Size()
}

func readFile(fileName string, byteChan chan<- []byte, cpus int) {
	startTime := time.Now()

	chunkLimitsData, _ := getChunkSizes2(cpus, fileName)
	readBufferLength := 1024 * 1024 * 16

	rwg := sync.WaitGroup{}

	for i := 0; i < cpus; i++ {
		rwg.Add(1)
		go func(chunkLimitsData fileChunkLimits) {
			defer rwg.Done()
			file, fileOpenError := os.Open(fileName)
			check(fileOpenError)
			defer file.Close()

			var readBuffer []byte = make([]byte, readBufferLength)
			var unprocessedBuffer []byte = make([]byte, 0, readBufferLength)
			totalBytesReadByThisGoRoutine := int64(0)
			for totalBytesReadByThisGoRoutine < chunkLimitsData.bytesToRead {
				n, fileReadErr := file.ReadAt(readBuffer, chunkLimitsData.readFrom+totalBytesReadByThisGoRoutine)
				if fileReadErr != nil && fileReadErr != io.EOF {
					check(fileReadErr)
				}
				if n > int(chunkLimitsData.bytesToRead-totalBytesReadByThisGoRoutine) {
					n = int(chunkLimitsData.bytesToRead - totalBytesReadByThisGoRoutine)
				}
				totalBytesReadByThisGoRoutine += int64(n)
				bytesToProcess := append(unprocessedBuffer, readBuffer[:n]...)
				lastNewLineIndex := bytes.LastIndex(bytesToProcess, []byte{'\n'})
				byteChan <- bytesToProcess[:lastNewLineIndex]
				unprocessedBuffer = bytesToProcess[lastNewLineIndex+1:]
			}
		}(chunkLimitsData[i])
	}
	rwg.Wait()
	defer close(byteChan)
	fmt.Printf("Time taken to read the file: %v\n", time.Since(startTime))
}

func readByteChannelAndSendToMapChannel(byteChan <-chan []byte, mapChan chan<- map[string]temperatureData) {
	var resMap map[string]temperatureData = make(map[string]temperatureData)
	var city string
	var cnt int = 0
	for bd := range byteChan {
		lastNewLine := 0
		lastSep := 0
		for i := 0; i < len(bd); i++ {
			b := bd[i]
			switch b {
			case byte(59):
				city = string(bd[lastNewLine:i])
				lastSep = i + 1
			case byte(13):
				temp := string(bd[lastSep:i])
				lastNewLine = i + 2
				tempFloat, tempFloatErr := strconv.ParseFloat(temp, 64)
				check(tempFloatErr)
				entry, ok := resMap[city]
				if !ok {
					entry = temperatureData{
						minTemp:   tempFloat,
						maxTemp:   tempFloat,
						sumTemp:   tempFloat,
						dataCount: 1,
					}
				} else {
					entry.dataCount++
					entry.sumTemp += tempFloat
					if entry.minTemp > tempFloat {
						entry.minTemp = tempFloat
					}
					if entry.maxTemp < tempFloat {
						entry.maxTemp = tempFloat
					}
				}
				resMap[city] = entry
				cnt++
				if cnt > 1024*1024*16 {
					mapChan <- resMap
					cnt = 0
					resMap = make(map[string]temperatureData)
				}
			}
		}
	}
	mapChan <- resMap
}

func readFileInChunks(inputFile string, mapChan chan map[string]temperatureData, cpus int) {
	var byteChan chan []byte = make(chan []byte, 1024)
	wg := sync.WaitGroup{}

	for id := 0; id < cpus; id++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			readByteChannelAndSendToMapChannel(byteChan, mapChan)
		}()
	}
	readFile(inputFile, byteChan, cpus)
	wg.Wait()
	close(mapChan)
}

func processFile(inputFile string, cpus int) map[string]temperatureData {
	var mapChan chan map[string]temperatureData = make(chan map[string]temperatureData, 128)
	go readFileInChunks(inputFile, mapChan, cpus)
	var resMap map[string]temperatureData = make(map[string]temperatureData)
	for md := range mapChan {
		for c, d := range md {
			entry, ok := resMap[c]
			if !ok {
				entry = d
			} else {
				entry.dataCount += d.dataCount
				entry.sumTemp += d.sumTemp
				if entry.minTemp > d.minTemp {
					entry.minTemp = d.minTemp
				}
				if entry.maxTemp < d.maxTemp {
					entry.maxTemp = d.maxTemp
				}
			}
			resMap[c] = entry
		}
	}
	return resMap
}

func main() {
	var inputFile = flag.String("inputfile", "measurements.txt", "name of the file to process with the temperatures data")
	var numberOfChunks = flag.Int("filechunks", runtime.NumCPU(), "number of parallel chunks to process the file, its a good idea to set this to the processor thread or core number")
	var readBufferLength = flag.Int("readbuffer", 1048576, "length of the read buffer, the amount we read at a time")
	var channelBufferLength = flag.Int("channelbuffer", 64, "length of the channel buffer for messageing between go routines")
	var noprint = flag.Bool("noprint", false, "when set, the result will not be printed")
	var processType = flag.Int("processtype", 0, "0 - in memory (uses a lot of memory, 1 - with go routines (uses less memory), 2 - with multiple go routines communicating with channels, 4 - more efficient go routines and channels")
	flag.Parse()

	var startTime = time.Now()
	var cityDatas []map[string]temperatureData
	var mergedCityTemperatureData map[string]temperatureData
	switch *processType {
	case 0:
		chunkLimitsData, _ := getChunkSizes2(*numberOfChunks, *inputFile)
		cityDatas = loadFileIntoMemory(*inputFile, chunkLimitsData, *readBufferLength)
		mergedCityTemperatureData = mergeCityDatas(&cityDatas)
	case 1:
		chunkLimitsData, _ := getChunkSizes2(*numberOfChunks, *inputFile)
		cityDatas = processFileSimpleGoRoutine(*inputFile, chunkLimitsData, *readBufferLength)
		mergedCityTemperatureData = mergeCityDatas(&cityDatas)
	case 2:
		chunkLimitsData, _ := getChunkSizes2(*numberOfChunks, *inputFile)
		cityDatas = processFileMultipleGoRoutines(*inputFile, chunkLimitsData, *readBufferLength, *channelBufferLength)
		mergedCityTemperatureData = mergeCityDatas(&cityDatas)
	case 3:
		mergedCityTemperatureData = processFile(*inputFile, *numberOfChunks)
	}
	printDataSorted(&mergedCityTemperatureData, *noprint)
	fmt.Printf("Time taken to solve the challenge: %v\n", time.Since(startTime))
	fmt.Println(len(mergedCityTemperatureData), *channelBufferLength, *readBufferLength, *numberOfChunks, *inputFile, *processType)
	fmt.Printf("==============================================================================================================================================================\n")
}
