package main

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"os"
	"runtime"
	"sort"
	"strconv"
	"sync"
	"time"
)

var numWorkers int

type stats struct {
	min, max, sum float64
	count         int64
}

func processChunk(chunk []byte, resultsChan chan<- map[string]*stats, wg *sync.WaitGroup) {
	defer wg.Done()
	stationStats := make(map[string]*stats)
	var word []byte
	var numw []byte
	k := 0
	for i := 0; i < len(chunk); i++ {
		c := chunk[i]
		if c == '\n' {
			k = 0
			if len(word) > 0 && len(numw) > 0 {
				num, err := strconv.ParseFloat(string(numw), 64)
				if err != nil {
					panic(err)
				}
				s := stationStats[string(word)]
				if s == nil {
					stationStats[string(word)] = &stats{
						max:   num,
						min:   num,
						count: 1,
						sum:   num,
					}
				} else {
					s.min = min(s.min, num)
					s.max = max(s.max, num)
					s.sum += num
					s.count++
				}

				word = word[:0] // reset word buffer
				numw = numw[:0] // reset numw buffer
			} else {
				continue
			}
		} else if c == ';' {
			k = 1
		} else {
			if k == 0 {
				word = append(word, c)
			} else {
				numw = append(numw, c)
			}
		}
	}

	// Send the computed stats to resultsChan
	resultsChan <- stationStats
}

func printResult(statsMap map[string]*stats) {

	f, err := os.OpenFile("out.txt", os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	output := bufio.NewWriter(f)

	var stations []string
	for station := range statsMap {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := statsMap[station]
		mean := s.sum / float64(s.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f\n", station, s.min, mean, s.max)
	}
	fmt.Fprint(output, "}\n")

	output.Flush()
}

func main() {

	numWorkers = runtime.NumCPU()
	runtime.GOMAXPROCS(numWorkers)

	file, err := os.Open("../file.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	resultsChan := make(chan map[string]*stats, numWorkers)
	var wg sync.WaitGroup
	var aggWg sync.WaitGroup

	aggWg.Add(1)
	finalResults := make(map[string]*stats)

	start := time.Now()

	// Start a separate goroutine for aggregation
	go func() {
		defer aggWg.Done()
		for workerResult := range resultsChan {
			for station, stats := range workerResult {
				finalStats, ok := finalResults[station]
				if !ok {
					finalResults[station] = stats
					continue
				}
				finalStats.min = min(finalStats.min, stats.min)
				finalStats.max = max(finalStats.max, stats.max)
				finalStats.sum += stats.sum
				finalStats.count += stats.count
				finalResults[station] = finalStats
			}
		}
	}()

	buf := make([]byte, 1024*1024)
	readStart := 0

	go func() {
		for {
			n, err := file.Read(buf[readStart:])
			if err != nil && err != io.EOF {
				panic(err)
			}
			if readStart+n == 0 {
				break
			}
			// Copy the chunk to a new slice, because the
			// buffer will be reused in the next iteration.
			chunk := make([]byte, len(buf[:readStart+n]))
			copy(chunk, buf[:readStart+n])
			newline := bytes.LastIndexByte(chunk, '\n')
			if newline < 0 {
				break
			}
			remaining := chunk[newline+1:]
			chunk = chunk[:newline+1]

			// Spin up goroutine for every chunk
			wg.Add(1)
			go processChunk(chunk, resultsChan, &wg)

			readStart = copy(buf, remaining)
		}
		wg.Wait()
		close(resultsChan)
	}()

	aggWg.Wait()

	// Print results
	printResult(finalResults)

	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "Processed in %s\n", elapsed)

}
