package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

func r1(inputPath string, output io.Writer) error {
	type stats struct {
		min, max, sum float64
		count         int64
	}

	f, err := os.Open(inputPath)
	if err != nil {
		return err
	}
	defer f.Close()

	stationStats := make(map[string]stats)

	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		station, tempStr, hasSemi := strings.Cut(line, ";")
		if !hasSemi {
			continue
		}

		num, err := strconv.ParseFloat(tempStr, 64)
		if err != nil {
			return err
		}

		s, ok := stationStats[station]
		if !ok {
			s.max = num
			s.min = num
			s.count += 1
			s.sum += num
		} else {
			s.min = min(s.min, num)
			s.max = max(s.max, num)
			s.sum += num
			s.count += 1
		}
		stationStats[station] = s
	}

	stations := make([]string, 0, len(stationStats))
	for station := range stationStats {
		stations = append(stations, station)
	}
	sort.Strings(stations)

	fmt.Fprint(output, "{")
	for i, station := range stations {
		if i > 0 {
			fmt.Fprint(output, ", ")
		}
		s := stationStats[station]
		mean := s.sum / float64(s.count)
		fmt.Fprintf(output, "%s=%.1f/%.1f/%.1f\n", station, s.min, mean, s.max)
	}
	fmt.Fprint(output, "}\n")
	return nil
}

func main() {
	start := time.Now()
	f, err := os.OpenFile("out.txt", os.O_RDWR|os.O_APPEND, 0644)
	if err != nil {
		panic(err)
	}
	defer f.Close()
	output := bufio.NewWriter(f)

	err = r1("../file.txt", output)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	output.Flush()
	elapsed := time.Since(start)
	fmt.Fprintf(os.Stderr, "Processed in %s\n", elapsed)

}
