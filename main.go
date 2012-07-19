/*
   go-osmpbf-filter; filtering software for OpenStreetMap PBF files.
   Copyright (C) 2012  Mathieu Fenniak

   This program is free software: you can redistribute it and/or modify
   it under the terms of the GNU General Public License as published by
   the Free Software Foundation, either version 3 of the License, or
   (at your option) any later version.

   This program is distributed in the hope that it will be useful,
   but WITHOUT ANY WARRANTY; without even the implied warranty of
   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
   GNU General Public License for more details.

   You should have received a copy of the GNU General Public License
   along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

package main

import (
	"OSMPBF"
	"bufio"
	"code.google.com/p/goprotobuf/proto"
	"flag"
	"fmt"
	"io"
	"os"
	"runtime"
	"strings"
)

type boundingBoxUpdate struct {
	wayIndex int
	lon      float64
	lat      float64
}

type node struct {
	id  int64
	lon float64
	lat float64
}

type way struct {
	id      int64
	nodeIds []int64
	keys    []string
	values  []string
}

type myway struct {
	id      int64
	nodeIds []int64
	highway string
}

func supportedFilePass(file *os.File) {
	for data := range MakePrimitiveBlockReader(file) {
		if *data.blobHeader.Type != "OSMHeader" {
			continue
		}
		// processing OSMHeader
		blockBytes, err := DecodeBlob(data)
		if err != nil {
			println("OSMHeader blob read error:", err.Error())
			os.Exit(5)
		}

		header := &OSMPBF.HeaderBlock{}
		err = proto.Unmarshal(blockBytes, header)
		if err != nil {
			println("OSMHeader decode error:", err.Error())
			os.Exit(5)
		}

		for _, feat := range header.RequiredFeatures {
			if feat != "OsmSchema-V0.6" && feat != "DenseNodes" {
				println("Unsupported feature required in OSM header:", feat)
				os.Exit(5)
			}
		}
	}
}

func findMatchingWaysPass(file *os.File, filterTag string, totalBlobCount int, output *bufio.Writer) [][]int64 {
	wayNodeRefs := make([][]int64, 0, 100)
	pending := make(chan bool)

	appendNodeRefs := make(chan []int64)
	appendNodeRefsComplete := make(chan bool)

	go func() {
		for nodeRefs := range appendNodeRefs {
			wayNodeRefs = append(wayNodeRefs, nodeRefs)
		}
		appendNodeRefsComplete <- true
	}()
	wayqueue := make(chan *myway)
	exitqueue := make(chan bool)
	done := make(chan bool)
	wCount := runtime.NumCPU() * 2

	blockDataReader := MakePrimitiveBlockReader(file)
	for i := 0; i < wCount; i++ {
		go func() {
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for _, primitiveGroup := range primitiveBlock.Primitivegroup {
						for _, way := range primitiveGroup.Ways {
							for i, keyIndex := range way.Keys {
								valueIndex := way.Vals[i]
								key := string(primitiveBlock.Stringtable.S[keyIndex])
								value := string(primitiveBlock.Stringtable.S[valueIndex])
								if key == filterTag {
									var nodeRefs = make([]int64, len(way.Refs))
									var prevNodeId int64 = 0
									for index, deltaNodeId := range way.Refs {
										nodeId := prevNodeId + deltaNodeId
										prevNodeId = nodeId
										nodeRefs[index] = nodeId
									}
									wayqueue <- &myway{*way.Id, nodeRefs, value}

									appendNodeRefs <- nodeRefs
								}
							}
						}
					}
				}
				pending <- true
			}
			exitqueue <- true
		}()
	}

	go func() {
		j := 0
		for i := 0; true; i++ {
			select {
			case way := <-wayqueue:
				csv := make([]string, len(way.nodeIds))
				for i, v := range way.nodeIds {
					csv[i] = fmt.Sprintf("%d", v)
				}
				output.WriteString(fmt.Sprintf("%d,%s,%s\n", way.id, way.highway, strings.Join(csv, ",")))
				if i%1000 == 0 {
					output.Flush()
				}
			case <-exitqueue:
				j++
				output.Flush()
				fmt.Println("Work return, ", i, "nodes processed")
				if j == wCount {
					done <- true
					return
				}
			}
		}
	}()

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
			close(appendNodeRefs)
			<-appendNodeRefsComplete
			close(appendNodeRefsComplete)
		}
	}

	<-done
	return wayNodeRefs
}

func findMatchingNodesPass(file *os.File, wayNodeRefs [][]int64, totalBlobCount int, output *bufio.Writer) {
	// maps node ids to wayNodeRef indexes
	nodeOwners := make(map[int64]bool, len(wayNodeRefs)*3)
	for _, way := range wayNodeRefs {
		for _, nodeId := range way {
			if nodeOwners[nodeId] == false {
				nodeOwners[nodeId] = true
			}
		}
	}

	pending := make(chan bool)

	blockDataReader := MakePrimitiveBlockReader(file)

	nodequeue := make(chan *node)
	exitqueue := make(chan bool)
	done := make(chan bool)

	wCount := runtime.NumCPU() * 2
	for i := 0; i < wCount; i++ {
		go func() {
			var n node
			var lon, lat float64
			for data := range blockDataReader {
				if *data.blobHeader.Type == "OSMData" {
					blockBytes, err := DecodeBlob(data)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					primitiveBlock := &OSMPBF.PrimitiveBlock{}
					err = proto.Unmarshal(blockBytes, primitiveBlock)
					if err != nil {
						println("OSMData decode error:", err.Error())
						os.Exit(6)
					}

					for absNode := range MakeNodeReader(primitiveBlock) {
						owners := nodeOwners[absNode.GetNodeId()]
						if owners == false {
							continue
						}
						lon, lat = absNode.GetLonLat()
						n = node{absNode.GetNodeId(),
							lon,
							lat}
						nodequeue <- &n
					}
				}
				pending <- true
			}
			exitqueue <- true
		}()
	}

	go func() {
		j := 0
		for i := 0; true; i++ {
			select {
			case n := <-nodequeue:
				output.WriteString(fmt.Sprintf("%d,%f,%f\n", (*n).id, (*n).lat, (*n).lon))
				if i%1000 == 0 {
					output.Flush()
				}
			case <-exitqueue:
				j++
				output.Flush()
				fmt.Println("Work return, ", i, "nodes processed")
				if j == wCount {
					done <- true
					return
				}
			}
		}
	}()

	blobCount := 0
	for _ = range pending {
		blobCount += 1
		if blobCount%500 == 0 {
			println("\tComplete:", blobCount, "\tRemaining:", totalBlobCount-blobCount)
		}
		if blobCount == totalBlobCount {
			close(pending)
		}
	}
	<-done
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU() * 2)

	inputFile := flag.String("i", "input.pbf.osm", "input OSM PBF file")
	//outputFile := flag.String("o", "output.pbf.osm", "output OSM PBF file")
	highMemory := flag.Bool("high-memory", false, "use higher amounts of memory for higher performance")
	filterTag := flag.String("t", "highway", "tag to filter ways based upon")
	//filterValue := flag.String("v", "golf_course", "value to ensure that the way's tag is set to")
	flag.Parse()

	file, err := os.Open(*inputFile)
	if err != nil {
		println("Unable to open file:", err.Error())
		os.Exit(1)
	}

	// Count the total number of blobs; provides a nice progress indicator
	totalBlobCount := 0
	for {
		blobHeader, err := ReadNextBlobHeader(file)
		if err == io.EOF {
			break
		} else if err != nil {
			println("Blob header read error:", err.Error())
			os.Exit(2)
		}

		totalBlobCount += 1
		file.Seek(int64(*blobHeader.Datasize), os.SEEK_CUR)
	}
	println("Total number of blobs:", totalBlobCount)

	if *highMemory {
		cacheUncompressedBlobs = make(map[int64][]byte, totalBlobCount)
	}

	println("Pass 1/3: Find OSMHeaders")
	supportedFilePass(file)
	println("Pass 1/3: Complete")

	// saving ways in a csv file

	waysfile, err := os.OpenFile("ways.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Pass 2/3: Find node references of matching areas")
	wayNodeRefs := findMatchingWaysPass(file, *filterTag, totalBlobCount, bufio.NewWriter(waysfile))
	println("Pass 2/3: Complete;", len(wayNodeRefs), "matching ways found.")

	nodesfile, err := os.OpenFile("nodes.csv", os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0664)
	if err != nil {
		println("Output file write error:", err.Error())
		os.Exit(2)
	}

	println("Pass 3/3: Finding nodes")
	findMatchingNodesPass(file, wayNodeRefs, totalBlobCount, bufio.NewWriter(nodesfile))
	println("Pass 3/3: Complete; nodes recorded.")

}
