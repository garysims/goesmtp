package main

import (
"fmt"
"os"
"encoding/binary"
"io"
)

//
// MessageCounter files have fixed lengths, 1 x uint64 for nodeid and then 1x uint64 for counter
//

// Create a new counter file with space for 256 nodes
func createMessageCounterFile(name string) bool {
	var i uint64
	var z uint64
	
	fd, err := os.Open(name, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
	if(err == nil) {
		z = 0
		ior := io.Writer(fd)
		for i=0;i<256;i++ {
			if err := binary.Write(ior, binary.LittleEndian, z); err != nil {
				fmt.Printf("binary.Write failed: %s\n", err);
			}			
		}
		fd.Close()
		return true
	} else {
		// Error creating file, BIG problem!
		fmt.Printf("Can't create %s... FATAL.\n", name)
		return false
	}

	return false
}

func cantOpenMessageCounterFile(name string) bool {
	// Error opening file, does it exist?
	stat, _ := os.Stat(name)
	if(stat == nil) {
		// File doesn't exist
		fmt.Printf("%s doesn't exist... Create one...\n", name)
		return createMessageCounterFile(name)
	} else {
		fmt.Printf("Can't open %s, but it does exist... FATAL.\n", name)
		return false
	}
	
	return false
}

func getCounter(name string, node int64) uint64 {
	var counter uint64
	var offset int64
	
	fd, err := os.Open(name, os.O_RDONLY, 0666)
	if(err == nil) {
		defer fd.Close()
		// node 1 is the first node
		offset = (node - 1) * 8
		_, seekerr := fd.Seek(offset, 0)
		if(seekerr == nil) {
			ior := io.Reader(fd)
			if err := binary.Read(ior, binary.LittleEndian, &counter); err != nil {
				fmt.Printf("binary.Read failed: %s\n", err);
			} else {
				fd.Close()
				return counter
			}
		} else {
			fmt.Printf("Seek error for node %d to offset %d\n", node, offset)
			fd.Close()
		}
	} else {
		cantOpenMessageCounterFile(name)
	}
	
	return 0
}

func setCounter(name string, node int64, counter uint64) uint64 {
	var offset int64
	
	fd, err := os.Open(name, os.O_WRONLY, 0666)
	if(err == nil) {
		defer fd.Close()
		// node 1 is the first node
		offset = (node - 1) * 8
		_, seekerr := fd.Seek(offset, 0)
		if(seekerr == nil) {
			ior := io.Writer(fd)
			if err := binary.Write(ior, binary.LittleEndian, counter); err != nil {
				fmt.Printf("binary.Write failed: %s\n", err);
			} else {
				fd.Close()
				return counter
			}
		} else {
			fmt.Printf("Seek error for node %d to offset %d\n", node, offset)
			fd.Close()
		}
	} else {
		if(cantOpenMessageCounterFile(name) == true) {
			setCounter(name, node, counter)
		}
	}
	
	return 0
}
