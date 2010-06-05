// 		Copyright 2010 Gary Sims. All rights reserved.
// 		http://www.garysims.co.uk
//
//    	This file is part of GoESMTP.
//		http://code.google.com/p/goesmtp/
//		http://goesmtp.posterous.com/
//
//    	GoESMTP is free software: you can redistribute it and/or modify
//    	it under the terms of the GNU General Public License as published by
//    	the Free Software Foundation, either version 2 of the License, or
//    	(at your option) any later version.
//
//    	GoESMTP is distributed in the hope that it will be useful,
//   	but WITHOUT ANY WARRANTY; without even the implied warranty of
//   	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
//    	GNU General Public License for more details.
//
//    	You should have received a copy of the GNU General Public License
//    	along with GoESMTP.  If not, see <http://www.gnu.org/licenses/>.

package main

import (
"net"
"fmt"
"os"
"bufio"
"strconv"
"sync"
)

var G_idServerLock sync.Mutex
var G_id uint64

func createNewIDFile() {
	fd, err := os.Open(IDFILE, os.O_WRONLY | os.O_TRUNC | os.O_CREATE, 0666)
	if(err == nil) {
		ids := fmt.Sprintf("1\n")
		fd.Write([]byte(ids))
		fd.Close()
	} else {
		// Error opening file, BIG problem!
			fmt.Println("Can't create id.txt... FATAL.")
			os.Exit(-3)		
	}
}

func updateIDFile(id uint64) {
	G_idServerLock.Lock()
	fd, err := os.Open(IDFILE, os.O_WRONLY | os.O_TRUNC, 0666)
	if(err == nil) {
		ids := fmt.Sprintf("%d\n", id)
		fd.Write([]byte(ids))
		fd.Close()
	} else {
		// Error opening file, does it exist?
	}
	G_idServerLock.Unlock()	
}

func getIDFromIDFile() (uint64, os.Error) {
	G_idServerLock.Lock()
	defer G_idServerLock.Unlock()
	fd, err := os.Open(IDFILE, os.O_RDONLY, 0666)
	if(err == nil) {
		buf := bufio.NewReader(fd);
		lineofbytes, errb := buf.ReadBytes('\n');
		if(errb == nil) {
			lineofbytes = TrimCRLF(lineofbytes)
			fd.Close()
			return strconv.Atoui64(string(lineofbytes))
		} else {
			fmt.Println("Error creating bufio.NewReader(fd)")
		}
	} else {
		// Error reading file, does it exist?
		stat, _ := os.Stat(IDFILE)
		if(stat == nil) {
			// File doesn't exist
			fmt.Println("id.txt doesn't exist... Create one...")
			createNewIDFile()
		} else {
			fmt.Println("Can't open id.txt, but it does exist... FATAL.")
			os.Exit(-2)
		}
		return 0, nil
	}
	
	return 0, os.NewError("Impossible")
}

func handleIDServerReq (con *net.TCPConn) {

	if(isNodeAuthenticated(con)==false) { 
		con.Close()
		return
	}

	G_idServerLock.Lock()
	idstring := fmt.Sprintf("%d\r\n", G_id)
	G_idServerLock.Unlock()

	con.Write([]byte(idstring))
	con.Close()

	G_idServerLock.Lock()
	G_id += 1
	G_idServerLock.Unlock()

	updateIDFile(G_id)		
	
}

func startIDServer() {
	var err os.Error
	
	G_id, err = getIDFromIDFile()
	
	if(err!=nil) {
		fmt.Println("Error from getIDFromIDFile. FATAL.")
		os.Exit(-1)	
	}
	
	tcpAddress, _ := net.ResolveTCPAddr(":4321")
	
	listener, _ := net.ListenTCP("tcp", tcpAddress)
	
	for
	{
		con, _ := listener.AcceptTCP()
		go handleIDServerReq(con)
	}
	
	listener.Close()

}
