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

// Router

import (
"os"
"fmt"
"mysql"
"net"
"bufio"
"strings"
"strconv"
)


type DHTForceSyncStruct struct {
	logger *LogStruct
	DBusername string
	DBpassword string
	DBhost string
	DBdatabase string
	db *mysql.MySQL
}

func NewDHTForceSync() (myDHTForceSync *DHTForceSyncStruct) {

	// Create and return a new instance of DHTForceSyncStruct
    myDHTForceSync = new(DHTForceSyncStruct)

	myDHTForceSync.logger = NewLogger("DHT Force Sync ", G_LoggingLevel)
	
	myDHTForceSync.logger.Log(LMIN, "Starting...")
	
	c, err := ReadConfigFile("config.cfg");
	if(err==nil) {
		myDHTForceSync.DBusername, _ = c.GetString("db", "username");
		myDHTForceSync.DBpassword, _ = c.GetString("db", "password");
		myDHTForceSync.DBhost, _ = c.GetString("db", "host");
		myDHTForceSync.DBdatabase, _ = c.GetString("db", "database");
	}
	
	myDHTForceSync.connectToDB()
 
 	myDHTForceSync.forceSync()
 	
 	return
}

func (myDHTForceSync *DHTForceSyncStruct) connectToDB() {
	// Create new instance
	myDHTForceSync.db = mysql.New()
	// Enable/Disable logging
	myDHTForceSync.db.Logging = false
	// Connect to database
	myDHTForceSync.db.Connect(myDHTForceSync.DBhost, myDHTForceSync.DBusername, myDHTForceSync.DBpassword, myDHTForceSync.DBdatabase)
	if myDHTForceSync.db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", myDHTForceSync.db.Errno, myDHTForceSync.db.Error)
			os.Exit(1)
	}
	
	q := fmt.Sprintf("use %s;", myDHTForceSync.DBdatabase)
	myDHTForceSync.db.Query(q)
	if myDHTForceSync.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTForceSync.db.Errno, myDHTForceSync.db.Error)
		os.Exit(1)
	}
	
}

//
// At the moment the force sync happens with the master node... This should be 
// configurable later on...
//
func (myDHTForceSync *DHTForceSyncStruct) forceSync() {

	numberofrows := 0
	rowsreceived := 0
	
	// Empty the current DHT table
	myDHTForceSync.db.Query("TRUNCATE TABLE DHT;")
	if myDHTForceSync.db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", myDHTForceSync.db.Errno, myDHTForceSync.db.Error)
	}
	
	
	m := fmt.Sprintf("%s:4322", G_masterNode)
	con, errdial := net.Dial("tcp", "", m)
	if(errdial == nil) {
	
		// Get greeting
		buf := bufio.NewReader(con);
		lineofbytes, err := buf.ReadBytes('\n');
		if err != nil {
			myDHTForceSync.logger.Log(LMIN, "Network connection unexpected closed.")			
			return
		}
	
		con.Write([]byte("DUMP\r\n"))
	
		// First line of reply is number of results
		lineofbytes, err = buf.ReadBytes('\n');
		if err != nil {
			con.Close()
			myDHTForceSync.logger.Log(LMIN, "Network connection unexpected closed.")			
			return
		} else {
			lineofbytes = TrimCRLF(lineofbytes)
			numberofrows, err = strconv.Atoi(string(lineofbytes))
			if(err!=nil) {
				myDHTForceSync.logger.Log(LMIN, "Unexpected result during force sync.")	
				return
			}
		}

		var sql string
		
		for {
			lineofbytes, err = buf.ReadBytes('\n');
			if err != nil {
				con.Close()
				break
			} else {
				lineofbytes = TrimCRLF(lineofbytes)
fmt.Printf("forceSync: %s\n", string(lineofbytes))
				fields := strings.Split(string(lineofbytes), ",", 0)
				
				sql = fmt.Sprintf("INSERT INTO DHT (id, sha1, mailbox, cached, size, orignodeid) VALUES ('%s', '%s', '%s', NULL, '%s', '%s')", fields[0], fields[1], fields[2], fields[3], fields[4])
				myDHTForceSync.logger.Logf(LMAX, "forceSync SQL: %s", sql)

				myDHTForceSync.db.Query(sql)
				if myDHTForceSync.db.Errno != 0 {
						fmt.Printf("Error #%d %s\n", myDHTForceSync.db.Errno, myDHTForceSync.db.Error)
				}
				rowsreceived++
			}
		}
		if(rowsreceived != numberofrows) {
			myDHTForceSync.logger.Logf(LMIN, "forceSync - Not enough results: wanted=%d got=%d", numberofrows, rowsreceived)
		}

	} else {
		fmt.Printf("Can't connect to master node %s. FATAL.", m)
		os.Exit(-1)
	}
}
