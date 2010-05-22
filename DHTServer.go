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
"time"
"os"
"fmt"
"mysql"
"net"
"bufio"
"regexp"
"bytes"
"strings"
"sync"
"container/list"
"strconv"
)

type nodesInClusterStruct struct {
	ip string
	nodeid string
	lastPing int64
}

type DHTServerStruct struct {
	logger *LogStruct
	DBusername string
	DBpassword string
	DBhost string
	DBdatabase string
	db *mysql.MySQL
	bigLock sync.Mutex
}

var G_nodes list.List
var G_nodesLock sync.Mutex

func (myDHTServer *DHTServerStruct) processRemoteNewMessageLog() {

	numberofrows := 0
	rowsreceived := 0
	
	// Go through list of remote nodes in cluster and process
	// their new message log
	G_nodesLock.Lock()
 	for c := range G_nodes.Iter() {
 		// Don't connect to ourselves!
		if(c.(*nodesInClusterStruct).ip != G_IPAddress) {
		
			m := fmt.Sprintf("%s:4322", c.(*nodesInClusterStruct).ip)
			con, errdial := net.Dial("tcp", "", m)
			
			if(errdial != nil) {
				myDHTServer.logger.Logf(LMAX, "Can't connect to: %s (processRemoteNewMessageLog)", m)
				G_nodesLock.Unlock()
				return	
			}
	
			// Get greeting
			buf := bufio.NewReader(con);
			lineofbytes, err := buf.ReadBytes('\n');
			if err != nil {
				myDHTServer.logger.Log(LMIN, "Network connection unexpected closed while in processRemoteNewMessageLog.")			
				G_nodesLock.Unlock()
				return
			}
			
			hid := myDHTServer.getHighestIDForNodeFromDHT(c.(*nodesInClusterStruct).nodeid)
			con.Write([]byte(fmt.Sprintf("NEWMSGS %d\r\n", hid)))
		
			// Process reply	
			// First line of reply is number of results
			lineofbytes, err = buf.ReadBytes('\n');
			if err != nil {
				con.Close()
				myDHTServer.logger.Log(LMIN, "Network connection unexpectly closed while receiving results.")			
				G_nodesLock.Unlock()
				return
			} else {
				lineofbytes = TrimCRLF(lineofbytes)
				numberofrows, err = strconv.Atoi(string(lineofbytes))
				if(err!=nil) {
					myDHTServer.logger.Log(LMIN, "Unexpected result during NEWMSGS.")	
					G_nodesLock.Unlock()
					return
				}
			}
	
			for {
				lineofbytes, err = buf.ReadBytes('\n');
				if err != nil {
					con.Close()
					break
				} else {
					lineofbytes = TrimCRLF(lineofbytes)
					fields := strings.Split(string(lineofbytes), ",", 0)
		
					if(len(fields) != 5) {
						// Not enough fields, just list nodes in cluster and exit
						myDHTServer.logger.Log(LMIN, "Unexpected result (not enough fields) during processRemoteNewMessageLog.")					
						return
					}
					sql := fmt.Sprintf("INSERT INTO DHT (id, sha1, mailbox, cached, size, orignodeid) VALUES ('%s', '%s', '%s', NULL, '%s', '%s')", fields[0], fields[1], fields[2], fields[3], fields[4])
					myDHTServer.logger.Logf(LMED, "New message added from remote server %s for %s (%s/%s)", fields[4], fields[2], fields[0], fields[1])
					myDHTServer.logger.Logf(LMAX, "processRemoteNewMessageLog SQL: %s", sql)

					myDHTServer.db.Query(sql)
					if myDHTServer.db.Errno != 0 {
							fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
					}
					
					// Update the counters	
					iid, ierr := strconv.Atoi64(fields[0])
					if(ierr!=nil) {
						myDHTServer.logger.Log(LMIN, "Unexpected Atoi conversion")	
						return
					}

					myDHTServer.updateNewMessageCounter(iid, fields[4])					
					rowsreceived++
				}
			}
			if(rowsreceived != numberofrows) {
				myDHTServer.logger.Logf(LMIN, "processRemoteNewMessageLog - Not enough results: wanted=%d got=%d", numberofrows, rowsreceived)
			}

		}
	}
	G_nodesLock.Unlock()
}

func (myDHTServer *DHTServerStruct) processNewMessageLog() int{

	//
	// LOCAL
	//

	hid := myDHTServer.getHighestIDForNodeFromDHT(G_nodeID)
//	myDHTServer.logger.Logf(LMAX, "Highest ID for Node %s is %d", G_nodeID, hid)

	// Query newMessageLog
	sql := fmt.Sprintf("SELECT * FROM newMessageLog WHERE id > %d order by id LIMIT 100", hid)
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		os.Exit(1)
	}
    
    // Process results
    count := 0
    var row map[string] interface{}
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		sql := fmt.Sprintf("INSERT INTO DHT (id, sha1, mailbox, cached, size, orignodeid) VALUES ('%d', '%s', '%s', NULL, '%d', '%s')", row["id"], row["sha1"], row["mailbox"], row["size"], G_nodeID)
		myDHTServer.logger.Logf(LMAX, "processNewMessageLog SQL: %s", sql)

		myDHTServer.db.Query(sql)
		if myDHTServer.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		}
		
		// Update the counters	
		myDHTServer.updateNewMessageCounter(row["id"].(int64), G_nodeID)
		count += 1
	}

	return count
}

func (myDHTServer *DHTServerStruct) updateNewMessageCounter(id int64, nodeid string) {
		sql := fmt.Sprintf("INSERT INTO newMessageCounter (id, nodeid) VALUES ('%d', '%s')", id, nodeid)
		myDHTServer.logger.Logf(LMAX, "updateNewMessageCounter SQL: %s", sql)

		myDHTServer.db.Query(sql)
		if myDHTServer.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		}

		sql = fmt.Sprintf("DELETE FROM newMessageCounter WHERE ID < '%d' and nodeid = '%s'", id, nodeid)
		myDHTServer.logger.Logf(LMAX, "updateNewMessageCounter SQL: %s", sql)

		myDHTServer.db.Query(sql)
		if myDHTServer.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		}
}

func (myDHTServer *DHTServerStruct) getHighestIDForNodeFromDHT(node string) int64 {
	sql := fmt.Sprintf("SELECT max(id) FROM newMessageCounter WHERE nodeid = %s", node)
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		return -1
	} else {
		// Results
		var row map[string] interface { }
		for {
			row = res.FetchMap()
			if row == nil {
				break
			}
			
			return  row["max(id)"].(int64)
		}
	}
	
	return 1
}


func (myDHTServer *DHTServerStruct) updateDelMessageCounter(id int64, nodeid string) {
		sql := fmt.Sprintf("INSERT INTO delMessageCounter (id, nodeid) VALUES ('%d', '%s')", id, nodeid)
		myDHTServer.logger.Logf(LMAX, "updateDelMessageCounter SQL: %s", sql)

		myDHTServer.db.Query(sql)
		if myDHTServer.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		}

		sql = fmt.Sprintf("DELETE FROM delMessageCounter WHERE ID < '%d' and nodeid = '%s'", id, nodeid)
		myDHTServer.logger.Logf(LMAX, "updateDelMessageCounter SQL: %s", sql)

		myDHTServer.db.Query(sql)
		if myDHTServer.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		}
}

func (myDHTServer *DHTServerStruct) getHighestIDFordelMessageLog(node string) int64 {
	sql := fmt.Sprintf("SELECT max(id) FROM delMessageCounter WHERE nodeid = %s", node)
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		return -1
	} else {
		// Results
		var row map[string] interface { }
		for {
			row = res.FetchMap()
			if row == nil {
				break
			}
			
			return  row["max(id)"].(int64)
		}
	}
	
	return 1
}

func (myDHTServer *DHTServerStruct) processRemoteDelMessageLog() {

	numberofrows := 0
	rowsreceived := 0
	
	// Go through list of remote nodes in cluster and process
	// their del message log
	G_nodesLock.Lock()
 	for c := range G_nodes.Iter() {
 		// Don't connect to ourselves!
		if(c.(*nodesInClusterStruct).ip != G_IPAddress) {
		
			m := fmt.Sprintf("%s:4322", c.(*nodesInClusterStruct).ip)
			con, errdial := net.Dial("tcp", "", m)
			
			if(errdial != nil) {
				myDHTServer.logger.Logf(LMAX, "Can't connect to: %s (processRemoteDelMessageLog)", m)
				G_nodesLock.Unlock()
				return	
			}
	
			// Get greeting
			buf := bufio.NewReader(con);
			lineofbytes, err := buf.ReadBytes('\n');
			if err != nil {
				myDHTServer.logger.Log(LMIN, "Network connection unexpected closed while in processRemoteNewMessageLog.")			
				G_nodesLock.Unlock()
				return
			}
			
			hid := myDHTServer.getHighestIDFordelMessageLog(c.(*nodesInClusterStruct).nodeid)
			con.Write([]byte(fmt.Sprintf("DELMSGS %d\r\n", hid)))
		
			// Process reply		
			// First line of reply is number of results
			lineofbytes, err = buf.ReadBytes('\n');
			if err != nil {
				con.Close()
				myDHTServer.logger.Log(LMIN, "Network connection unexpectly closed while receiving results.")			
				G_nodesLock.Unlock()
				return
			} else {
				lineofbytes = TrimCRLF(lineofbytes)
				numberofrows, err = strconv.Atoi(string(lineofbytes))
				if(err!=nil) {
					myDHTServer.logger.Log(LMIN, "Unexpected result during DELMSGS.")	
					G_nodesLock.Unlock()
					return
				}
			}
	
			for {
				lineofbytes, err = buf.ReadBytes('\n');
				if err != nil {
					con.Close()
					break
				} else {
					lineofbytes = TrimCRLF(lineofbytes)
					fields := strings.Split(string(lineofbytes), ",", 0)
		
					if(len(fields) != 4) {
						// Not enough fields, just list nodes in cluster and exit
						myDHTServer.logger.Log(LMIN, "Unexpected result (not enough fields) during processRemoteDelMessageLog.")					
						return
					}
					
					shastr := fields[1]
					sql := fmt.Sprintf("DELETE FROM DHT WHERE sha1 = '%s' LIMIT 1", shastr)
					myDHTServer.logger.Logf(LMAX, "processRemoteDelMessageLog SQL: %s", sql)
	
					myDHTServer.db.Query(sql)
					if myDHTServer.db.Errno != 0 {
						fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
					}
			
					// Delete the from the message store
					// Just try to delete the file as if we have a copy (original or cached) it needs to
					// be deleted
			
					path := fmt.Sprintf("%s/%c/%c/%c", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37])		
					dele822 := fmt.Sprintf("%s/%s.822", path, shastr)
					myDHTServer.logger.Logf(LMAX, "Delete from message store: %s", dele822)
					os.Remove(dele822)
					
					// Update the counters	
					iid, ierr := strconv.Atoi64(fields[0])
					if(ierr!=nil) {
						myDHTServer.logger.Log(LMIN, "Unexpected Atoi conversion")	
						return
					}

					myDHTServer.updateDelMessageCounter(iid, fields[3])					
					rowsreceived++
				}
			}
			if(rowsreceived != numberofrows) {
				myDHTServer.logger.Logf(LMIN, "processRemoteDelMessageLog - Not enough results: wanted=%d got=%d", numberofrows, rowsreceived)
			}

		}
	}
	G_nodesLock.Unlock()
}

func (myDHTServer *DHTServerStruct) processDelMessageLog() int{

	//
	// LOCAL
	//
	hid := myDHTServer.getHighestIDFordelMessageLog(G_nodeID)
	
	// Query delMessageLog
	sql := fmt.Sprintf("SELECT * FROM delMessageLog WHERE id > %d order by id LIMIT 100", hid)
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		return 0
	}
    
    // Process results
    count := 0
    var row map[string] interface{}
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		shastr := row["sha1"].(string)
		sql := fmt.Sprintf("DELETE FROM DHT WHERE sha1 = '%s' LIMIT 1", shastr)
		myDHTServer.logger.Logf(LMAX, "processDelMessageLog SQL: %s", sql)

		myDHTServer.db.Query(sql)
		if myDHTServer.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		}
		
		// Update the counters	
		myDHTServer.updateDelMessageCounter(row["id"].(int64), G_nodeID)
		
		// Delete the from the message store
		// Just try to delete the file as if we have a copy (original or cached) it needs to
		// be deleted
		
		path := fmt.Sprintf("%s/%c/%c/%c", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37])		
		dele822 := fmt.Sprintf("%s/%s.822", path, shastr)
		myDHTServer.logger.Logf(LMAX, "Delete from message store: %s", dele822)
		os.Remove(dele822)
		
		count += 1
	}

	return count
}

func NewDHTServer() (myDHTServer *DHTServerStruct) {
	// Create and return a new instance of DHTServerStruct
	myDHTServer = new(DHTServerStruct)

	myDHTServer.logger = NewLogger("DHTServer ", G_LoggingLevel)
	
	myDHTServer.logger.Log(LMIN, "Starting...")
	
	c, err := ReadConfigFile("config.cfg");
	if(err==nil) {
		myDHTServer.DBusername, _ = c.GetString("db", "username");
		myDHTServer.DBpassword, _ = c.GetString("db", "password");
		myDHTServer.DBhost, _ = c.GetString("db", "host");
		myDHTServer.DBdatabase, _ = c.GetString("db", "database");
	}
	
	myDHTServer.connectToDB()
  	
	G_nodesLock.Lock()
 	n := new(nodesInClusterStruct)
 	n.ip = G_IPAddress
 	n.nodeid = G_nodeID
 	n.lastPing = time.Seconds()
	G_nodes.PushBack(n)
	G_nodesLock.Unlock()
	 	
 	return
}


func (myDHTServer *DHTServerStruct) connectToDB() {
	// Create new instance
	myDHTServer.db = mysql.New()
	// Enable/Disable logging
	myDHTServer.db.Logging = false
	// Connect to database
	myDHTServer.db.Connect(myDHTServer.DBhost, myDHTServer.DBusername, myDHTServer.DBpassword, myDHTServer.DBdatabase)
	if myDHTServer.db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
			os.Exit(1)
	}
	
	q := fmt.Sprintf("use %s;", myDHTServer.DBdatabase)
	myDHTServer.db.Query(q)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		os.Exit(1)
	}
	
}

// Send the New Message Log to a remote node
// First line of response is number of items
func (myDHTServer *DHTServerStruct) sendNewMessageLog(con *net.TCPConn, hid string) {

	// Query DHT
	sql := fmt.Sprintf("SELECT * FROM newMessageLog where id > %s order by id", hid)
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		os.Exit(1)
	}
    
    // Send results
    var row map[string] interface{}
    sz := fmt.Sprintf("%d\r\n", res.RowCount)
	con.Write([]byte(sz))    
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		reply := fmt.Sprintf("%d,%s,%s,%d,%s\r\n", row["id"], row["sha1"], row["mailbox"], row["size"], G_nodeID)
		con.Write([]byte(reply))
	}
}

// Send the deleted message log to a remote node
// First line of response is number of items
func (myDHTServer *DHTServerStruct) sendDelMessageLog(con *net.TCPConn, hid string) {

	// Query DHT
	sql := fmt.Sprintf("SELECT * FROM delMessageLog where id > %s order by id", hid)
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		os.Exit(1)
	}
    
    // Send results
    var row map[string] interface{}
    sz := fmt.Sprintf("%d\r\n", res.RowCount)
	con.Write([]byte(sz))    
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		reply := fmt.Sprintf("%d,%s,%s,%s\r\n", row["id"], row["sha1"], row["mailbox"], G_nodeID)
		con.Write([]byte(reply))
	}
}


// Dump the DHT
// First line of response is number of items
// It is important that the DHT is delivered in one go as
// otherwise there will be sync issues. Imagine you dump a bit now, then delete
// a message what was part of that dump and then dump some more, the delete would
// be lost.
// Will this work for 1,000,000 messages in the DHT?
func (myDHTServer *DHTServerStruct) dumpDHT(con *net.TCPConn) {

	// Query DHT
	sql := fmt.Sprintf("SELECT * FROM DHT order by id")
	res := myDHTServer.db.Query(sql)
	if myDHTServer.db.Errno != 0 {
		fmt.Printf("Error #%d %s\n", myDHTServer.db.Errno, myDHTServer.db.Error)
		os.Exit(1)
	}
    
    // Send results
    var row map[string] interface{}
    sz := fmt.Sprintf("%d\r\n", res.RowCount)
	con.Write([]byte(sz))    
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		reply := fmt.Sprintf("%d,%s,%s,%d,%d\r\n", row["id"], row["sha1"], row["mailbox"], row["size"], row["orignodeid"])
		con.Write([]byte(reply))
	}


}

//
// PING node nodeid hash
// PING 192.168.1.5 1 cf81a8580f8296424ee7589c3aca3b83981af958
//
func (myDHTServer *DHTServerStruct) newPing(con *net.TCPConn, s string, chall string) {
	
	fields := strings.Split(s, " ", 0)

	if(len(fields) != 4) {
		// Not enough fields just exit
		myDHTServer.logger.Logf(LMED, "Received ping with insufficient params")	
		return
	}
	
	// Authenticate
	sharesp := fields[3]	
	shap := SHA1String(fmt.Sprintf("%s%s", chall, G_clusterKey))
	shastr := fmt.Sprintf("%x", shap)
	if(shastr != sharesp) {
		myDHTServer.logger.Logf(LMIN, "Failed PING: %s %s", fields[1], fields[2])	
		return
	}

	if(compareConWithIPString(con, fields[1])==false) {
		myDHTServer.logger.Logf(LMIN, "Bad PING, RemoteAddr and IP parameter different: %s %s", con.RemoteAddr().String(), fields[1])
		return
	}
	
	// Does this IP address or node already exist?
	G_nodesLock.Lock()
 	for c := range G_nodes.Iter() {
		if((c.(*nodesInClusterStruct).ip == fields[1]) && (c.(*nodesInClusterStruct).nodeid == fields[2])) {
			// Already exits, great
			// Update time stamp
			c.(*nodesInClusterStruct).lastPing = time.Seconds()
			G_nodesLock.Unlock() // listCluster uses G_nodesLock
			myDHTServer.listCluster(con)
			return
		}
	}	
		
	// Need to check for IP in list with different nodeid and vice versa
	// TDB
 
 	n := new(nodesInClusterStruct)
 	n.ip = fields[1]
 	n.nodeid = fields[2]
 	n.lastPing = time.Seconds()
	G_nodes.PushBack(n)
	G_nodesLock.Unlock()

	myDHTServer.logger.Logf(LMED, "New node joins cluster %s %s", fields[1], fields[2])
	myDHTServer.listCluster(con)
}

func (myDHTServer *DHTServerStruct) listCluster(con *net.TCPConn) {
	var r string

	G_nodesLock.Lock()
	// First line of response is number of nodes in cluster
	con.Write([]byte(fmt.Sprintf("%d\r\n",G_nodes.Len())))


 	for c := range G_nodes.Iter() {
		r = fmt.Sprintf("%s %s\r\n", c.(*nodesInClusterStruct).ip, c.(*nodesInClusterStruct).nodeid)
		con.Write([]byte(r))
 	}
	G_nodesLock.Unlock()
 	
}

func (myDHTServer *DHTServerStruct) checkForPingTimeouts() {
	myDHTServer.bigLock.Lock()
	G_nodesLock.Lock()	
	for e := G_nodes.Front(); e != nil; e = e.Next() {
		if(e.Value.(*nodesInClusterStruct).nodeid != G_nodeID) {
			if(time.Seconds() > (e.Value.(*nodesInClusterStruct).lastPing + 120)) {
				myDHTServer.logger.Logf(LMED, "Not heard from %s %s recently, removing from cluster", e.Value.(*nodesInClusterStruct).ip, e.Value.(*nodesInClusterStruct).nodeid)
				G_nodes.Remove(e)
			}
		}	
	}
	G_nodesLock.Unlock()		
	myDHTServer.bigLock.Unlock()
}

func (myDHTServer *DHTServerStruct) pingMaster() {
	numberofrows := 0
	rowsreceived := 0
	
	m := fmt.Sprintf("%s:4322", G_masterNode)
	con, errdial := net.Dial("tcp", "", m)

	if(errdial != nil) {
		myDHTServer.logger.Logf(LMAX, "Can't ping the Master node: %s", m)
		return	
	}
	
	// Get greeting
	buf := bufio.NewReader(con);
	lineofbytes, err := buf.ReadBytes('\n');
	if err != nil {
		myDHTServer.logger.Log(LMIN, "Network connection unexpected closed while sending ping.")			
		return
	}
	
	// Greeting is in the form +OK <10038.1274507578@example.com>
	lineofbytes = TrimCRLF(lineofbytes)
	f := strings.Split(string(lineofbytes), " ", 0)
	if(len(f) != 2) {
		// Not enough fields in greeting message
		myDHTServer.logger.Log(LMIN, "Unexpected result (not enough fields) during ping.")					
		return
	}
	
	// The hash on the ping is the SHA1 hash of the greeting challenge eg text (e.g. <10038.1274507578@example.com> )
	// including the angle brackets and the shared secret (password)
	// e.g. <10038.1274507578@example.com>password
	respstr := fmt.Sprintf("%s%s", f[1], G_clusterKey)
	respsha := fmt.Sprintf("%x", SHA1String(respstr))
	con.Write([]byte(fmt.Sprintf("PING %s %s %s\r\n", G_IPAddress, G_nodeID, respsha)))
	
	// Reply is a list of nodes in the cluster	
	// First line of reply is number of results
	lineofbytes, err = buf.ReadBytes('\n');
	if err != nil {
		con.Close()
		myDHTServer.logger.Log(LMIN, "Network connection unexpected closed while receiving ping results.")			
		return
	} else {
		lineofbytes = TrimCRLF(lineofbytes)
		numberofrows, err = strconv.Atoi(string(lineofbytes))
		if(err!=nil) {
			myDHTServer.logger.Log(LMIN, "Unexpected result during ping.")	
			return
		}
	}

	G_nodesLock.Lock()
	G_nodes.Init()
	
	for {
		lineofbytes, err = buf.ReadBytes('\n');
		if err != nil {
			con.Close()
			break
		} else {
			lineofbytes = TrimCRLF(lineofbytes)
			fields := strings.Split(string(lineofbytes), " ", 0)

			if(len(fields) != 2) {
				// Not enough fields, just list nodes in cluster and exit
				myDHTServer.logger.Log(LMIN, "Unexpected result (not enough fields) during ping.")					
				G_nodesLock.Unlock()
				return
			}
			n := new(nodesInClusterStruct)
			n.ip = fields[0]
			n.nodeid = fields[1]
			n.lastPing = time.Seconds()
			G_nodes.PushBack(n)
			myDHTServer.logger.Logf(LCRAZY, "Other nodes in cluster: %s %s", n.ip, n.nodeid)
			rowsreceived++
		}
	}
	G_nodesLock.Unlock()

	if(rowsreceived != numberofrows) {
		myDHTServer.logger.Logf(LMIN, "pingMaster - Not enough results: wanted=%d got=%d", numberofrows, rowsreceived)
	}
}

func (myDHTServer *DHTServerStruct) retrieveMessage(con *net.TCPConn, sha string) {
	fn := filename822AndPathFromSHA(sha)
	
	body, errb := os.Open(fn, os.O_RDONLY, 0666)
		
	if (errb == nil) {
		buf := bufio.NewReader(body);
		con.Write([]byte("+OK message follows\r\n"))
		myDHTServer.logger.Logf(LMAX, "retrieveMessage - sending: %s", fn)

		for {
			lineofbytes, errl := buf.ReadBytes('\n');
			if errl != nil {
				body.Close()
				break
			} else {
				con.Write(lineofbytes)
			}
		}
	} else {
		myDHTServer.logger.Logf(LMIN, "retrieveMessage - Can't open file: %s", fn)
		con.Write([]byte("-ERR no such message\r\n"))
		return
	}
	con.Write([]byte(".\r\n"))
}

func (myDHTServer *DHTServerStruct) handleConnection(con *net.TCPConn) {

	newmsgsCmd, _ := regexp.Compile("^NEWMSGS ")
	delemsgsCmd, _ := regexp.Compile("^DELMSGS ")
	dumpCmd, _ := regexp.Compile("^DUMP")
	pingCmd, _ := regexp.Compile("^PING")
	retrCmd, _ := regexp.Compile("^RETR ")
	passwordsCmd, _ := regexp.Compile("^PASSWORDS")

	h, _ := os.Hostname()
	chall := fmt.Sprintf("<%d.%d@%s>",os.Getpid(), time.Seconds(), h)
	con.Write([]byte(fmt.Sprintf("+OK %s\r\n", chall)))


	buf := bufio.NewReader(con);
	lineofbytes, err := buf.ReadBytes('\n');
	if err != nil {
		print("Client disconnected rudely\n");
		con.Close()
		return
	} else {
		lineofbytes = TrimCRLF(lineofbytes)
		lineofbytesU := bytes.ToUpper(lineofbytes)
		myDHTServer.logger.Logf(LCRAZY, "C: %s", string(lineofbytes))
		
		if len(lineofbytes) > 0 {
			switch {
				case newmsgsCmd.Match(lineofbytesU):
					if(isNodeAuthenticated(con)==false) { goto FINISHED }
					// List the new messages starting from ID provided
					ll := len(lineofbytes)
					lineofbytes = TrimCRLF(lineofbytes)
					myDHTServer.sendNewMessageLog(con, string(lineofbytes[8:ll]))
					break;
				case delemsgsCmd.Match(lineofbytesU):
					if(isNodeAuthenticated(con)==false) { goto FINISHED }
					// List the deletyed messages starting from ID provided
					ll := len(lineofbytes)
					lineofbytes = TrimCRLF(lineofbytes)
					myDHTServer.sendDelMessageLog(con, string(lineofbytes[8:ll]))
					break;
				case dumpCmd.Match(lineofbytesU):
					// Dump the DHT
					myDHTServer.dumpDHT(con)
					break;
				case pingCmd.Match(lineofbytesU):
					// New node joins cluster
					if(G_nodeType == "master") {
						myDHTServer.newPing(con, string(lineofbytes), chall)
					}
					break;
				case retrCmd.Match(lineofbytesU):
					if(isNodeAuthenticated(con)==false) { goto FINISHED }
						f := strings.Split(string(lineofbytes), " ", 0)					
						if((len(f)==2) && (len(f[1])==40)) {
							myDHTServer.retrieveMessage(con, f[1])
						}
					break;
				case passwordsCmd.Match(lineofbytesU):
					if(isNodeAuthenticated(con)==false) { goto FINISHED }
					dumpPasswordsToConn(con)
					break;
			}
		}
	}
FINISHED:
	con.Close();
}

func (myDHTServer *DHTServerStruct) startDHTServer() {
	for
	{
		tcpAddress, _ := net.ResolveTCPAddr(":4322")
		
		listener, _ := net.ListenTCP("tcp", tcpAddress)
		
		for
		{
			con, _ := listener.AcceptTCP()
		
			go myDHTServer.handleConnection(con)			
		}				
	}
}

func (myDHTServer *DHTServerStruct) startDHTProcesses() {
	i := 12
	
	go myDHTServer.startDHTServer()
	
	for
	{
		if(i==12) {
			if(G_nodeType == "master") {
				myDHTServer.checkForPingTimeouts()
			} else {
				myDHTServer.pingMaster()		
			}
			i = 0
		} else {
			i = i + 1
		}

		// In nano seconds, 1 second = 1 000 000 000 nanoseconds
		time.Sleep(5000000000)

		// New messages
		for myDHTServer.processNewMessageLog() > 0 {
		}
		myDHTServer.processRemoteNewMessageLog()
		
		// Deleted messages
		for myDHTServer.processDelMessageLog() > 0 {
		}
		myDHTServer.processRemoteDelMessageLog()
	}
}
