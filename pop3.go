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
"net"
"os"
"fmt"
"mysql"
"bufio"
"regexp"
"bytes"
"strings"
"strconv"
"time"
)


type POP3Struct struct {
	logger *LogStruct
	DBusername string
	DBpassword string
	DBhost string
	DBdatabase string
	db *mysql.MySQL
}

func NewPOP3() (myPOP3 *POP3Struct) {
	// Create and return a new instance of POP3Struct
	myPOP3 = new(POP3Struct)

	myPOP3.logger = NewLogger("POP3 ", G_LoggingLevel)
	
	myPOP3.logger.Log(LMIN, "Starting...")
	
	c, err := ReadConfigFile("config.cfg");
	if(err==nil) {
		myPOP3.DBusername, _ = c.GetString("db", "username");
		myPOP3.DBpassword, _ = c.GetString("db", "password");
		myPOP3.DBhost, _ = c.GetString("db", "host");
		myPOP3.DBdatabase, _ = c.GetString("db", "database");
	}
	
	myPOP3.connectToDB()
 
 	return
}


func (myPOP3 *POP3Struct) connectToDB() {
	// Create new instance
	myPOP3.db = mysql.New()
	// Enable/Disable logging
	myPOP3.db.Logging = false
	// Connect to database
	myPOP3.db.Connect(myPOP3.DBhost, myPOP3.DBusername, myPOP3.DBpassword, myPOP3.DBdatabase)
	if myPOP3.db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
			os.Exit(1)
	}
}

func (myPOP3 *POP3Struct) doListAll(con *net.TCPConn, user string) {

	// NOTE: Expensive as DB queried twice

	// +OK 2 messages (320 octets)
	nummsgs, szmsgs := myPOP3.getStat(user)				
	con.Write([]byte(fmt.Sprintf("+OK %d messages (%d octets)\r\n", nummsgs, szmsgs)))
	
	// Query DHT
	sql := fmt.Sprintf("SELECT id, size from DHT where mailbox='%s' order by id;", user)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		os.Exit(1)
	}

    // Process results
	var row map[string] interface{}
	i := 1
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		szmsg := row["size"].(int)
		con.Write([]byte(fmt.Sprintf("%d %d\r\n", i, szmsg)))				
		i = i + 1
	}
	con.Write([]byte(".\r\n"))	
}

func (myPOP3 *POP3Struct) doListN(con *net.TCPConn, user string, msgnumstr string) {
	
	msgnum, err := strconv.Atoi(msgnumstr)
	if(err != nil) {
		con.Write([]byte("-ERR eh?\r\n"))	
		return
	}
	
	// Query DHT
	sql := fmt.Sprintf("SELECT id, size from DHT where mailbox='%s' order by id limit %d, 1;", user, msgnum - 1)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		os.Exit(1)
	}

//	i := 1
    // Process results
	var row map[string] interface{}
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
//		if(i==msgnum) {
			szmsg := row["size"].(int)
			con.Write([]byte(fmt.Sprintf("+OK %d %d\r\n", msgnum, szmsg)))				
			return
//		}
//		i = i + 1
	}
	con.Write([]byte("-ERR no such message\r\n"))
}

func (myPOP3 *POP3Struct) doList(con *net.TCPConn, user string, msgnum string) {
	if(msgnum=="") {
		myPOP3.doListAll(con, user)
	} else {
		myPOP3.doListN(con, user, msgnum)
	}
}

func (myPOP3 *POP3Struct) doUIDLAll(con *net.TCPConn, user string) {

	con.Write([]byte("+OK\r\n"))
	
	// Query DHT
	sql := fmt.Sprintf("SELECT id, sha1 from DHT where mailbox='%s' order by id;", user)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		os.Exit(1)
	}

    // Process results
	var row map[string] interface{}
	i := 1
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		sha := row["sha1"].(string)
		con.Write([]byte(fmt.Sprintf("%d %s\r\n", i, sha)))				
		i = i + 1
	}
	con.Write([]byte(".\r\n"))
}

func (myPOP3 *POP3Struct) doUIDLN(con *net.TCPConn, user string, msgnumstr string) {
	
	msgnum, err := strconv.Atoi(msgnumstr)
	if(err != nil) {
		con.Write([]byte("-ERR eh?\r\n"))	
		return
	}
	
	// Query DHT
	sql := fmt.Sprintf("SELECT id, sha1 from DHT where mailbox='%s' order by id limit %d, 1;", user, msgnum - 1)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		os.Exit(1)
	}

//	i := 1
    // Process results
	var row map[string] interface{}
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
//		if(i==msgnum) {
			sha := row["sha1"].(string)
			con.Write([]byte(fmt.Sprintf("+OK %d %s\r\n", msgnum, sha)))				
			return
//		}
//		i = i + 1
	}
	con.Write([]byte("-ERR no such message\r\n"))
}

func (myPOP3 *POP3Struct) doUIDL(con *net.TCPConn, user string, msgnum string) {
	if(msgnum=="") {
		myPOP3.doUIDLAll(con, user)
	} else {
		myPOP3.doUIDLN(con, user, msgnum)
	}
}

func (myPOP3 *POP3Struct) getStat(user string) (int64, int64) {
	
	var nummsgs int64
	var szmsgs int64
	nummsgs = -1
	szmsgs = -1
	
	// Query DHT
	sql := fmt.Sprintf("SELECT sum(size), count(id) from DHT where mailbox='%s';", user)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		os.Exit(1)
	}

    // Process results
	var row map[string] interface{}
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		for key, value := range row {
			if(key=="sum(size)") {
				szmsgs, _ = strconv.Atoi64(value.(string))
			}
			if(key=="count(id)") {
				nummsgs = value.(int64)
			}
		}
	}

	if((nummsgs == -1) || (szmsgs == -1)) {
		return 0, 0
	} else {
		return nummsgs, szmsgs
	}
	
	return 0, 0
}

func (myPOP3 *POP3Struct) doRemoteRETR(con *net.TCPConn, sha1 string, orignodeid string) {
	// TDB: Check cache, is there a local copy?
	
	// Try the original node (the receiving node)
	ip := findNodeIPFromNodeID(orignodeid)
	if(ip=="") {
		// Node not up or in cluster at the moment
		con.Write([]byte("-ERR Message not available.\r\n"))
		return
	}
	
	m := fmt.Sprintf("%s:4322", ip)	
	dcon, errdial := net.Dial("tcp", "", m)
	
	if(errdial != nil) {
		myPOP3.logger.Log(LMAX, "Network connection unexpectedly closed.")			
		return	
	}

	// Get greeting
	buf := bufio.NewReader(dcon);
	lineofbytes, err := buf.ReadBytes('\n');
	if err != nil {
		myPOP3.logger.Log(LMAX, "Network connection unexpectedly closed.")			
		return
	}
	
	dcon.Write([]byte(fmt.Sprintf("RETR %s\r\n", sha1)))
	
	// Reply is like POP3 (+OK or -ERR)
	lineofbytes, err = buf.ReadBytes('\n');
	if err != nil {
		dcon.Close()
		myPOP3.logger.Log(LMIN, "Network connection unexpected closed while receiving results.")			
		return
	} else {
		lineofbytes = TrimCRLF(lineofbytes)
		myPOP3.logger.Logf(LMAX, "S: %s", string(lineofbytes))			
		if(lineofbytes[0]=='+') {
			con.Write([]byte("+OK message follows\r\n"))			
			// Read the rest of the message and send it back to the POP3 client			
			for {
				lineofbytes, err = buf.ReadBytes('\n');
				if err != nil {
					dcon.Close()
					break
				} else {
					con.Write(lineofbytes)
				}
			}
			// OK, message received and sent to POP3 client
			return
			
		} else {
			myPOP3.logger.Logf(LMED, "Received -ERR from other sever: %s", string(lineofbytes))					
		}
	}
					
	// TBD: If there is no local cache or the original node doesn't have it or is unavailable
	// try another node in the cluster that is replicating the messages
	myPOP3.logger.Log(LMED, "doRemoteRETR - Send -ERR to POP3 client")
	con.Write([]byte("-ERR Message not available.\r\n"))
}

func (myPOP3 *POP3Struct) doRETR(con *net.TCPConn, user string, msgnumstr string) {
	msgnum, err := strconv.Atoi(msgnumstr)
	if(err != nil) {
		con.Write([]byte("-ERR eh?\r\n"))	
		return
	}
	
	// Query DHT
	sql := fmt.Sprintf("SELECT id, sha1, orignodeid from DHT where mailbox='%s' order by id limit %d, 1;", user, msgnum - 1)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
	}

    // Process results
	var row map[string] interface{}
	for {
		row = res.FetchMap()
		if row == nil {
				break
		}
		orignodeid := fmt.Sprintf("%d", row["orignodeid"].(int))
		if(orignodeid != G_nodeID) {
			// Need to get the message from another server in the cluster
			myPOP3.logger.Logf(LMED, "Need to get the message from another server in the cluster: %s", orignodeid)
			myPOP3.doRemoteRETR(con, row["sha1"].(string), orignodeid)
			return
		} else {
			sha := row["sha1"].(string)

			fn := filename822AndPathFromSHA(sha)

			body, errb := os.Open(fn, os.O_RDONLY, 0666)
	
			if (errb == nil) {
				buf := bufio.NewReader(body);
				con.Write([]byte("+OK message follows\r\n"))				
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
				myPOP3.logger.Logf(LMIN, "doRETR - Can't open file: %s", fn)
			}
			con.Write([]byte(".\r\n"))
			return
		}
	}
	con.Write([]byte("-ERR no such message\r\n"))
}

func (myPOP3 *POP3Struct) doDELE(con *net.TCPConn, user string, msgnumstr string, msgsToDel []int) []int {
	msgnum, err := strconv.Atoi(msgnumstr)
	if(err != nil) {
		con.Write([]byte("-ERR eh?\r\n"))	
		return msgsToDel
	}
	
	n := len(msgsToDel)
	msgsToDel = msgsToDel[0 : n+1]
	msgsToDel[n] = msgnum

	con.Write([]byte("+OK message deleted\r\n"))	
	
	return msgsToDel
}

func (myPOP3 *POP3Struct) getSHAKeyForID(msgnum int, user string) string {
	// Query DHT
	sql := fmt.Sprintf("SELECT sha1 from DHT where mailbox='%s' order by id limit %d, 1;", user, msgnum - 1)
	res, mysqlerr := myPOP3.db.Query(sql)
	if mysqlerr != nil {
		fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		return ""
	}

	var row map[string] interface{}
	row = res.FetchMap()
	return row["sha1"].(string)
}

func (myPOP3 *POP3Struct) reallyDoDELE(msgsToDel []int, user string) {

	for i:= 0 ; i < len(msgsToDel) ; i++ {
		s := myPOP3.getSHAKeyForID(msgsToDel[i], user)
		id := getIDFromIDServer()
		sql := fmt.Sprintf("INSERT INTO delMessageLog (id, sha1, mailbox) VALUES ('%s', '%s', '%s')", id, s, user)
		myPOP3.logger.Logf(LMAX, "reallyDoDELE SQL: %s", sql)

		myPOP3.db.Query(sql)
		if myPOP3.db.Errno != 0 {
				fmt.Printf("Error #%d %s\n", myPOP3.db.Errno, myPOP3.db.Error)
		}		
	}

}

//
// Handle the POP3 connection
//
// NB: LIST and UIDL currently include messages marked for deletion
//
func (myPOP3 *POP3Struct) handleConnection(con *net.TCPConn) {
	var msgsToDel []int

	apopCmd, _ := regexp.Compile("^APOP")
	deleCmd, _ := regexp.Compile("^DELE")
	listCmd, _ := regexp.Compile("^LIST")
	noopCmd, _ := regexp.Compile("^NOOP")
	passCmd, _ := regexp.Compile("^PASS")
	quitCmd, _ := regexp.Compile("^QUIT")
	retrCmd, _ := regexp.Compile("^RETR")
	rsetCmd, _ := regexp.Compile("^RSET")
	statCmd, _ := regexp.Compile("^STAT")
	topCmd, _ := regexp.Compile("^TOP")
	uidlCmd, _ := regexp.Compile("^UIDL")
	userCmd, _ := regexp.Compile("^USER")

	msgsToDel = make([]int, 1, 8092)
	msgsToDel = msgsToDel[0:0]
	
	user := ""
	password := ""
	
	disconnected := false
	authenticated := false

	h, _ := os.Hostname()
	md5ts := fmt.Sprintf("<%d.%d@%s>",os.Getpid(), time.Seconds(), h)
	con.Write([]byte(fmt.Sprintf("+OK POP3 server ready %s\r\n", md5ts)))

	buf := bufio.NewReader(con);
	for
	{
		lineofbytes, err := buf.ReadBytes('\n');
		if err != nil {
			print("Client disconnected rudely\n");
			con.Close()
			disconnected = true;
			break
		} else {
			lineofbytes = TrimCRLF(lineofbytes)	
			lineofbytesU := bytes.ToUpper(lineofbytes)
			myPOP3.logger.Log(LMAX, string(lineofbytes))
			
			if len(lineofbytes) > 0 {
				 switch {
					case quitCmd.Match(lineofbytesU):
						if(authenticated==true) {
							myPOP3.reallyDoDELE(msgsToDel, user)
						}
						con.Write([]byte("+OK Bye for now\r\n"))
						con.Close();
						print("Client disconnected nicely\n");
						disconnected = true;
						break;
					case apopCmd.Match(lineofbytesU):
							f := strings.Split(string(lineofbytes), " ", 0)					
							if(len(f)!=3) {
								con.Write([]byte("-ERR eh?\r\n"))
							} else {
								user = f[1]
								password = f[2]
								ourpassword, _ := getPasswordInfo(user)
								md5p := MD5String(fmt.Sprintf("%s%s", md5ts, ourpassword))
								md5ps := fmt.Sprintf("%x", md5p)
								if(md5ps == password) {
									con.Write([]byte("+OK\r\n"))
									myPOP3.logger.Logf(LMIN, "Successful POP3 login from %s", user)
									authenticated = true
								} else {
									con.Write([]byte("-ERR\r\n"))
									authenticated = false
								}
							}
							break;
					case deleCmd.Match(lineofbytesU):
						if(authenticated==false) {
							con.Write([]byte("-ERR Authenticate first please\r\n"))
						} else {					
							f := strings.Split(string(lineofbytes), " ", 0)					
							if(len(f)!=2) {
								con.Write([]byte("-ERR eh?\r\n"))
							} else {
								msgsToDel = myPOP3.doDELE(con, user, f[1], msgsToDel)
							}
						}
						break;
					case listCmd.Match(lineofbytesU):
						if(authenticated==false) {
							con.Write([]byte("-ERR Authenticate first please\r\n"))
						} else {					
							f := strings.Split(string(lineofbytes), " ", 0)					
							if(len(f)>2) {
								con.Write([]byte("-ERR eh?\r\n"))
							} else {
								if(len(f)==1) {
									myPOP3.doList(con, user, "")
								} else {
									myPOP3.doList(con, user, f[1])
								}
							}
						}					
						break;
					case noopCmd.Match(lineofbytesU):
						con.Write([]byte("+OK\r\n"))
						break;
					case passCmd.Match(lineofbytesU):
						// What about password with spaces???
						f := strings.Split(string(lineofbytes), " ", 0)					
						if(len(f)!=2) {
							con.Write([]byte("-ERR eh?\r\n"))
						} else {
							password = f[1]
							ourpassword, _ := getPasswordInfo(user)
							if(password == ourpassword) {
								authenticated = true						
								con.Write([]byte("+OK\r\n"))
								myPOP3.logger.Logf(LMIN, "Successful POP3 login from %s", user)
							} else {
								con.Write([]byte("-ERR\r\n"))
								authenticated = false							
							}
						}
						break;
					case retrCmd.Match(lineofbytesU):
						if(authenticated==false) {
							con.Write([]byte("-ERR Authenticate first please\r\n"))
						} else {					
							f := strings.Split(string(lineofbytes), " ", 0)					
							if(len(f)!=2) {
								con.Write([]byte("-ERR eh?\r\n"))
							} else {
								myPOP3.doRETR(con, user, f[1])
							}
						}					
						break;
					case rsetCmd.Match(lineofbytesU):
						authenticated = false
						con.Write([]byte("+OK\r\n"))
						break;
					case statCmd.Match(lineofbytesU):
						if(authenticated==false) {
							con.Write([]byte("-ERR Authenticate first please\r\n"))
						} else {
							nummsgs, szmsgs := myPOP3.getStat(user)				
							con.Write([]byte(fmt.Sprintf("+OK %d %d\r\n", nummsgs, szmsgs)))
						}
						break;
					case topCmd.Match(lineofbytesU):
						con.Write([]byte("-ERR eh?\r\n"))
						break;
					case uidlCmd.Match(lineofbytesU):
						if(authenticated==false) {
							con.Write([]byte("-ERR Authenticate first please\r\n"))
						} else {					
							f := strings.Split(string(lineofbytes), " ", 0)					
							if(len(f)>2) {
								con.Write([]byte("-ERR eh?\r\n"))
							} else {
								if(len(f)==1) {
									myPOP3.doUIDL(con, user, "")
								} else {
									myPOP3.doUIDL(con, user, f[1])
								}
							}
						}					
						break;
					case userCmd.Match(lineofbytesU):
						f := strings.Split(string(lineofbytes), " ", 0)
						if(len(f)!=2) {
							con.Write([]byte("-ERR eh?\r\n"))
						} else {
							user = f[1]
							con.Write([]byte("+OK\r\n"))
						}
						break;
					default:
						con.Write([]byte("-ERR eh?\r\n"))
						break;
				}
			}
		}
		
		if disconnected == true {
			break;
		}
	}	
}

func (myPOP3 *POP3Struct) startPOP3() {
	for
	{
		tcpAddress, _ := net.ResolveTCPAddr(":110")
		
		listener, _ := net.ListenTCP("tcp", tcpAddress)
		
		for
		{
			con, _ := listener.AcceptTCP()
			
			go myPOP3.handleConnection(con)			
		}				
	}
}
