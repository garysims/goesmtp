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
"crypto/sha1"
"crypto/md5"
 "os"
"strings"
"fmt"
"bufio"
"net"
"bytes"
"encoding/base64"
"mysql"
)

// Hashes a byte slice and returns a 20 byte string.
func SHA1Bytes(b []byte) string {
	h := sha1.New()
	h.Write(b)
	return string(h.Sum())
}

// Hashes a string and returns a 20 byte string.
func SHA1String(s string) string {
	h := sha1.New()
	h.Write([]byte(s))
	return string(h.Sum())
}

func SHA1File(filename string) []byte {
	const CHUNKSIZE = 256
	var inp []byte = make([]byte, CHUNKSIZE)
	var finalchunk []byte
	var le int
	h := sha1.New()

	fd, err := os.Open(filename, os.O_RDONLY, 0666)
	if(err == nil) {
		buf := bufio.NewReader(fd);
		for {
			le, _ = buf.Read(inp)
			if (le == 0) {
				break
			}
			if(le == CHUNKSIZE) {
				h.Write(inp)
			} else {
				// Less than CHUNKSIZE in the buffer so resize the buffer as
				// h.Write will write CHUNKSIZE bytes not just the current length
				finalchunk = make([]byte, le)
				for i := 0; i < le; i++ {
					finalchunk[i] = inp[i]
				}
				h.Write(finalchunk)
			}
		}
	}
		
	return h.Sum()
}

// Hashes a string and returns a 20 byte string.
func MD5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return string(h.Sum())
}


func GetHomedir() string {
	env := os.Environ()
	for _, e := range env {
		if strings.HasPrefix(e, "HOME=") {
		return strings.Split(e, "=", 0)[1]
		}
	}
	return "/tmp"
}

func TrimCRLF(s []byte) []byte {
    start, end := 0, len(s) - 1
    for start < end {
        if s[end] != '\r' && s[end] != '\n' {
            break
        }
        end -= 1
    }
    return s[start:end+1]
}

func getAddressFromMailFrom(s []byte) []byte {
	l := len(s)
	ab := strings.Index(string(s), "<")
	return s[ab+1:l-1]
}

func getAddressFromRcptTo(s []byte) []byte {
	l := len(s)
	ab := strings.Index(string(s), "<")
	return s[ab+1:l-1]
}

func getFilenameForMsg(serverid int) string {
	t1, t2, _ := os.Time();
	return fmt.Sprintf("%d-%d-%d", serverid, t1, t2)
}

func getDomainFromHELO(s []byte) []byte {
	l := len(s)
	return s[5:l]
}

func createWorkingDirs() {
	var p string
	const perms int = 0766
	var atoz = map[int] string {
    0 : "0",
    1 : "1",
    2 : "2",
    3 : "3",
    4 : "4",
    5 : "5",
    6 : "6",
    7 : "7",
    8 : "8",
    9 : "9",
    10 : "a",
    11 : "b",
    12 : "c",
    13 : "d",
    14 : "e",
    15 : "f",
    16 : "g",
    17 : "h",
    18 : "i",
    19 : "j",
    20 : "k",
    21 : "l",
    22 : "m",
    23 : "n",
    24 : "o",
    25 : "p",
    26 : "q",
    27 : "r",
    28 : "s",
    29 : "t",
    30 : "u",
    31 : "v",
    32 : "w",
    33 : "x",
    34 : "y",
    35 : "z",    
}
	
	os.Mkdir(INQUEUEDIR, perms)
	os.Mkdir(OUTQUEUEDIR, perms)
	os.Mkdir(MESSAGESTOREDIR, perms)
	
	d1 := 0
	d2 := 0
	d3 := 0
//	d4 := 1

	for {
//		p = fmt.Sprintf("%s/%s/%s/%s/%s", MESSAGESTOREDIR, atoz[d1], atoz[d2], atoz[d3], atoz[d4])
		p = fmt.Sprintf("%s/%s/%s/%s", MESSAGESTOREDIR, atoz[d1], atoz[d2], atoz[d3])
//		fmt.Println(p)
		os.MkdirAll(p, perms)
		d1++	
		if(d1 == 36) {
			d1 = 0
			d2++
			if(d2 == 36) {
				d1 = 0
				d2 = 0
				d3++
				if(d3 == 36) {
					d1 = 0
					d2 = 0
					d3 = 0
					break
//					d4++
//					if(d4 == 36) {
//						break
//					}
				}
			}
		}	
	}
}

func delAllFiles(path string) {
fmt.Printf("Purging %s\r", path)

	dir, errd := os.Open(path, 0, 0666)
	
	if(errd != nil) {
		fmt.Printf("os.Open %s\n", errd)	
		return
	}
	
	fi, err := dir.Readdir(-1)
		
	if(err == nil) {
		for i := 0; i < len(fi); i++ {
			full := fmt.Sprintf("%s/%s", path, fi[i].Name)
			st, errst := os.Stat(full)
			if(errst == nil) {
				if(st.IsDirectory()==false) {
					os.Remove(full)
				}
			} else {
				fmt.Printf("Error %s\n", errst)			
			}
		}
	}
	dir.Close()
}

func purgeMessageStore() {
	var p string
	const perms int = 0766
	var atoz = map[int] string {
    0 : "0",
    1 : "1",
    2 : "2",
    3 : "3",
    4 : "4",
    5 : "5",
    6 : "6",
    7 : "7",
    8 : "8",
    9 : "9",
    10 : "a",
    11 : "b",
    12 : "c",
    13 : "d",
    14 : "e",
    15 : "f",
    16 : "g",
    17 : "h",
    18 : "i",
    19 : "j",
    20 : "k",
    21 : "l",
    22 : "m",
    23 : "n",
    24 : "o",
    25 : "p",
    26 : "q",
    27 : "r",
    28 : "s",
    29 : "t",
    30 : "u",
    31 : "v",
    32 : "w",
    33 : "x",
    34 : "y",
    35 : "z",    
}

	delAllFiles(INQUEUEDIR)
	delAllFiles(OUTQUEUEDIR)
	
	d1 := 0
	d2 := 0
	d3 := 0

	for {
		p = fmt.Sprintf("%s/%s/%s/%s", MESSAGESTOREDIR, atoz[d1], atoz[d2], atoz[d3])
		delAllFiles(p)
		d1++	
		if(d1 == 36) {
			d1 = 0
			d2++
			if(d2 == 36) {
				d1 = 0
				d2 = 0
				d3++
				if(d3 == 36) {
					d1 = 0
					d2 = 0
					d3 = 0
					break
				}
			}
		}	
	}
}
func getIDFromIDServer() string {
	id := ""
	
	m := fmt.Sprintf("%s:4321", G_masterNode)
	con, errdial := net.Dial("tcp", "", m)
	if(errdial == nil) {
		buf := bufio.NewReader(con);
		lineofbytes, err := buf.ReadBytes('\n');
		if err != nil {
			con.Close()
			fmt.Printf("Error reading from master node %s. FATAL.", m)
			os.Exit(-1)
		} else {
			lineofbytes = TrimCRLF(lineofbytes)
			id = string(lineofbytes)
			con.Close()
		}
	} else {
		fmt.Printf("Can't connect to master node %s. FATAL.", m)
		os.Exit(-1)
	}

	return id
}

func pathFromSHA(shastr string) string {
	return fmt.Sprintf("%s/%c/%c/%c", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37])
}

func filename822AndPathFromSHA(shastr string) string {
	return fmt.Sprintf("%s/%c/%c/%c/%s.822", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37], shastr)
}

func findNodeIPFromNodeID(nodeid string) string {

	G_nodesLock.Lock()
 	for c := range G_nodes.Iter() {
		if(c.(*nodesInClusterStruct).nodeid == nodeid) {
			G_nodesLock.Unlock()
			return c.(*nodesInClusterStruct).ip
		}
	}	
	G_nodesLock.Unlock()
	return ""
}

func encodeSMTPAuthPlain(user string, password string) []byte {
	var dest []byte
	var plain []byte
	var zero []byte
	zero = make([]byte, 1)
	zero[0] = 0
	
	encoding := base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")

	plain = bytes.Add(plain, []byte(user))
	plain = bytes.Add(plain, []byte(zero))
	plain = bytes.Add(plain, []byte(user))
	plain = bytes.Add(plain, []byte(zero))
	plain = bytes.Add(plain, []byte(password))

	dest = make([]byte, encoding.EncodedLen(len(plain))); 
	encoding.Encode(dest, []byte(plain))
	
	return dest
}

func decodeSMTPAuthPlain(b64 string) (string, string, string) {
	var dest []byte; 
	encoding := base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
	dest = make([]byte, encoding.DecodedLen(len(b64))); 
	encoding.Decode(dest, []byte(b64))

	var zero []byte
	zero = make([]byte, 1)
	zero[0] = 0
	f := bytes.Split(dest, zero, 0)

	if((len(f) == 4) || (len(f) == 3)) {
		return string(f[0]), string(f[1]), string(f[2])
	} else {
		return "","",""
	}

	return "","",""
}

func getInput() string {

	in := bufio.NewReader(os.Stdin);
	//the string value of the input is stored in the variable input
   input, err := in.ReadString('\n');
   if err != nil {
			// handle error
			return ""
	}
	
	return string(TrimCRLF([]byte(input)))
}

func truncateAllTables () {


	c, err := ReadConfigFile("config.cfg");
	if(err!=nil) {
			fmt.Printf("Can't read config file\n")
			os.Exit(-1)
	}
		
	DBusername, _ := c.GetString("db", "username");
	DBpassword, _ := c.GetString("db", "password");
	DBhost, _ := c.GetString("db", "host");
	DBdatabase, _ := c.GetString("db", "database");

	// Create new instance
	db := mysql.New()
	// Enable/Disable logging
	db.Logging = false
	// Connect to database
	db.Connect(DBhost, DBusername, DBpassword, DBdatabase)
	if db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", db.Errno, db.Error)
			os.Exit(1)
	}


	// Empty the current DHT table
	db.Query("TRUNCATE TABLE DHT;")
	if db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", db.Errno, db.Error)
	}

	// Empty the current newMessageLog table
	db.Query("TRUNCATE TABLE newMessageLog;")
	if db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", db.Errno, db.Error)
	}

	// Empty the current delMessageLog table
	db.Query("TRUNCATE TABLE delMessageLog;")
	if db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", db.Errno, db.Error)
	}
	
	// Empty the current delMessageCounter table
	db.Query("TRUNCATE TABLE delMessageCounter;")
	if db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", db.Errno, db.Error)
	}

	// Empty the current newMessageCounter table
	db.Query("TRUNCATE TABLE newMessageCounter;")
	if db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", db.Errno, db.Error)
	}
}

func compareConWithIPString(con *net.TCPConn, ip2 string) bool {
	raddr := con.RemoteAddr()

	ipbits := strings.Split(raddr.String() , ":", 0)
	if(ipbits[0] == ip2) {
		return true
	}
	
	return false
}

func isNodeAuthenticated(con *net.TCPConn) bool {
	raddr := con.RemoteAddr()

	ipbits := strings.Split(raddr.String() , ":", 0)

	// Does this IP address or node already exist?
	G_nodesLock.Lock()
	defer G_nodesLock.Unlock()
 	for c := range G_nodes.Iter() {
		if((c.(*nodesInClusterStruct).ip == ipbits[0])) {
			return true
		}
	}	
	return false
}

