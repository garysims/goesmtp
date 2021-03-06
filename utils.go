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
"crypto/hmac"
"os"
"strings"
"fmt"
"bufio"
"net"
"bytes"
"encoding/base64"
"gosqlite.googlecode.com/hg/sqlite"
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
	
	fd.Close()
	return h.Sum()
}

// Hashes a string and returns a 20 byte string.
func MD5String(s string) string {
	h := md5.New()
	h.Write([]byte(s))
	return string(h.Sum())
}

func keyedMD5(key string, s string) string {
	var kb []byte = make([]byte, 64)
	
	for i:=0 ; i < 64 ; i++ {
		if(i >= len(key)) {
			kb[i] = 0
		} else {
			kb[i] = []byte(key)[i]
		}
	}
	h := hmac.NewMD5(kb)
	h.Write([]byte(s))
	return fmt.Sprintf("%x", h.Sum())
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
        if s[end] != '\r' && s[end] != '\n' && s[end] != 0 {
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

func getFilenameForMsg(serverid int, msgsForThisConnection int) string {
	t1, t2, _ := os.Time();
	return fmt.Sprintf("%d-%d-%d-%d", serverid, t1, t2, msgsForThisConnection)
}

func getDomainFromHELO(s []byte) []byte {
	l := len(s)
	return s[5:l]
}

// Create the working directory structure
// This can be more efficient as MkdirAll creates the directory named,
// along with any necessary parents
func createWorkingDirs() {
	var p string
	const perms int = 0766
	var zerotof = map[int] string {
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
}
	
	os.MkdirAll(INQUEUEDIR, perms)
	os.MkdirAll(OUTQUEUEDIR, perms)
	os.MkdirAll(MESSAGESTOREDIR, perms)
	
	d1 := 0
	d2 := 0
	d3 := 0
	d4 := 0

	for {
		p = fmt.Sprintf("%s/%s/%s/%s/%s", MESSAGESTOREDIR, zerotof[d1], zerotof[d2], zerotof[d3], zerotof[d4])
		fmt.Printf("Creating %s\r", p)
		os.MkdirAll(p, perms)
		d1++	
		if(d1 == 16) {
			d1 = 0
			d2++
			if(d2 == 16) {
				d1 = 0
				d2 = 0
				d3++
				if(d3 == 16) {
					d1 = 0
					d2 = 0
					d3 = 0
					d4++
					if(d4 == 16) {
						d1 = 0
						d2 = 0
						d3 = 0
						d4 = 0
						break
					}
				}
			}
		}	
	}
	fmt.Printf("\nDone\n\n")	
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
	var zerotof = map[int] string {
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
}

	delAllFiles(INQUEUEDIR)
	delAllFiles(OUTQUEUEDIR)
	
	d1 := 0
	d2 := 0
	d3 := 0
	d4 := 0

	for {
		p = fmt.Sprintf("%s/%s/%s/%s/%s", MESSAGESTOREDIR, zerotof[d1], zerotof[d2], zerotof[d3], zerotof[d4])
		delAllFiles(p)
		d1++	
		if(d1 == 16) {
			d1 = 0
			d2++
			if(d2 == 16) {
				d1 = 0
				d2 = 0
				d3++
				if(d3 == 16) {
					d1 = 0
					d2 = 0
					d3 = 0
					d4++
					if(d4 == 16) {
						d1 = 0
						d2 = 0
						d3 = 0
						d4 = 0
						break
					}
				}
			}
		}	
	}
	fmt.Printf("\nDone\n\n")	
}

func checkWorkingDirs() bool {

		// in directory
		st, errst := os.Stat(INQUEUEDIR)
		if(errst == nil) {
			if(st.IsDirectory()==false) {
				return false
			}
		} else {
			return false
		}

		// out directory
		st, errst = os.Stat(OUTQUEUEDIR)
		if(errst == nil) {
			if(st.IsDirectory()==false) {
				return false
			}
		} else {
			return false
		}

		// message store directory
		st, errst = os.Stat(MESSAGESTOREDIR)
		if(errst == nil) {
			if(st.IsDirectory()==false) {
				return false
			}
		} else {
			return false
		}

		// messagestore/0/0/0/0
		p := fmt.Sprintf("%s/0/0/0/0", MESSAGESTOREDIR)
		st, errst = os.Stat(p)
		if(errst == nil) {
			if(st.IsDirectory()==false) {
				return false
			}
		} else {
			return false
		}
		
		// And finally messagestore/f/f/f/f
		p = fmt.Sprintf("%s/f/f/f/f", MESSAGESTOREDIR)
		st, errst = os.Stat(p)
		if(errst == nil) {
			if(st.IsDirectory()==false) {
				return false
			}
		} else {
			return false
		}
		
		return true
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
	return fmt.Sprintf("%s/%c/%c/%c/%c", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37], shastr[36])
}

func filename822AndPathFromSHA(shastr string) string {
	return fmt.Sprintf("%s/%c/%c/%c/%c/%s.822", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37], shastr[36], shastr)
}

func findNodeIPFromNodeID(nodeid string) string {

	G_nodesLock.Lock()
	defer G_nodesLock.Unlock()
 	for c := range G_nodes.Iter() {
		if(c.(*nodesInClusterStruct).nodeid == nodeid) {
			return c.(*nodesInClusterStruct).ip
		}
	}	
	return ""
}

func encodeBase64String(s string) string {
	var dest []byte
	var plain []byte
	
	encoding := base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")

	plain = bytes.Add(plain, []byte(s))
	dest = make([]byte, encoding.EncodedLen(len(plain))); 
	encoding.Encode(dest, []byte(plain))
	
	return string(dest)
}

func decodeBase64String(b64 string) string {
	var dest []byte; 
	encoding := base64.NewEncoding("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/")
	dest = make([]byte, encoding.DecodedLen(len(b64))); 
	encoding.Decode(dest, []byte(b64))

	return string(dest)
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
		
	dht, err := sqlite.Open("/var/spool/goesmtp/DHT.db")
	if(err!=nil) {
		fmt.Printf("Can't open the DHT database. FATAL: %s\n", err)
		os.Exit(-1)
	}


	nml, err := sqlite.Open("/var/spool/goesmtp/newMessageLog.db")
	if(err!=nil) {
		fmt.Printf("Can't open the DHT database. FATAL: %s\n", err)
		os.Exit(-1)
	}

	dml, err := sqlite.Open("/var/spool/goesmtp/delMessageLog.db")
	if(err!=nil) {
		fmt.Printf("Can't open the DHT database. FATAL: %s\n", err)
		os.Exit(-1)
	}

	// Empty the current DHT table
	err = dht.Exec("DELETE FROM DHT;")
	if(err!=nil) {
		fmt.Printf("Can't delete table dht. FATAL.\n")
		os.Exit(-1)
	}

	// Empty the current newMessageLog table
	err = nml.Exec("DELETE FROM newMessageLog;")
	if(err!=nil) {
		fmt.Printf("Can't delete table newMessageLog. FATAL.\n")
		os.Exit(-1)
	}

	// Empty the current delMessageLog table
	err = dml.Exec("DELETE FROM delMessageLog;")
	if(err!=nil) {
		fmt.Printf("Can't delete table delMessageLog. FATAL.\n")
		os.Exit(-1)
	}
	
	os.Remove(NEWMESSAGECOUNTERFILE)
	os.Remove(DELMESSAGECOUNTERFILE)
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

func createSQLiteTables() {

	// DHT
	dht, err := sqlite.Open("/var/spool/goesmtp/DHT.db")
	if(err!=nil) {
		fmt.Printf("Can't open the DHT database. FATAL: %s\n", err)
		os.Exit(-1)
	}
	
	err = dht.Exec("create table if not exists dht ( id, sha1, mailbox, cached, size, orignodeid );")
	if(err!=nil) {
		fmt.Printf("Can't create table dht. FATAL.\n")
		os.Exit(-1)
	}

	// Don't check the errors on these as if they already exist it will 
	// produce an error... There is probably a better way in which
	// the error can be checked and ignored if it is the already exits.
	dht.Exec("create index dht_sha1_idx on dht(sha1);")
	dht.Exec("create index dht_id_idx on dht(id);")
	dht.Exec("create index dht_mailbox_idx on dht(mailbox);")
	
	dht.Close()

	// newMessageLog
	nml, err := sqlite.Open("/var/spool/goesmtp/newMessageLog.db")
	if(err!=nil) {
		fmt.Printf("Can't open the newMessageLog database. FATAL: %s\n", err)
		os.Exit(-1)
	}
	
	err = nml.Exec("create table if not exists newMessageLog ( id, sha1, mailbox, size );")
	if(err!=nil) {
		fmt.Printf("Can't create table newMessageLog. FATAL.\n")
		os.Exit(-1)
	}

	// Don't check the errors on these as if they already exist it will 
	// produce an error... There is probably a better way in which
	// the error can be checked and ignored if it is the already exits.
	nml.Exec("create index nml_sha1_idx on newMessageLog(sha1);")
	nml.Exec("create index nml_id_idx on newMessageLog(id);")
	nml.Exec("create index nml_mailbox_idx on newMessageLog(mailbox);")
	
	nml.Close()


	// delMessageLog
	dml, err := sqlite.Open("/var/spool/goesmtp/delMessageLog.db")
	if(err!=nil) {
		fmt.Printf("Can't open the delMessageLog database. FATAL: %s\n", err)
		os.Exit(-1)
	}
	
	err = dml.Exec("create table if not exists delMessageLog ( id, sha1, mailbox );")
	if(err!=nil) {
		fmt.Printf("Can't create table delMessageLog. FATAL.\n")
		os.Exit(-1)
	}

	// Don't check the errors on these as if they already exist it will 
	// produce an error... There is probably a better way in which
	// the error can be checked and ignored if it is the already exits.
	dml.Exec("create index nml_sha1_idx on delMessageLog(sha1);")
	dml.Exec("create index nml_id_idx on delMessageLog(id);")
	dml.Exec("create index nml_mailbox_idx on delMessageLog(mailbox);")
	
	dml.Close()


}
