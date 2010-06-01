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
"regexp"
"fmt"
"mysql"
"bufio"
"time"
"container/list"
"sync"
"strings"
"net"
)


// Forwarding???
type passwordStruct struct {
	username string
	password string
	alias string
}

var G_passwords list.List
var G_passwordLock sync.Mutex

//
// Accounts / Passwords functions
//

// NOTE: When you implement aliases note that ggary@garysims.co.uk and gary@garysims.co.uk
// are different and so a simple substring match in alias (comma separated list) won't work

func getPasswordInfo(username string) (string, string) {
	G_passwordLock.Lock()
	defer G_passwordLock.Unlock()
 	for c := range G_passwords.Iter() {
		if(c.(*passwordStruct).username == username) {
			return c.(*passwordStruct).password, c.(*passwordStruct).alias
		}
	}	
	return "", ""
}

func dumpPasswordsToConn(con *net.TCPConn) {
	// Probaly need to send the number of passwords as the first line
	// Encryption???
	G_passwordLock.Lock()
 	for c := range G_passwords.Iter() {
		con.Write([]byte(fmt.Sprintf("%s:%s:%s\r\n", c.(*passwordStruct).username, c.(*passwordStruct).password, c.(*passwordStruct).alias)))
	}	
	G_passwordLock.Unlock()
}

// This should be a MAP not a linked list
func (myRouter *routerStruct) addPasswordToList(username string, password string, alias string) {
	G_passwordLock.Lock()
 	p := new(passwordStruct)
 	p.username = username
 	p.password = password
 	p.alias = alias
	G_passwords.PushBack(p)	
	G_passwordLock.Unlock()
}

func (myRouter *routerStruct) readPasswords() {
	fd, err := os.Open("passwords.txt", os.O_RDONLY, 0666)
	if(err == nil) {
		buf := bufio.NewReader(fd);
		for {
			lineofbytes, errl := buf.ReadBytes('\n');
			if errl != nil {
				fd.Close()
				break
			} else {
				lineofbytes = TrimCRLF(lineofbytes)
				fmt.Printf("readPasswords: %s\n", string(lineofbytes))
				f := strings.Split(string(lineofbytes), ":", 0)
				if(len(f)!=3) {
					fmt.Printf("readPasswords: badly formatted line: %s\n", string(lineofbytes))
				} else {
					myRouter.addPasswordToList(f[0], f[1], f[2])
				}
			}
		}
	} else {
		fmt.Println("Can't open passwords.txt... FATAL")
		os.Exit(-4)
	}
}

func (myRouter *routerStruct) askForPasswords() {
	m := fmt.Sprintf("%s:4322", G_masterNode)	
	dcon, errdial := net.Dial("tcp", "", m)
	
	if(errdial != nil) {
		myRouter.logger.Log(LMAX, "Network connection unexpectedly closed.")			
		return	
	}

	// Get greeting
	buf := bufio.NewReader(dcon);
	lineofbytes, err := buf.ReadBytes('\n');
	if err != nil {
		myRouter.logger.Log(LMAX, "Network connection unexpectedly closed.")			
		return
	}


	dcon.Write([]byte(fmt.Sprintf("PASSWORDS\r\n")))

	
	// Reply is just a dump of the password. Maybe the first line should be the
	// number of entries.
	// Encryption?
	for {
		lineofbytes, err = buf.ReadBytes('\n');
		if err != nil {
			dcon.Close()
			break
		} else {
			lineofbytes = TrimCRLF(lineofbytes)
			fmt.Printf("askForPasswords: %s\n", string(lineofbytes))
			f := strings.Split(string(lineofbytes), ":", 0)
			if(len(f)!=3) {
				fmt.Printf("askForPasswords: badly formatted line: %s\n", string(lineofbytes))
			} else {
				myRouter.addPasswordToList(f[0], f[1], f[2])
			}		
		}
	}
}

type routerStruct struct {
	logger *LogStruct
	DBusername string
	DBpassword string
	DBhost string
	DBdatabase string
	db *mysql.MySQL
}

func NewRouter() (myRouter *routerStruct) {
	// Create and return a new instance of routerStruct
	myRouter = new(routerStruct)

	myRouter.logger = NewLogger("Router ", G_LoggingLevel)
	
	myRouter.logger.Log(LMIN, "Starting...")
	
	c, err := ReadConfigFile("config.cfg");
	if(err==nil) {
		myRouter.DBusername, _ = c.GetString("db", "username");
		myRouter.DBpassword, _ = c.GetString("db", "password");
		myRouter.DBhost, _ = c.GetString("db", "host");
		myRouter.DBdatabase, _ = c.GetString("db", "database");
	}

	myRouter.connectToDB()
 
 	return
}

func (myRouter *routerStruct) updateNewMessageLog(sha1 string, sz int64, mailbox string) bool {

	id := getIDFromIDServer()
	sql := fmt.Sprintf("INSERT INTO goesmtp.newMessageLog (id, sha1, mailbox, size) VALUES ('%s', '%s', '%s', '%d')", id, sha1, mailbox, sz)
	myRouter.logger.Logf(LMAX, "updateNewMessageLog SQL: %s", sql)

	myRouter.db.Query(sql)
	if myRouter.db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", myRouter.db.Errno, myRouter.db.Error)
			return false
	}
	
	return true
}

// e.g
//Received: from air177.startdedicated.com (69.64.33.197)
//        by mx.google.com with ESMTP id j8si6313268ana.99.2010.05.07.07.12.36;
//        Fri, 07 May 2010 07:12:37 -0700 (PDT)

func (myRouter *routerStruct) addOurReceivedField(fd *os.File, mailfrom string, rcptto string, helo string, ehlo string, uid string, fn822 string, addReturnPath bool) {

	var wi, fr string = "", ""

	hostname, _ := os.Hostname()
	if(len(ehlo) != 0) {
		wi = "ESMTP"
		fr = ehlo
	} else {
		wi = "SMTP"
		fr = helo
	}

	dateTime:= time.LocalTime().Format("Mon, 02 Jan 2006 15:04:05 -0700")
	
	if(addReturnPath==true) {
		r := fmt.Sprintf("Return-Path: <%s>\r\n", mailfrom)
		fd.Write([]byte(r))
	}

	r := fmt.Sprintf("Received: from %s\r\n\tby %s with %s id %s\r\n\tfor %s;\r\n\t%s\r\n",
		fr,
		hostname,
		wi,
		uid,
		rcptto,
		dateTime)
		
	fd.Write([]byte(r))
}

//
// routetorecipient
// 
// Returns false on failure which means the original file in the INQUEUEDIR should NOT be deleted.
// Returns true when file was routed successfully
//
func (myRouter *routerStruct) routetorecipient(f string, fn821 string, fn822 string, mailfrom string, rcptto string, helo string, ehlo string) bool {
	var addReturnPath bool
	var routefn821 string
	var routefn822 string
	
	
	myRouter.logger.Logf(LMIN, "Route message (%s) from sender %s to %s", fn822, mailfrom, rcptto)

	mailbox, _  := getPasswordInfo(rcptto)

	//
	// Create a new .821 file with only ONE recipient for routing
	// Only needed when NOT for local delivery
	//
	if(mailbox=="") {
	
		routefn821 = fmt.Sprintf("%s.route", fn821)
	
		myRouter.logger.Logf(LMAX, ".821 file is: %s", fn821)	
		myRouter.logger.Logf(LMAX, ".821.route file is: %s", routefn821)
		
		fd, err := os.Open(routefn821, os.O_CREATE | os.O_RDWR, 0666)
	
		if (err == nil) {
			if(len(ehlo)==0) {
				fd.Write([]byte(fmt.Sprintf("helo %s\r\n", helo)))	
			} else {
				fd.Write([]byte(fmt.Sprintf("ehlo %s\r\n", ehlo)))	
			}
			fd.Write([]byte(fmt.Sprintf("mail from:<%s>\r\n", mailfrom)))	
			fd.Write([]byte(fmt.Sprintf("rcpt to:<%s>\r\n", rcptto)))	
		} else {
			myRouter.logger.Logf(LMIN, "Can't create file: %s", routefn821)
			return false
		}
		fd.Close()
	}	

	//
	// Create new .822 file with a new received field
	// This is needed regardless of local or remote delivery
	//
	routefn822 = fmt.Sprintf("%s.route", fn822)

	myRouter.logger.Logf(LMAX, ".822 file is: %s", fn822)	
	myRouter.logger.Logf(LMAX, ".822.route file is: %s", routefn822)	
	
	fd, err := os.Open(routefn822, os.O_CREATE | os.O_RDWR, 0666)

	if (err == nil) {
		if(mailbox=="") {
			addReturnPath = false
		} else {
			addReturnPath = true
		}
		myRouter.addOurReceivedField(fd, mailfrom, rcptto, helo, ehlo, f, fn822, addReturnPath)
		
	} else {
		myRouter.logger.Logf(LMIN, "Can't create file: %s", routefn822)
		return false
	}

	// Now open the original body file and add the rest of the data
	
	body, errb := os.Open(fn822, os.O_RDONLY, 0666)
	
	if (errb == nil) {
		buf := bufio.NewReader(body);
		for {
			lineofbytes, errl := buf.ReadBytes('\n');
			if errl != nil {
				body.Close()
				break
			} else {
				fd.Write(lineofbytes)
			}
		}
	} else {
		myRouter.logger.Logf(LMIN, "routetorecipient - Can't open file: %s", fn822)
		return false
	}
	fd.Close()

	//
	// At this point 0-1273590401-748974000.822.route has the body with the added
	// recevied fields...
	// The .821 (if it exists) is just for one recipient...
	// Now it can be routed...
	//

	sha := SHA1File(routefn822)
	shastr := fmt.Sprintf("%x", sha)
	myRouter.logger.Logf(LMAX, "sha1 for %s is %s", routefn822, shastr)

	// Local or remote delivery???
	if(mailbox=="") {
		// Not for local delivery, move to out queue
		myRouter.logger.Logf(LMIN, "Not for local delivery (%s), send to out queue", rcptto)
		path := fmt.Sprintf("%s", OUTQUEUEDIR)

		// Prepare to move files to out queue
		newoq821 := fmt.Sprintf("%s/%s.821", path, shastr)
		newoq822 := fmt.Sprintf("%s/%s.822", path, shastr)
		myRouter.logger.Logf(LMAX, ".821 in out queue: %s", newoq821)
		myRouter.logger.Logf(LMAX, ".822 in out queue: %s", newoq822)

		// Move files to out queue
		os.Rename(routefn821, newoq821)
		os.Rename(routefn822, newoq822)
	} else {
		// Local delivery, move to message store
		path := fmt.Sprintf("%s/%c/%c/%c/%c", MESSAGESTOREDIR, shastr[39], shastr[38], shastr[37], shastr[36])
		myRouter.logger.Logf(LMAX, "Path in message store: %s", path)
		
		// Prepare to move files to message store
		newms822 := fmt.Sprintf("%s/%s.822", path, shastr)
		myRouter.logger.Logf(LMAX, ".822 in message store: %s", newms822)
	
		// Get the size of the body
		stat, staterr := os.Stat(routefn822)	
		
		if(staterr != nil) {
			myRouter.logger.Logf(LMIN, "Can't stat file: %s - FATAL", routefn822)
			return false
		}
		
		// Update transaction log
		if(myRouter.updateNewMessageLog(shastr, stat.Size, rcptto)) {
	
			// Move files to message store
			os.Rename(routefn822, newms822)
		} else {
			// Couldn't update DB... Remove the .route and leave the original
			// to be processed again later
			os.Remove(routefn822)
		}
		myRouter.logger.Logf(LMIN, "Message for %s saved in message store", rcptto)
		
	}
	
	return true
}

func (myRouter *routerStruct) route(f string) {

	heloCmd, _ := regexp.Compile("^helo ");
	ehloCmd, _ := regexp.Compile("^ehlo ");
	mailfromCmd, _ := regexp.Compile("^mail from:");
	rcpttoCmd, _ := regexp.Compile("^rcpt to:");
	var helo string
	var ehlo string
	var mailfrom string
	var rcptto string
	
	fnlen := len(f)
	
	fn821 := fmt.Sprintf("%s/%s1", INQUEUEDIR, f[0:fnlen-1])
	fn822 := fmt.Sprintf("%s/%s", INQUEUEDIR, f)
	
	//
	// Open the .821 file and process the RCPT TO fields
	//
	fd, err := os.Open(fn821, os.O_RDONLY, 0666)

	if (err == nil) {
		anyProblems := false
		buf := bufio.NewReader(fd);
		for {
			lineofbytes, errl := buf.ReadBytes('\n');
			lineofbytes = TrimCRLF(lineofbytes)
			if errl != nil {
				fd.Close()
				break
			} else {
				switch {
					case heloCmd.Match(lineofbytes):
						helo = string(getDomainFromHELO(lineofbytes))
						myRouter.logger.Logf(LMAX, "HELO field: %s", helo)						
						break
					case ehloCmd.Match(lineofbytes):
						ehlo = string(getDomainFromHELO(lineofbytes))
						myRouter.logger.Logf(LMAX, "EHLO field: %s", ehlo)						
						break
					case mailfromCmd.Match(lineofbytes):
						mailfrom = string(getAddressFromMailFrom(lineofbytes))
						myRouter.logger.Logf(LMAX, "MAIL FROM field: %s", mailfrom)						
						break
					case rcpttoCmd.Match(lineofbytes):
						rcptto = string(getAddressFromMailFrom(lineofbytes))
						myRouter.logger.Logf(LMAX, "RCPT TO field: %s", rcptto)
						if(myRouter.routetorecipient(f, fn821, fn822, mailfrom, rcptto, helo, ehlo) == false) {
							anyProblems = true
						}
						break
					default:
						break
				}
			}
		}
		if(anyProblems == false) {
			os.Remove(fn822)
			os.Remove(fn821)
		}
	} else {
		myRouter.logger.Logf(LMIN, "Can't open file: %s", fn821)		
	}
}

func (myRouter *routerStruct) connectToDB() {
	// Create new instance
	myRouter.db = mysql.New()
	// Enable/Disable logging
	myRouter.db.Logging = false
	// Connect to database
	myRouter.db.Connect(myRouter.DBhost, myRouter.DBusername, myRouter.DBpassword, myRouter.DBdatabase)
	if myRouter.db.Errno != 0 {
			fmt.Printf("Error #%d %s\n", myRouter.db.Errno, myRouter.db.Error)
			os.Exit(1)
	}
}

func (myRouter *routerStruct) startRouter() {
	endingWith822, _ := regexp.Compile(".822$");

    // Possible critical race here as the passwords can't be retrieved until the
    // master has been pinged at least once to establish authentication
    if(G_nodeType=="master") {
		myRouter.readPasswords()
	} else {
		myRouter.askForPasswords()
	}
	
	for
	{
		// In nano seconds, 1 second = 1 000 000 000 nanoseconds
		time.Sleep(5000000000)
	
		dir, direrr := os.Open(INQUEUEDIR, 0, 0666)
		
		if(direrr == nil) {		
			fi, err := dir.Readdir(-1)
			
			if(err == nil) {
				for i := 0; i < len(fi); i++ {
					if(endingWith822.Match([]byte(fi[i].Name))) {
						myRouter.logger.Logf(LMIN, "Processing message %s\n", fi[i].Name)
						myRouter.route(fi[i].Name)
					}
				}
			}		
			dir.Close()
		}
	}
}
