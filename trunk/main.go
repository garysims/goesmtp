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
"fmt"
"flag"
"os"
"time"
"sync"
)

const VERSION = "V0.1r48"

const MAXRCPT = 100
const INQUEUEDIR = "/var/spool/goesmtp/in"
const OUTQUEUEDIR = "/var/spool/goesmtp/out"
const MESSAGESTOREDIR = "/var/spool/goesmtp/messagestore"
const PASSWORDFILE = "/var/spool/goesmtp/passwords.txt"
const NEWMESSAGECOUNTERFILE = "/var/spool/goesmtp/newmessagecounter"
const DELMESSAGECOUNTERFILE = "/var/spool/goesmtp/delmessagecounter"
const IDFILE = "/var/spool/goesmtp/id.txt"
const CONFIGFILE = "/etc/goesmtp.cfg"

var G_masterNode string
var G_nodeType string
var G_nodeID string
var G_IPAddress string
var G_hostname string = ""
var G_greetDomain string = ""
var G_clusterKey string
var G_dnsbl string = ""
var G_logFileFile *os.File
var G_logFileFile2 *os.File = nil

var G_dhtDBLock sync.Mutex
var G_nmlDBLock sync.Mutex
var G_dmlDBLock sync.Mutex

const G_LoggingLevel = LMIN

func main() {
		
	//
	// Command line actions
	//
	logFileValue := ""
	flag.StringVar(&logFileValue, "f", "", "Log filename")
	cFlag := flag.Bool("c", false, "Log to console")
	
	flag.Parse()   // Scans the arg list and sets up flags
	if(len(logFileValue)>0) {
		fmt.Printf("log: %s\n", logFileValue)
	} else {
		if flag.NArg() != 0 {
			for i := 0; i < flag.NArg(); i++ {
				if(flag.Arg(i) == "forcesync") {
					NewDHTForceSync()
				} else if(flag.Arg(i) == "purge") {
					fmt.Print("Delete all the messages and reset the DB. Are you sure? [y/n] ")
					yorn := getInput()
					if((yorn=="Y") || (yorn=="y")) {
						fmt.Print("Are you really, really sure? [y/n] ")
						yorn = getInput()
						if((yorn=="Y") || (yorn=="y")) {
							// Purge the message store
							purgeMessageStore()
							truncateAllTables()
							updateIDFile(1)
						}
					}
				} else if(flag.Arg(i) == "quickpurge") {
					fmt.Print("Delete all the messages from the DB. Are you sure? [y/n] ")
					yorn := getInput()
					if((yorn=="Y") || (yorn=="y")) {
						fmt.Print("Are you really, really sure? [y/n] ")
						yorn = getInput()
						if((yorn=="Y") || (yorn=="y")) {
							// Purge the DB but not the message store
							truncateAllTables()
							updateIDFile(1)
						}
					}
				} else if(flag.Arg(i) == "createdirs") {
					createWorkingDirs()
				}
			}
			os.Exit(0)
		}
	}

	//
	// Setup logging
	//
	
	os.MkdirAll("/var/log/goesmtp", 0766)
	
	lfn := fmt.Sprintf("/var/log/goesmtp/%s.%d.log",time.LocalTime().Format("20060102.1504"),os.Getpid())
	var err os.Error
	G_logFileFile, err = os.Open(lfn, os.O_CREATE | os.O_RDWR, 0666)
	if (err != nil) {
		G_logFileFile = os.Stdout
	}
	if *cFlag {
		G_logFileFile2 = os.Stdout
	}
	
	// Symbolic link to latest log
	os.Remove("/var/log/goesmtp/goesmtp.log")
	os.Symlink(lfn, "/var/log/goesmtp/goesmtp.log")
	
	logger := NewLogger("MAIN ", G_LoggingLevel)
	logger.Logf(LMIN, "GoESMTP %s starting %s\n", VERSION, time.LocalTime().Format("Mon, 02 Jan 2006 15:04:05 -0700"))

	createSQLiteTables()
	
	c, err := ReadConfigFile(CONFIGFILE);
	if(err==nil) {
		// Need to check the presence of these variables
		// otherwise exit
		var e os.Error
		G_nodeType, e = c.GetString("cluster", "type");
		if(e!=nil) {
			logger.Logf(LERRORSONLY, "Error reading node type (cluster/type) from configuration file. FATAL.")
			os.Exit(-1)
		}
		G_IPAddress, e = c.GetString("cluster", "ip");
		if(e!=nil) {
			logger.Logf(LERRORSONLY, "Error reading node IP address (cluster/ip) from configuration file. FATAL.")
			os.Exit(-1)
		}
		G_masterNode, e = c.GetString("cluster", "master");
		if(e!=nil) {
			if(G_nodeType!="master") {
				logger.Logf(LERRORSONLY, "Error reading master node IP address (cluster/master) from configuration file. FATAL.")
				os.Exit(-1)
			} else {
				logger.Logf(LMIN, "WARNING: Setting master node IP address to %s", G_IPAddress)
				G_masterNode = G_IPAddress
			}
		}
		G_nodeID, e = c.GetString("cluster", "id");
		if(e!=nil) {
			if(G_nodeType!="master") {
				logger.Logf(LERRORSONLY, "Error reading node ID (cluster/id) from configuration file. FATAL.")
				os.Exit(-1)
			} else {
				logger.Logf(LMIN, "WARNING: Setting node ID to 1")
				G_nodeID = "1"
			}
		}
		G_clusterKey, e = c.GetString("cluster", "key");
		if(e!=nil) {
			if(G_nodeType!="master") {
				logger.Logf(LERRORSONLY, "Error reading secret key (cluster/key) from configuration file. FATAL.")
				os.Exit(-1)
			} else {
				logger.Logf(LMIN, "WARNING: No cluster key set. Should be OK if this is a single node configuration.")
				G_nodeID = "1"
			}
		}
		G_hostname, e = c.GetString("smtp", "hostname");
		if(e!=nil) {
			G_hostname, _ = os.Hostname()
		}
		G_greetDomain, e = c.GetString("smtp", "greetdomain");
		if(e!=nil) {
			G_greetDomain, _ = os.Hostname()
		}
		G_dnsbl, e = c.GetString("smtp", "dnsbl");
		if(e!=nil) {
			G_dnsbl = ""
		}
	} else {
		print("Error reading configuation file.\n");
	}
	
	if(checkWorkingDirs()==false) {
		fmt.Printf("There seems to be a problem with the working directory structure. Do you need to run 'goesmtp createdirs'?\n")
		os.Exit(-1)
	}
	
	// Init the list of nodes in the cluster and accounts/passwords
 	G_nodes.Init()
 	G_passwords.Init()

	// For the master node, start cluster ID server so each transaction across the cluster is unique
	if(G_nodeType == "master") {
		go startIDServer()
	}

	// Start process which updates the DHT tables from our transaction
	// log and from other servers in the cluster
	go NewDHTServer().startDHTProcesses()
	
	// Start inbound and outbound SMTP servers
	go NewSMTP().startSMTP()
	go NewSMTPOut().startSMTPOut()
	
	// Start POP3 server
	go NewPOP3().startPOP3()
	
	// Start router
	go NewRouter().startRouter()
	
	// Sleep forever
	for
	{
		// In nano seconds, 1 second = 1 000 000 000 nanoseconds
		time.Sleep(60000000000)
	}	

}

