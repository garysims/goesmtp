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
)

const MAXRCPT = 100
const INQUEUEDIR = "./in"
const OUTQUEUEDIR = "./out"
const MESSAGESTOREDIR = "./messagestore"
const VERSION = "V0.1"

var G_masterNode string
var G_nodeType string
var G_nodeID string
var G_IPAddress string
var G_domainOverride string = ""
var G_clusterKey string

const G_LoggingLevel = LCRAZY

func main() {
	fmt.Printf("goESMTP %s starting...\n", VERSION)

	c, err := ReadConfigFile("config.cfg");
	if(err==nil) {
		// Need to check the presence of these variables
		// otherwise exit
		G_masterNode, _ = c.GetString("cluster", "master");
		G_nodeType, _ = c.GetString("cluster", "type");
		G_nodeID, _ = c.GetString("cluster", "id");
		G_IPAddress, _ = c.GetString("cluster", "ip");
		G_clusterKey, _ = c.GetString("cluster", "key");
		G_domainOverride, _ = c.GetString("smtp", "domain");
	} else {
		print("Error reading configuation file.\n");
	}
	
	//
	// Command line actions
	//
	
	flag.Parse()   // Scans the arg list and sets up flags
	if flag.NArg() != 0 {
        for i := 0; i < flag.NArg(); i++ {
			if(flag.Arg(i) == "forcesync") {
				NewDHTForceSync()
			} else if(flag.Arg(i) == "purge") {
				fmt.Println("Delete all the messages and reset the DB. Are you sure? [y/n]")
				yorn := getInput()
				if((yorn=="Y") || (yorn=="y")) {
					fmt.Println("Are you really, really sure? [y/n]")
					yorn = getInput()
					if((yorn=="Y") || (yorn=="y")) {
						// Purge the message store
						purgeMessageStore()
						truncateAllTables()
						updateIDFile(1)
					}
				}
			} else if(flag.Arg(i) == "quickpurge") {
				fmt.Println("Delete all the messages from the DB. Are you sure? [y/n]")
				yorn := getInput()
				if((yorn=="Y") || (yorn=="y")) {
					fmt.Println("Are you really, really sure? [y/n]")
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

