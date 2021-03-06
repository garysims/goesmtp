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
"log"
"sync"
)

const LOFF = 0
const LERRORSONLY = 1
const LMIN = 2
const LMED = 3
const LMAX = 4
const LCRAZY = 5

var G_loggerLock sync.Mutex

type LogStruct struct {
	Logging         bool
	Prefix			string
	Flags			int
	Level			int
	L				*log.Logger
}

/**
 * Create a new instance 
 */
func NewLogger(p string, level int) (mylog *LogStruct) {
        // Create and return a new instance of LogStruct
        mylog = new(LogStruct)
        mylog.Prefix = p
        mylog.Logging = true
        mylog.Level = level
        switch level {
        	case LOFF:
        		mylog.Flags = 0
        		break;
        	case LERRORSONLY:
        		mylog.Flags = log.Ldate | log.Ltime
        		break;
        	case LMIN:
        		mylog.Flags = log.Ltime
        		break;
        	case LMED:
        		mylog.Flags = log.Ltime
        		break;
        	case LMAX:
        		mylog.Flags = log.Ltime
        		break;
        	case LCRAZY:
        		mylog.Flags = log.Ldate | log.Ltime
        		break;
        }

       	mylog.L = log.New(G_logFileFile, G_logFileFile2, p, mylog.Flags)
        return
}

func (mylog *LogStruct) Log(l int, s string) {
	G_loggerLock.Lock()
	defer G_loggerLock.Unlock()

	if(mylog.Logging) {
		if(l <= mylog.Level) {
			mylog.L.Log(s)
		}
	}
}

func (mylog *LogStruct) Logf(l int, format string, v ...interface{}) {
	G_loggerLock.Lock()
	defer G_loggerLock.Unlock()

	if(mylog.Logging) {
		if(l <= mylog.Level) {
			mylog.L.Logf(format, v)
		}
	}
}
