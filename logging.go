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
"os"
)

const LMIN = 0
const LMED = 1
const LMAX = 2
const LCRAZY = 3

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
        	case LMIN:
        		mylog.Flags = log.Ltime
        		break;
        	case LMED:
        		mylog.Flags = log.Ltime
        		break;
        	case LMAX:
        		mylog.Flags = log.Ldate | log.Ltime
        		break;
        	case LCRAZY:
        		mylog.Flags = log.Ldate | log.Ltime
        		break;
        }
        mylog.L = log.New(os.Stdout, nil, p, mylog.Flags)
        return
}

func (mylog *LogStruct) Log(l int, s string) {
	if(mylog.Logging) {
		if(l <= mylog.Level) {
			mylog.L.Log(s)
		}
	}
}

func (mylog *LogStruct) Logf(l int, format string, v ...interface{}) {
	if(mylog.Logging) {
		if(l <= mylog.Level) {
			mylog.L.Logf(format, v)
		}
	}
}
