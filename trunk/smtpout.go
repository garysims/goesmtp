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

// SMTP Out

import (
"os"
"regexp"
"time"
"fmt"
"bufio"
"strings"
"net"
)


type smtpOutStruct struct {
	logger *LogStruct
}

func NewSMTPOut() (mySMTPOut *smtpOutStruct) {
	// Create and return a new instance of smtpOutStruct
	mySMTPOut = new(smtpOutStruct)

	mySMTPOut.logger = NewLogger("SMTP OUT ", G_LoggingLevel)
	
	mySMTPOut.logger.Log(LMIN, "Starting...")
	 
 	return
}

func (mySMTPOut *smtpOutStruct) findPrefMX(addrs []string, prefs []uint16) string {
	p := prefs[0]
	a := addrs[0]
	
	for i:=1; i<len(addrs); i++ {
		if(prefs[i] < p) {
			p = prefs[i]
			a = addrs[i]
		}
	}
	
	// Return the best MX host and remove the trailing .
	return a[0:len(a)-1]
}

func (mySMTPOut *smtpOutStruct) getAndCheckResp(buf *bufio.Reader, code string) bool {
	lineofbytes, err := buf.ReadBytes('\n');
	if err != nil {
		mySMTPOut.logger.Log(LMIN, "Network connection unexpected closed while sending message.")			
		return false
	} else {
		mySMTPOut.logger.Logf(LMAX, "Server said: %s", string(lineofbytes))			
		if(string(lineofbytes)[0:len(code)]==code) {
			return true
		} else {
			return false
		}
	}
	
	return false
}

func (mySMTPOut *smtpOutStruct) createNDN(sender string, failedrcptto string, origfn822 string) bool {

	ndn821 := fmt.Sprintf("%s/NDN-%s1.tmp", INQUEUEDIR, origfn822[0:len(origfn822)-1])
	ndn822 := fmt.Sprintf("%s/NDN-%s.tmp", INQUEUEDIR, origfn822)

	mySMTPOut.logger.Logf(LMIN, "Send NDN to %s for failed recipient %s", sender, failedrcptto)
	mySMTPOut.logger.Logf(LMAX, "Files for NDN are: %s %s", ndn821, ndn822)
	
	// Create the .821 file
	fd, err := os.Open(ndn821, os.O_CREATE | os.O_RDWR, 0666)

	if (err == nil) {
		fd.WriteString("helo ")
		h, _ := os.Hostname()
		fd.WriteString(h);		
		fd.WriteString("\r\n");
		
		fd.WriteString("mail from:<mailer-daemon@")
		fd.WriteString(h);		
		fd.WriteString(">\r\n");

		fd.WriteString("rcpt to:<")
		fd.WriteString(sender);		
		fd.WriteString(">\r\n");

		fd.Close()
	} else {
		mySMTPOut.logger.Logf(LMIN, "Big OOOPs, can't create .821 file: %s\n", ndn821)
		return false
	}

	// Create the .822 file
	fd, err = os.Open(ndn822, os.O_CREATE | os.O_RDWR, 0666)

	if (err == nil) {
		h, _ := os.Hostname()
		fd.WriteString("Return-Path: <>\r\n")
		fd.WriteString("MIME-Version: 1.0\r\n")
		fd.WriteString("From: Mail Delivery Subsystem <mailer-daemon@")
		fd.WriteString(h)
		fd.WriteString(">\r\n")
		fd.WriteString("To: ")
		fd.WriteString(sender);		
		fd.WriteString("\r\n");
		fd.WriteString("Subject: Delivery Status Notification (Failure)\r\n")
		fd.WriteString("Message-ID: <")
		fd.WriteString(origfn822[0:len(origfn822)-4])
		fd.WriteString("@")
		fd.WriteString(h)
		fd.WriteString(">\r\n")
		dateTime:= time.LocalTime().Format("Mon, 02 Jan 2006 15:04:05 -0700")
		fd.WriteString("Date: ")
		fd.WriteString(dateTime)		
		fd.WriteString("\r\n")
		fd.WriteString("Content-Type: text/plain; charset=ISO-8859-1\r\n\r\n")
		
		fd.WriteString("Delivery to the following recipient failed permanently:\r\n\r\n")
		fd.WriteString(failedrcptto)
		fd.WriteString("\r\n\r\n")
		fd.WriteString("----- Original message -----\r\n\r\n")

		// Now open the original body file and add the rest of the data
		fn822 := fmt.Sprintf("%s/%s", OUTQUEUEDIR, origfn822)
		
		body, errb := os.Open(fn822, os.O_RDONLY, 0666)
		linecount := 0
		inbody := false
		if (errb == nil) {
			buf := bufio.NewReader(body);
			for {
				lineofbytes, errl := buf.ReadBytes('\n');
				if((inbody==false) && (len(lineofbytes)<=2)) { inbody=true }
				if errl != nil {
					body.Close()
					break
				} else {
					fd.Write(lineofbytes)
				}
				if(inbody) {
					linecount += 1
					if(linecount > 20) {
						body.Close()
						break
					}
				}
			}
		} else {
			mySMTPOut.logger.Logf(LMIN, "createNDN - Can't open file: %s", fn822)
			os.Exit(-1)	
		}


		fd.Close()
	} else {
		mySMTPOut.logger.Logf(LMIN, "Big OOOPs, can't create .822 file: %s\n", ndn822)
		return false
	}

	
	// Now rename the files so they will be processed by the router
	newndn821 := fmt.Sprintf("%s/NDN-%s1", INQUEUEDIR, origfn822[0:len(origfn822)-1])
	newndn822 := fmt.Sprintf("%s/NDN-%s", INQUEUEDIR, origfn822)
	os.Rename(ndn821, newndn821)
	os.Rename(ndn822, newndn822)		
	
	return true
}

func (mySMTPOut *smtpOutStruct) sendTheBody(con net.Conn, fn822 string) {
	fn := fmt.Sprintf("%s/%s", OUTQUEUEDIR, fn822)
	body, errb := os.Open(fn, os.O_RDONLY, 0666)
		
	if (errb == nil) {
		buf := bufio.NewReader(body);
		mySMTPOut.logger.Logf(LMAX, "sendTheBody - sending: %s", fn)

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
		mySMTPOut.logger.Logf(LMIN, "sendTheBody - Can't open file: %s", fn)
		return
	}
	con.Write([]byte(".\r\n"))
}

func (mySMTPOut *smtpOutStruct) sendBySMTP(fn821 string, fn822 string, mailfrom string, rcptto string) {
	parts := strings.Split(rcptto, "@", 0)
	if(len(parts)!=2) {
		mySMTPOut.logger.Logf(LMAX, "Bad RCPT TO field: %s", rcptto)
		return
	} else {
		mx, prefs, err := LookupMX(parts[1])
		if(err == nil) {
			toserver := mySMTPOut.findPrefMX(mx, prefs)
			mySMTPOut.logger.Logf(LMIN, "Send message for %s to %s", rcptto, toserver)
			m := fmt.Sprintf("%s:25", toserver)
			con, errdial := net.Dial("tcp", "", m)
			if(errdial == nil) {
				buf := bufio.NewReader(con);
	
				// First line is SMTP greeting e.g. "220 mx.google.com ESMTP w12si8713323fah.87"
				
				// TBD: All the cases when the wrong reponse is sent!
				// TBD: ESMTP. Note the first 250 response can be multi line.
				if(mySMTPOut.getAndCheckResp(buf, "220 ")==true) {
					h, _ := os.Hostname()
					con.Write([]byte(fmt.Sprintf("HELO %s\r\n", h)))
					if(mySMTPOut.getAndCheckResp(buf, "250 ")==true) {
						con.Write([]byte(fmt.Sprintf("MAIL FROM:<%s>\r\n", mailfrom)))
						if(mySMTPOut.getAndCheckResp(buf, "250 ")==true) {
							con.Write([]byte(fmt.Sprintf("RCPT TO:<%s>\r\n", rcptto)))
							if(mySMTPOut.getAndCheckResp(buf, "250 ")==true) {
								con.Write([]byte(fmt.Sprintf("DATA\r\n")))
								if(mySMTPOut.getAndCheckResp(buf, "354 ")==true) {
									mySMTPOut.sendTheBody(con, fn822)
									if(mySMTPOut.getAndCheckResp(buf, "250 ")==true) {
										con.Write([]byte(fmt.Sprintf("QUIT\r\n")))
										
										// OK message sent, remove it from the out queue
										// TBD: This is a bit inconsistent, the 821 file includes
										// the out queue dir and the 822 file does NOT.
										os.Remove(fmt.Sprintf("%s/%s", OUTQUEUEDIR, fn822))
										os.Remove(fn821)										
									} else {
										// Doesn't like data sent... Again this could be because of SPAM filtering
										// For example Google reply to the DATA (after it is sent) with:
										// 550-5.7.1 [X.X.X.X] The IP you're using to send mail is not authorized to										
										mySMTPOut.logger.Logf(LMAX, "Server (%s) didn't like the data (RFC822 body) sent", toserver)
										// Send NDN
										if(mySMTPOut.createNDN(mailfrom, rcptto, fn822)) {
											os.Remove(fmt.Sprintf("%s/%s", OUTQUEUEDIR, fn822))
											os.Remove(fn821)
										}
									}
								} else {
									// Doesn't like DATA command
									mySMTPOut.logger.Logf(LMAX, "Server (%s) didn't like the DATA command", toserver)
								}
							} else {
								// Bad response to RCPT TO... Send NDN
								if(mySMTPOut.createNDN(mailfrom, rcptto, fn822)) {
									os.Remove(fmt.Sprintf("%s/%s", OUTQUEUEDIR, fn822))
									os.Remove(fn821)										
								}
							}
						} else {
							// Bad response to MAIL FROM
							mySMTPOut.logger.Logf(LMAX, "Server (%s) didn't like the MAIL FROM", toserver)
						}
					} else {
						// Bad reponse to our HELO command
						mySMTPOut.logger.Logf(LMAX, "Server (%s) didn't like our HELO command", toserver)
					}
				} else {
					// Can't connect to the server or didn't get a 220 response from greeting
					// Probably means the remote server isn't accepting the connection
					// due to spam worries.
					// For exampple this is what Yahoo says:
					// 553 5.7.1 [BL21] Connections will not be accepted from X.X.X.X,
					// because the ip is in Spamhaus's list; see http://postmaster.yahoo.com/550-bl23.html					
					// Send NDN
					mySMTPOut.logger.Logf(LMAX, "Bad greeting from server %s", toserver)
					if(mySMTPOut.createNDN(mailfrom, rcptto, fn822)) {
						os.Remove(fmt.Sprintf("%s/%s", OUTQUEUEDIR, fn822))
						os.Remove(fn821)
					}
				}
				con.Close()
			} else {
				mySMTPOut.logger.Logf(LMAX, "Couldn't connect to SMTP server %s", toserver)			
				return	
			}
		} else {
			// No MX records, create NDN or deadletter accordingly
			mySMTPOut.logger.Logf(LMED, "No MX records for: %s", rcptto)
			if(mySMTPOut.createNDN(mailfrom, rcptto, fn822)) {
				os.Remove(fmt.Sprintf("%s/%s", OUTQUEUEDIR, fn822))
				os.Remove(fn821)										
			}			
		}
	}
}

// TODO: Retry queue with delay so messages aren't retried every 5 seconds!
func (mySMTPOut *smtpOutStruct) route(fn822 string) {

	heloCmd, _ := regexp.Compile("^helo ");
	ehloCmd, _ := regexp.Compile("^ehlo ");
	mailfromCmd, _ := regexp.Compile("^mail from:");
	rcpttoCmd, _ := regexp.Compile("^rcpt to:");
	var helo string
	var ehlo string
	var mailfrom string
	var rcptto string

	fn821 := fmt.Sprintf("%s/%s1", OUTQUEUEDIR, fn822[0:len(fn822)-1])


	//
	// Open the .821 file and process the RCPT TO fields
	//
	fd, err := os.Open(fn821, os.O_RDONLY, 0666)

	if (err == nil) {
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
						mySMTPOut.logger.Logf(LMAX, "HELO field: %s", helo)						
						break
					case ehloCmd.Match(lineofbytes):
						ehlo = string(getDomainFromHELO(lineofbytes))
						mySMTPOut.logger.Logf(LMAX, "EHLO field: %s", ehlo)						
						break
					case mailfromCmd.Match(lineofbytes):
						mailfrom = string(getAddressFromMailFrom(lineofbytes))
						mySMTPOut.logger.Logf(LMAX, "MAIL FROM field: %s", mailfrom)						
						break
					case rcpttoCmd.Match(lineofbytes):
						rcptto = string(getAddressFromMailFrom(lineofbytes))
						mySMTPOut.logger.Logf(LMAX, "RCPT TO field: %s", rcptto)
						mySMTPOut.sendBySMTP(fn821, fn822, mailfrom, rcptto)
						break
					default:
						break
				}
			}
		}
	} else {
		mySMTPOut.logger.Logf(LMIN, "Can't open file: %s", fn821)		
	}


}

func (mySMTPOut *smtpOutStruct) startSMTPOut() {
	endingWith822, _ := regexp.Compile(".822$");
	
	for
	{
		// In nano seconds, 1 second = 1 000 000 000 nanoseconds
		time.Sleep(5000000000)
	
		dir, direrr := os.Open(OUTQUEUEDIR, 0, 0666)
		
		if(direrr == nil) {
			fi, err := dir.Readdir(-1)
			
			if(err == nil) {
				for i := 0; i < len(fi); i++ {
					if(endingWith822.Match([]byte(fi[i].Name))) {
						mySMTPOut.logger.Logf(LMAX, "Processing %s\n", fi[i].Name)
						mySMTPOut.route(fi[i].Name)
					}
				}
			}
			dir.Close()
		}
	}
}
