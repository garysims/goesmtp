#!/bin/sh
# 		Copyright 2010 Gary Sims. All rights reserved.
# 		http://www.garysims.co.uk
#
#    	This file is part of GoESMTP.
#		http://code.google.com/p/goesmtp/
#		http://goesmtp.posterous.com/
#
#    	GoESMTP is free software: you can redistribute it and/or modify
#    	it under the terms of the GNU General Public License as published by
#    	the Free Software Foundation, either version 2 of the License, or
#    	(at your option) any later version.
#
#    	GoESMTP is distributed in the hope that it will be useful,
#   	but WITHOUT ANY WARRANTY; without even the implied warranty of
#   	MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#    	GNU General Public License for more details.
#
#    	You should have received a copy of the GNU General Public License
#    	along with GoESMTP.  If not, see <http://www.gnu.org/licenses/>.
# 		-------------------------------------------------------------------------
# 		Portions from http://bash.cyberciti.biz/misc-shell/read-local-ip-address/
# 		Copyright (c) 2005 nixCraft project <http://cyberciti.biz/fb/>
# 		This script is part of nixCraft shell script collection (NSSC)
# 		Visit http://bash.cyberciti.biz/ for more information.
# 		-------------------------------------------------------------------------
OS=`uname`
IO="" # store IP
case $OS in
   Linux) IP=`ifconfig  | grep 'inet addr:'| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $1}' | head -n 1`;;
   FreeBSD|OpenBSD) IP=`ifconfig  | grep -E 'inet.[0-9]' | grep -v '127.0.0.1' | awk '{ print $2}' | head -n 1` ;;
   SunOS) IP=`ifconfig -a | grep inet | grep -v '127.0.0.1' | awk '{ print $2}'  | head -n 1` ;;
   Darwin) IP=`ifconfig  | grep 'inet '| grep -v '127.0.0.1' | cut -d: -f2 | awk '{ print $2}' | head -n 1` ;;
   *) IP="Unknown";;
esac
echo "# goESMTP server configuration file"
echo "[cluster]"
echo "type=master"
echo "id=1"
echo "master=$IP"
echo "ip=$IP"
echo "key=secret"
echo ""
echo "[smtp]"
echo "domain=`hostname`"
echo ""
echo "[db]"
echo "type=MySQL"
echo "username=username"
echo "password=password"
echo "host=localhost"
echo "database=goesmtp"
echo "# End"
