# Building #

  1. Install Go
    * http://golang.org/doc/install.html
  1. Install git
    * http://git-scm.com/download
    * sudo apt-get install git-core
  1. Install gosqlite.googlecode.com/hg/sqlite
    * cd $GOROOT/src/pkg
    * mkdir gosqlite.googlecode.com/hg
    * hg clone http://gosqlite.googlecode.com/hg gosqlite.googlecode.com/hg
    * cd gosqlite.googlecode.com/hg/sqlite
    * make install
  1. Get Source for GoESMTP
  1. Run make