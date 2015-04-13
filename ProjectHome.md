GoESMTP is EXPERIMENTAL and is not yet ready for beta testing or for production deployment. Please read the [status](http://code.google.com/p/goesmtp/wiki/Status) page for the latest information on stability and functionality. Also please check the current [issues](http://code.google.com/p/goesmtp/issues/list).

GoESMTP is a multi-node ESMTP server written on Go. Designed from the ground up to work as a cluster, each node keeps an index of the available messages by using hashes which are distributed throughout the cluster.

For more information and details on how to build, install and configure GoESMTP see the [Wiki](http://code.google.com/p/goesmtp/wiki/Home)