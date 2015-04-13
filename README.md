GoESMTP is EXPERIMENTAL and is not yet ready for beta testing or for production deployment.

GoESMTP is a multi-node ESMTP server written on Go.

Designed from the ground up to work as a cluster, each node keeps an index of the available messages by using hashes which are distributed throughout the cluster.
