# nifi-ftps
Standalone NAR with FTPS support (while waiting for support in base NIFI)

## Why this extension?

FTPS support has started [some time ago](https://issues.apache.org/jira/browse/NIFI-2188), but was then moved to [a different idea
](https://issues.apache.org/jira/browse/NIFI-2278).

Since I cannot wait on it : I took the FTP part of the base NIFI and added FTPS support. Since verification is important, also
added some host verification

## Installation
Since the code only injects an FTPSClient iso an FTPClient, we use all the logic of the FTP classes. This means that there is a very large
dependency on the standard processors NAR.

However, using the construct of adding a NAR dependency, this lead to IllegalAccessExceptions when loading the FTPS processors.

So I decided to inject the additional JAR into the standard processor NAR. This way, the JAR feels right at home :-)

Installation is therefore building the "patched" standard processor NAR and replacing the original one by this one.

## Current state
Written with NIFI 1.11.4 (you should be able to get it working on other versions as well by changing the dependencies, changed
classes will not have changed a lot). 

To allow self-signed certificates, you must specify the option in the processor. 
Support for checking trusted certificates implemented against the CA certificates in the JRE keystore (so if you have any self-signed CA certificates,
install them there)

Tested with FileZilla and with [test.rebex.net](https://test.rebex.net/).

**Note** : it forces the FTPS "PROT P" option (encrypted transfer) over TLS (no more SSL) , but it does not support the "TLS session resumption" option

**Note 2** : since I have no proxy to test with, the proxy options are disabled

**Note 3** : there is no exactness check on self-signed certificates, only that you allow self-signed and that the CN matches.
