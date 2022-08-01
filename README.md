# Summer-Project-with-Dr-Mendes

--commands--
root - creates a new root -> all the requests for lease arrives here
getlease - inserts a key and entry after finding the peer responsible for the block
migrate - migrates a block to the closest peer -> currently working on this part/planning to implement load balancing using gossipsub


--command receiver/network event--
leaseRequest - if the peer contains the appropriate leaf block, the peer adds the entry/ otherwise, transmits the request to other peer who 
might be responsible for the entry
leaeseRequests - contains a vector of lease requests that are to be dumped on the peer that received the migrated block
migrateRequest - accepts a block from other peer
insertOnRemoteParent - if the peer is the parent of another block that split, peer adds the child block id


