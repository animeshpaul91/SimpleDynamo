# SimpleDynamo
## A simplified implementation of Dynamo DB - Amazon's massive key-value storage

This project aimed at the implementation of a simplified version of Dynamo DB which is Amazon's massive replicated key-value storage. In an android environment, there are five emulators which were used to store replicated data. The following functionalities were implemented in the project: 
1. Partitioning
2. Replication 
3. Failure Handling

There were basic CRUD operations that were implemented. These are the basic operations: 
1. Create (Insert Data)
2. Read which returns the most recent write (Query Data)
3. Update (Overwrite Existing Data)
4. Delete (Delete Data)

## Brief Description of Project

### Partitioning
Nodes (emulators) are laid along a circular ring based on their SHA-1 hash value. Every node is aware of its two next successors and its predecessor. Data is stored in nodes in redundant copies for failure handling. This allows for the access of data when a node fails. 

### Replication
Whenever data is inserted, the data gets inserted into the coordinator (node that is supposed to serve this data) and its next two successors. In this way, copies of data are redundantly stored all over the ring. Whenever an existing data is updated, the data is versioned (stored in multiple versions) across the node. This allows the latest versioned-value to be returned when a query occurrs later. 

### Failure Handling
There is a python script that performs a testing on the application. As this testing progresses, one emulator's instance gets aborted and later reinitialized. The test script performs queries on the data which it missed, while it was not alive. This node then fetches the data which its successors stored on behalf of it and returns the value with the highest/latest version. This is how the most recent data is returned. 

References: 
1. [Amazon Dynamo DB] (https://aws.amazon.com/dynamodb/)
2. [Project Description] (https://cse.buffalo.edu/~eblanton/course/cse586-2018-0s/materials/2018S/simple-dynamo.pdf)
