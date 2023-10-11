## DADTKV

The goal of this project is to design and implement DADTKV, a distributed transactional key-value store to manage data objects (each data object being a <key, value>
pair) that reside in server memory and can be accessed concurrently by transactional
programs that execute in different machines.

### State

- The Client is just sending a simple request to the Transaction Manager.

- The Transaction Managers are receiving the transactions from the clients, checking if they have necessary leases, and making lease requests if necessary. The Lease Managers contact the TM to inform about new agreed leases.

- The Paxos is correctly implemented and functional.

- File configuration not yet used!