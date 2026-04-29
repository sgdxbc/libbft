This is an unordered, unstructured collection of notes, acts as the long-term memory, of me, not agents.
Maybe can also be beneficial to agents later, but currently human (me) is doing all the coding.
This is nothing like document for public.
May be useful later for being the knowledge base where serious documents are "extracted" from.
The first thing I want to note down is that (me) should not write those documents.
Based on past experiences, whenever I start to create some serious documents for my researching codebase, my coding progress diminishes and the project begins to be abandoned.

**About the crates.**
Code is grouped into multiple crates based on dependencies and features, not functions.
Currently I create four crates, one with all application-level dependencies (full profile Tokio, OTLP and Prometheus exporter, etc), one with TCP dependencies, one with cryptography dependencies, and the core `libbft` crate for all the other code without these kinds of dependencies.
The code is in `libbft` crate is not because it is "protocol implementation" code or what, just because it does not have special dependencies, and I want to put as many code as possible into `libbft` so readers do not need to frequently track call stacks across crates.
This is also why crate names should somewhat reflect their dependencies instead of their function scopes.
Currently I have not deployed any feature gate yet, but if I do that, I may also group the code affected by a feature gate into a dedicated crate.

**The actor interfaces.**
Currently we set up a event-consuming actor by calling `register` on it with all producers (`impl Emit<SomeEvent>`) it requires.
This method name should be interpreted as the consumer _register_ itself to those producers, which is not a good name because it took me some time to remember the registration direction.
`subscribe` could be better: it would look like `consumer.subscribe(producer1, producer2, ...)`.
I almost go for it but eventually concern that _subscribe_ implies multiple subscriber fan-out model, which is not true in this codebase as the event producer can only produce to one consumer (so every producer has only one consumer registered).
Maybe just go with `connect`?

In actor model, actors usually send and receive _messages_ instead of _events_.
The term _event_ also leads to the pubsub confusion above and is probably not the best choice.
The primary rationale is to avoid using the word _message_, which is intended for the peer messages of the protocol implementations.

**Core protocols abstract cryptography away.**
This design looks like a legitimate performance measurement, if considering the microsecond latency protocols (which happens to be the niche I started to write code for BFT protocols).
However, in geological distributed deployments protocols take half seconds for each message step, and perform a 40us cryptographic operations on path seems to be sane.
The other argument for externalize cryptography would be better parallelism resource utilization, which is also defeated in the case of 100 node deployment each with 2 vCPUs.
Protocol thread/IO thread separation can already eat all parallelism.
At the same time, in some protocols the cryptographic operations are logically on path, such as combining threshold signatures, making externalizing cryptography a rather awkward, RPC form of code.

In this codebase, we do prefer exclude cryptography from core protocols, but only when doing this simplifying implementations instead of complicating them.
For example in HotStuff, we abstract away quorum certificate construction as an asynchronous step because we have the logic handling asynchronous remote quorum certificates anyway.
There's no different between handling an asynchronous quorum certificate produced locally or remotely, so we can reuse the logic.
Meanwhile, producing a block (during `onPropose` of the original paper) is partially synchronous: the block digest is synchronously returned, while the generic message is asynchronously fed into a callback.
The block digest is used to synchronously update protocol state `block_leaf`, which is crucial for correct leader behavior.
It is possible to delay updating state to receiving the loopback generic message, but then we have to do bookkeeping about we are pending the digest of the actual leaf block and the protocol is actually in an inconsistent state, which will effectively _block_ the following proposal just as the synchronous code would do, so we certainly should go with synchronous digest computing.
On the other hand, the original paper explicitly states that handling generic message has no difference either on leader itself or on other nodes, so we just go with the already existing generic message callback.

**Actor shutdown.**
The actors in this codebase are bare metal as in https://ryhl.io/blog/actors-with-tokio/, but the management is a bit different (probably more complicated).
Most significantly, the actors hold senders for their accepted event streams, and their shutdown is not implicitly signaled by channel(s) closing but an explicit cancellation token.
This is because in our codebase, cyclic workflow is everywhere on every level.
Egress workers loop back messages to the protocol actors.
Peers are interconnected in full mesh, and no connection will ever be shut down if the peers only shut down connections when someone else shut down earlier.
The article does mention a strategy for cyclic cases, but I don't think that is simpler (or clearer) than mine.

**Serialization solution.**
This codebase takes an uncommon approach that serialization is tightly coupled with cryptographic operations, for example, there is usually a single _egress_ method that does both encoding and signing signatures, instead of separated methods and workers.
This is because this codebase uses Borsh-based digest solution, first encoding structured data into deterministic bytes and then digesting it, instead of directly digesting structured data.
The rationale is that Rust does not have a widely-adopted solution for deriving digest hashing implementations.
Also, blockchain protocols often deal with large batches of transactions, and single traversal over transaction batches for encoding + digesting over encoded bytes should be more efficient than dual traversals over transaction batches for digesting and encoding.
Lastly, some candidate cryptography solutions only expose interfaces for hashing arbitrary-length byte sequences and digest internally to encapsulate complexity of various digest lengths.
In the end, the signing methods usually have the encoded bytes available as well and just takes both functions.
After all, in such a researching codebase, shorter and less complicated pipelines are somewhat considered as a good thing.

**Lossless atomic broadcast protocols.**
The _lossless delivery_ means all submitted transactions are eventually delivered.
Certain protocols may have further rules on how the transactions are submitted, e.g. in HotStuff each transaction should be submitted on every replica.
Normally this requirement is trivial for leader-based protocols, and should be easily feasible for leaderless protocols with a bit careful treatment.
Ensuring lossless delivery makes the protocols much more useful to close loop workloads and even specific open loop ones like UTXO transfers.