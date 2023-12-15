
## _Fastest Wins Strategy_ using Futures (recommended)
We recommend to use the `grpcmultiplex_fastestwins` implementation based on _Futures_.
You can take the example code from `stream_blocks_mainnet.rs`.

__Reconnects__ will handled transparently in inside the _Futures_.
Multiple _Futures_ are executed in parallel and the first result is returned.

No __guarantees__ are made about if the messages are continuous or not.

## _Fastest Wins Strategy_ using task/channel (experimental)
(see `grpcmultiplex_fastestwins_channels.rs`)

This implementation is built with _Tokio Tasks and Channels_ instead of futures.
It is simpler and more predictable than the _Futures_ implementation.
It uses a _push model_ rather than a _pull model_, i.e. the _task_ pushes the result to the _channel_ while the _Futures_ implementation pulls the result from the chain of _Futures_.

## _Perfect Seq Strategy_ (experimental)
(see `grpcmultiplex_perfectseq.rs`)

This strategy **guarantees** that the messages are **continuous** and in the **correct order**.
The _continuous_ property is defined by linking a _Block_ with previous _Block_ using `block.parent_slot` and `block.parent_hash`.
The **disadvantage** is that it will not produce messages if a _Block_ is missing.

It is limited to the commitment levels __confirmed__ and __finalized__. For __processed__ level there is no sequence due to the potential presence of forks with form a tree.
