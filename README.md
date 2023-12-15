
## _Fastest Wins Strategy_ using Futures (recommended)
We recommend to use the `grpcmultiplex_fastestwins` implementation based on _Futures_.
You can take the example code from `stream_blocks_mainnet.rs`.

__Reconnects__ will handled transparently in inside the _Futures_.
Multiple _Futures_ are executed in parallel and the first result is returned.

No __guarantees__ are made about if the messages are continuous or not.

