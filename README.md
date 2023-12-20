

## Solana Geyser gRPC Multiplexing and Reconnect
This project provides multiplexing of multiple [Yellowstone gRPC](https://github.com/rpcpool/yellowstone-grpc) subscriptions based on _Fastest Wins Strategy_.

* Multiple _Futures_ get **merged** where the first next block that arrives will be emitted.
* __Reconnects__ are handled transparently inside the _Futures_.
* No __guarantees__ are made about if the messages are continuous or not.

Disclaimer: The library is designed with the needs of
[LiteRPC](https://github.com/blockworks-foundation/lite-rpc) in mind
yet might be useful for other projects as well.

The implementation is based on _Rust Futures_.

Please open an issue if you have any questions or suggestions ->  [New Issue](https://github.com/blockworks-foundation/geyser-grpc-connector/issues/new).

## Usage
An example how to use the library is provided in `stream_blocks_mainnet.rs`.

## Known issues
* Library does not support other data than Blocks very well.
* Library needs messages to be in order and provide slot information to work properly.
* The gRPC filtering options are very limited.

