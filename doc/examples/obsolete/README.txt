- plotChunksCopies.tcl: while dependent on output from obsolete code, is still the
  only way we have of visualizing the chunk layout. Plan on some small work to port
  it for the current infrastructure.

- rebalance.py: provides a chunk-to-node mapping. It should work fine on the new
  partitioner's output. It depends on loader.py. We might borrow code from this
  and the old loader in building the multi-node loader.

- loader.py: previous loader and rebalance.py dependency

