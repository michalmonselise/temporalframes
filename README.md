## TemporalFrames

Temporal Graphs for Apache Spark

### Defining TemporalFrames

TemporalFrames can be one of two data structures:
 
#### TemporalFrame

This data structure contains a column in the edge table for each time unit. Time columns must start with `time_`

We define a TemporalFrame as :

`val temp_graph = TemporalFrame(vertices, edges)`

#### TemporalFrameSeq

This data structure contains a timestamp column which needs to be declared when defining a data structure. For example

`val temp_graph_seq = TemporalFrameSeq(vertices, edges, 'date')`

We can convert from `TemporalFrameSeq` to `TemporalFrame` using the function `to_temporalframe`.

`val temp_graph_seq = temp_graph.to_temporalframe()`

### Network Measures

Currently, there are 3 functions defined in the package for computing network measures:

- Burstiness
- Temporal Correlation Coefficient
- Temporal Volatility
