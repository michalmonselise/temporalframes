### TemporalFrames

Temporal Graphs for Apache Spark

TemporalFrames can be one of two data structures:
 
#### TemporalFrame

This data structure contains a column in the edge table for each time unit. Time columns must start with `time_`

#### TemporalFrameSeq

This data structure contains a timestamp column which needs to be declared when defining a data structure. For example

`temp_graph = TemporalFrameSeq(vertices, edges, 'date')`
