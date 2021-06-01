# DART: A Scalable and Adaptive Edge Stream Processing Engine

Many Internet of Things (IoT) applications are time-critical and dynamically changing. However, traditional data processing systems (e.g., stream processing systems, cloud-based IoT data processing systems, wide-area data analytics systems) are not well-suited for these IoT applications. These systems often do not scale well with a large number of concurrently running IoT applications, do not support low-latency processing under limited computing resources, and do not adapt to the level of heterogeneity and dynamicity commonly present at edge environments. This suggests a need for a new edge stream processing system that advances the stream processing paradigm to achieve efficiency and flexibility under the constraints presented by edge computing architectures.

We present Dart, a scalable and adaptive edge stream processing engine that enables fast processing of a large number of concurrent running IoT applications’ queries in dynamic edge environments. The novelty of our work is the introduction of a dynamic dataflow abstraction by leveraging distributed hash table (DHT) based peer-to-peer (P2P) overlay networks, which can automatically place, chain, and scale stream operators to reduce query latency, adapt to edge dynamics, and recover from failures. 

Below jar parckages are required to perform the routing and failover, where are based on [free pastry](https://www.freepastry.org/). 

Failure recovery is configrable based on requirments. The experiments were performed based on stream processing engine of [Apache Flume](https://github.com/apache/flume). 

FreePastry-2.1.jar

JavaReedSolomon-master.main.jar

mongo-java-driver-3.11.0.jar

xmlpull_1_1_3_4a.jar

xpp3-1.1.1.3.4d_b2.jar

DEBS 2015 Grand Challenge of Distributed Event input data can be accessed from this [respository](https://github.com/komushi/grand-challenge-debs2015). 

To perform a scheduler routing request a sample command is as below:

```java
Usage: 
java [-cp FreePastry-<version>.jar] forwarding.DistTutorial localbindport bootIP bootPort numNodes
example java DistTutorial 9001 localhost 9001 10000
```
