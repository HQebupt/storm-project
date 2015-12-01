Storm exmaple
===

# sequence-split-merge
* how to do rpc
* how to split one stream
* how to merge two stream

0.9.0 性能测试
===
这工程的代码文档，在[这里](https://github.com/alibaba/jstorm/wiki/stream-split-merge)

#内容
[参考文档](https://github.com/alibaba/jstorm/wiki/0.9.0-%E6%80%A7%E8%83%BD%E6%B5%8B%E8%AF%95)
[代码工程](https://github.com/longdafeng/storm-examples)

* 只看了`SequenceTopology`, 没看`DRPC`等代码。
* 关注一下TOPN的写法

JStorm 0.9.0 性能非常的好， 使用netty时单worker 发送最大速度为11万QPS， 使用zeromq时，最大速度为12万QPS.

> QPS: 吞吐量，每秒查询率(Query Per Second)


#结论
* JStorm 0.9.0 在使用Netty的情况下，比Storm 0.9.0 使用netty情况下，快10%， 并且JStorm netty是稳定的而Storm 的Netty是不稳定的
* 在使用ZeroMQ的情况下， JStorm 0.9.0 比Storm 0.9.0 快30%


#原因
* Zeromq 减少一次内存拷贝
* 增加反序列化线程
* 重写采样代码，大幅减少采样影响
* 优化ack代码
* 优化缓冲map性能
* Java 比clojure更底层


#编译运行
* mvn package
* start.sh

#注意
* pom.xml 里面的storm core依赖是0.7.1; 若换成storm-0.9.3以上版本的依赖，`conf.put(Config.TOPOLOGY_ACKERS, ackerParal);`代码会报错，接口有变化。
* 个人认为**JStorm**的接口是基于 *0.7.1*的。