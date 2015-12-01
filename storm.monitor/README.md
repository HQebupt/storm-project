大屏时延项目StormMonitor
============
# 编译打包
`mvn assembly:assembly`

# 集群运行
`storm jar xxx.jar main.$ClassName &`

> 注意: 由于依赖*oracle*的jar包,所以要把它进行打包,才可以运行.
> 集群运行是不需要*storm-core*核心包的.