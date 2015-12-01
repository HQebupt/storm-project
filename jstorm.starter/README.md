#jstorm-starter overview
@author von gosling

Jstorm-starter contains a variety of examples of using JStorm. the primary aim of this project is to varify aone2 deploy process and compatible update.

入门的JStorm代码，学习一下代码组织结构和配置文件的写法，学习`SimpleTopology`的组织方式。


## 简介
* `SimpleSpout`: 数据的生产者，每隔60s产生1个随机数
* `SimpleBolt` : 数据的消费者,接受到消息并打印出日志
* 无 ACKER


## 编译运行
* `mvn clean package -DskipTests -U`
* `start.sh`

## 注意
* pom.xml里面关于jstorm的jar包都必须在alibaba自己的私服中获取。