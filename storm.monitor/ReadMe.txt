时间：20141224
原因：区域时延占比增加一个条件,计算时将时延超过10秒的记录过滤掉.话单单位是ms

时间：20141219
原因：重新修改Wap、Soft时延占比，取消特殊页面的考虑。

时间：20141216
原因：修改Wap、Soft时延占比，增加一个划分区间。考虑特殊页面的考察办法。

时间：20141214
原因：新加2个指标：Wap、Soft时延占比

时间：20141117
原因：git版本管理和mvn依赖管理
解决：第三方jdbc无法从mvn中心参考导入。
有2种方法解决：
	①安装在私服
	②安装在本地：mvn install:install-file -DgroupId=oracle -DartifactId=ojdbc -Dversion=6 -Dpackaging=jar -Dfile=C:\Users\lenovo\Desktop\stormMonitor\StormMonitor\lib\ojdbc6.jar

时间：20141107
原因：修改客户端访问用户数指标算法。

时间：20141021
原因：soft_uv比较BI偏多14%
解决：
1.合并2个话单进行msisdn去重
2.修正terminal、uaname字段取数错误，字段位置不对


时间：20141018
原因：客户端PV和客户端访问用户数（含全网，分省，分地市）两个指标口径需要修改。
口径：
1、ues_pagevisit话单中access_type = 16 的记录和 interface中所有记录的记录；
2、过滤无效号码记录，msisdn字段不足7位。
3、过滤ues_pagevisit话单中terminal字段和interface话单中uaname字段前两位字符为MM 的记录。
 


时间：20140929
原因：修正3个指标
1.客户端访问用户数-全网 -- 据说是soft_plat_uv，没改这个指标。
2.付费用户数-全网 -- ord_uv
3.信息费-全网，涉及到  按次订购费用（ord_order_fee ） 和 包月费用 （ord_mopk_fee ）
新的规则：
1 订购表里如果RealInforFee数据为0 则去除该条数据
2 书券维表获取IsPay=0的Ticketid  获取到id后 与 订购表的ticketid 关联，关联上的去掉IsPay=0的数据。


时间：20140827
原因：新增的2个指标需求变动。
但需要对省份，[地市]，图书分类/产品包分类下的订购量排名，各取排名前50名，不足50名有多少取多少。
例如：
浙江省  图书 排名前50
浙江省  杂志 排名前50
浙江省  漫画 排名前50

时间：20140825
原因：新增2个指标，添加入BookProduct的topo中
说明：
1.添加了book和product的分省的指标排序，topN = 50
2.排序那块用到了内部类和hashMap、hashSet等。
3.更正了第2个指标传错了productInfo的路径
4.增加了log信息的输出，方便查找问题。

时间：20140808
原因：新增6个指标，涉及3个topo：OrderTopoV2、SoftTopo、TotalProPVUV
说明：在ProCityClean.java中读取省信息的split(,2)中有问题，在后续的split中会出现数组越界异常。因此改成了split(,-1)自己拆分和合并字符串，这样就无报错了。

时间：20140807
原因：目前告警显示以下几张表没有数据，请解决一下：
V_TOTAL_UV_PERIOD
V_CLIK_BKTP_BKCL
V_TOTAL
V_CLIK_PRO_BKTP_BKCL
说明：对应的指标是total_pv,total_uv,total_period_uv。共同点，insert 2位数据，在代码中注释掉// conn.close();修改后就可以成功了。
但是注意到，有的insert2个字段现网指标仍然没有注释掉这行在运行，如果出现问题，就给它注释掉，重启动。

时间：20140723
说明：添加了1小时统计一次的4个指标，统一了所有的指标用一个jar包启动。

时间：20140722
说明：代码重构完成。涉及的修改有
1.添加 startTopo.sh --启动Topology脚本，usage：./startTopo.sh xxxx.jar
2.添加 stopTopo.sh --停止Topology脚本，usage：./stopTopo.sh
3.添加 topoName --记录topology的名字，是入口的类名。
topoName的内容要和包main的类名相同，并和argsTopo.properties文件里面的topoName保持一致。
4.后续可修改的地方，看看并行度的管理是否需要配置文件。

时间：20140718
说明：代码重构。
大屏storm程序改进建议：
1、所有declare的域，字符串常量都用一个FName、StreamId类或者接口统一命名。
tips：应尽量避免直接在代码中使用字符串。虽说Tuple能在运行时高效地处理这些 局部变量，但在编译链接的时候，初始化带有字符串元素的代码并没有任何意 义。因此，我们还是建议使用静态变量定义代替这种直接使用字符串的方法
2、logger的方式替代syso。
3、改造本地和集群2种模式运行。
4、命令行提交参数过多，能否通过文件读取的方式获得，jar包里面有一个默认的文件，jar外面的可以覆盖。
5、中文注释或者不必要的注释都删掉，全部改成英文的。
6、每个java文件都应该有作者和日期。
7、并行度的设置是否应该放在配置文件里面。
main包里面的各个类需要修改。参考一下昊哥给发的storm项目。

时间：20140625
说明：全部修改了DB入库的方式，只要条数超过31条的，都采用了批量插入的方式。
删除了无用的类，规范修改了类名和成员变量名。
这次修改是为了修正6min计算结果偏少的问题，但是没有找到原因。想的是先修改成这样，在看看有没有问题。

时间：20140616
说明：oracle入库代码重构，图书产品指标。采用批量插入的方式。

时间：20140616
说明：客户要求将统一数据层storm计算周期调整至6分钟。
          修改周期需要修改util.CommonArgs.java 、spouts.SignalSortSpout.java、spouts.SignalSpout.java

时间:20140512
说明：不用oracleHive.properties这个表单，改用正式表。


时间:20140506
说明：修改周期时间为10min，对比数据。并用的是oracleHive.properties这个表单。
             修改了周期，注意清0时刻也是要修改了，比如周期10min，清0时刻为00:20

时间:20140421
说明：修改BookProduct获取维表的方式，不用再修改软连接了。

时间:20140410
说明：1.修改手机报用户过滤不成功。定位原因读取手机用户信息未成功。
     2.更改chaptervisit的2个指标的维表信息获取方式。自动获取维表信息。不在需要更改维表时间的软连接。

时间:20140408
说明：1.Order修改包月信息费和点播信息费算法和BI保持一致
     2.Order修改付费用户数
     

时间：20140404
说明：这个版本是为了添加5个话单的分省PV和UV。
 计算周期:15分钟
更新：1.2个指标完成：total_pro_uv,total_pro_uv


时间：20140331
说明：这个版本是为了添加 分省的chaptervisit.计算周期15分钟的。
更新：1.TotalUVClean.java bug：修订substring(0,1)数组越界。
      2.数据库迁移 到如下地址
tns：

JKDP_DB =
  (DESCRIPTION =
    (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.183)(PORT = 1521))
    (ADDRESS = (PROTOCOL = TCP)(HOST = 192.168.3.185)(PORT = 1521))
    (LOAD_BALANCE = yes)
    (CONNECT_DATA =
      (SERVER = DEDICATED)
      (SERVICE_NAME = service_ora)
    )
  )
      
