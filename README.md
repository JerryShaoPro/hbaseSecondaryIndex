# hbaseSecondaryIndex说明
hbaseSecondaryIndex是一个hbase二级索引的实现，基于solr+hbase coprocessor框架为hbase提供索引表支持。

此工程可打包为一个hbase的插件，通用方便。

这篇文档的目的是为了向您介绍hbaseSecondaryIndex的基本使用，使得您可以使用它来弥补hbase无内建二级索引支持的问题。

如果你已经准备好了，那么就开始阅读吧。

## 打包工程
该工程使用maven来构建，因此使用"mvn package"即可完成打包。

## 部署jar包
将jar包放到hbase各个节点的/HBEE_HOME/lib/下，放置后需要重启hbase。

## 创建hbase索引表
示例：
create 'index_demo','fm1' <br><br>

disable 'index_demo' <br><br>

alter 'index_demo', METHOD=>'table_att', 'coprocessor'=>'file:${HBASE_HOME}/lib/hbase_second_indexer-0.0.1-SNAPSHOT.jar|com.syx.hbase.SecondaryIndexer|1073741823' <br><br>

alter 'index_demo', METHOD=>'table_att', CONFIG=>{'solr_server_url'=>'http://${ip}:${port}/solr', 'max_add_documents_count'=>'100', 'auto_add_commit_documents_interval'=>'60000', 'isIndexTable'=>'true', 'rowkeyColumns'=>'column1,column2,column3', 'rowkeySplitor'=>'&', 'indexColumns'=>'column4,column5,column6,column7,column8', 'solrCollection'=>'index_demo_collection'}

enable 'index_demo'
 
## 索引表查询
先查询solr索引，得到rowkey——>再查询hbase得到记录
例如：针对上面创建的index_demo和index_demo_collection，首先使用solr api查询index_demo_collection获取到rowkey字段后，再查询index_demo表即可得到hbase表的最终结果。

注：solr使用高效的倒排索引结构，查询速度很快，同时具有分布式特性sorlCloud，因此将它与hbase结合来解决海量数据的多条件复杂查询我觉得是个不错的选择。
