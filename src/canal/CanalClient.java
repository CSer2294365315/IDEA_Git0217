package canal;

import com.alibaba.otter.canal.client.CanalConnector;
import com.alibaba.otter.canal.client.CanalConnectors;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.alibaba.otter.canal.protocol.Message;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

import java.net.InetSocketAddress;
import java.util.List;

public class CanalClient {

    public static void main(String[] args) {



        new Branch

confict
        /**

         * 主要任务
         * 用Cannal采集MySQL的增量数据
         * 把采集到的数据，存到Kafka
         */

        /**
         * 一、Canal的客户端，用于建立和Canal服务器的连接
         *
         * 如果Canal是集群模式的，就通过集群的方式连Canal服务器
         * 如果Canal服务端只有一台主机，那就用Single模式来连接。我们就用一个台服务器跑一个Canal，所以用Single模式的
         */
        CanalConnector canalConnector = CanalConnectors.newSingleConnector(
                new InetSocketAddress("hadoop1", 11111),
                "example",
                "",
                "");


        while (true) {

            /**
             * 连接Canal，抓取变化的数据，每次最多抓取100条SQL引起的变化
             */
            //连接Canal
            canalConnector.connect();
            //订阅所有库的所有表的变化：增删改
            canalConnector.subscribe("*.*");
            //抓取自上次抓取以来，数据库的所有变化，最多抓取100条SQL引起的变化，即使100条SQL引起了1万行的变化，也全部抓取过来
            Message message = canalConnector.get(100);

            /**
             * 从抓取到的所有数据库变化里面，过滤出来其中的新增的变化，然后存到Kafka里面
             *
             * 一个Message包含了本次抓取的所有变化。比如抓取了100条SQL引起的变化，这个Message里面就是有100个Entry
             *
             * 每个SQL引起的变化封装在一个Entry里面，这个SQL有很多种，可能是事务，事务不引起数据变化，但也会被抓取到，所以应该过滤出去，
             * 只选择引起行变化的SQL，也就是只保留ROWDATA类型的Entry
             *
             * 从Entry里面的数据需要取出来，然后进行解析，才是实际发生变化的行
             * 这个行里面不只包括了MySQL一行的数据，还封装了相关的信息，比如操作类型，是Insert，还是update。还有操作的那张表，表名是什么
             * 光知道变化后的行数据是不行的，比如我们这个需求是想要获取到订单表，新增的订单，
             * 那就需要把数据库发生的所有变化中，引起行变化的数据，而且操作的表是订单表，操作类型是新增的行数据给过滤出来，然后把消息写到Kafka里面去。
             *
             */
            if(message.getEntries().size() != 0) {
                //TODO  Message里面可以放100条SQL，每条SQL的执行结果，放在Message里面的一个Entry里面
                //TODO 一个Entry里面有很多行，都是这条SQL执行的结果
                for (CanalEntry.Entry entry : message.getEntries()) {
                    if (CanalEntry.EntryType.ROWDATA == entry.getEntryType()) {

                        ByteString storeValue = entry.getStoreValue();
                        //rowchange是结构化的,反序列化的sql单位
                        CanalEntry.RowChange rowChange = null;
                        try {
                            rowChange = CanalEntry.RowChange.parseFrom(storeValue);
                        } catch (InvalidProtocolBufferException e) {
                            e.printStackTrace();
                        }
                        //得到行集合


                        //TODO RowData是SQL里面的一行
                        //RowData是纯数据
                        // RowChange里面封装了操作类型和操作后的数据，也就是RowData
                        List<CanalEntry.RowData> rowDatasList = rowChange.getRowDatasList();
                        //得到操作类型
                        CanalEntry.EventType eventType = rowChange.getEventType();
                        // 得到表名
                        String tableName = entry.getHeader().getTableName();


                        //   4   根据不同业务类型发送到不同的kafka
                        //把在哪个表里面，进行了什么样的操作，产生了什么样的数据的信息，到到一个对象里面
                        CanalHandler canalHandler = new CanalHandler(eventType, tableName, rowDatasList);
                        //把设个对象里面的数据，转成JSON，存到Kafka里面去
                        canalHandler.handle();
                    }
                }
            }else if (message.getEntries().size() == 0) {
                System.out.println("没有数据，休息一会！");
                try {
                    Thread.sleep(5000);
                } catch (InterruptedException e) {
                    e.printStackTrace();
                }
            }
        }
    }
}



// 1个message对象 包含了100个什么？
// 100个SQL单位     1个SQL单位 = 一个sql执行后影响的row集合