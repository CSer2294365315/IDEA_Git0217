package canal;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.otter.canal.protocol.CanalEntry;
import com.atguigu.gmall0826.common.constant.GmallConstant;

import java.util.List;

public class CanalHandler {

    CanalEntry.EventType eventType;

    String tableName;

    List<CanalEntry.RowData> rowDataList;

    //TODO 把一句SQL产生的变化的数据，封装为一个CannalHandler对象，这个类，封装了把变化的行，给保存到Kafka的代码
    public CanalHandler(CanalEntry.EventType eventType, String tableName, List<CanalEntry.RowData> rowDataList) {
        this.eventType = eventType;
        this.tableName = tableName;
        this.rowDataList = rowDataList;
    }

    public void handle() {
        if (this.rowDataList != null && this.rowDataList.size() > 0) {
            if (tableName.equals("order_info") &&
                    eventType == CanalEntry.EventType.INSERT) {
                //TODO 把这一句SQL产生的变化的行，一行一个JSON，给发送到Kafka里面去
                sendKafka(rowDataList, GmallConstant.KAFKA_TOPIC_ORDER);
            }
            //单据变更
            //用户新增。。
        }
    }

    public void sendKafka(List<CanalEntry.RowData> rowDataList, String topic) {
        //TODO 处理一条SQL的处理结果的所有行，把每一行的数据，封装成一个JSON字符串，然后保存到Kafka订单topic里面去
        for (CanalEntry.RowData rowData : rowDataList) {

            //TODO 获取每一行的，变化之后的值，Column对象里面封装了列名和列值
            List<CanalEntry.Column> afterColumnsList = rowData.getAfterColumnsList();

            //TODO 把一列数据，封装成JSON串，key为列名，value为列值，这样一个JSON串就是一行，具体地说，是新增的订单
            JSONObject jsonObject = new JSONObject();
            for (CanalEntry.Column column : afterColumnsList) {
//                System.out.println(column.getName() + "::" + column.getValue());
                jsonObject.put(column.getName(), column.getValue());
            }
            String jsonString = jsonObject.toJSONString();

            //TODO 把每一行数据封装成的JSON串，发送到Kafka里面去
            KafkaSender.send(GmallConstant.KAFKA_TOPIC_ORDER, jsonString);
        }
    }
}
