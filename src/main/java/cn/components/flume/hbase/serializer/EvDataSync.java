package cn.components.flume.hbase.serializer;

import cn.components.utils.BytesUtils;
import cn.components.utils.Utils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.HbaseEventSerializer;
import org.apache.hadoop.hbase.client.Increment;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class EvDataSync implements HbaseEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(EvDataSync.class);

    private byte[] colFam;
    private Event currentEvent;

    @Override
    public void initialize(Event event, byte[] columnFamily) {
        this.currentEvent = event;
        this.colFam = columnFamily;
    }

    @Override
    public List<Row> getActions() {
        List<Row> actions = new ArrayList<>();
        String message = BytesUtils.getString(currentEvent.getBody());
        try {
            // Split the event body and get the values for the columns
            logger.debug("recv message: " + message);
            JSONObject keyValues = JSON.parseObject(message);
            String vin = keyValues.getOrDefault("vin", "").toString();
            String terminalTime = keyValues.getOrDefault("terminalTime", "").toString();
            long recvTime = System.currentTimeMillis();
            keyValues.put("recvTime", String.valueOf(recvTime));

            long timestamp = Utils.convertToTimestamp(terminalTime);

            String prefix = Integer.toString(Math.abs(vin.hashCode() % 7));
            byte[] currentRowKey = BytesUtils.getBytes(prefix + "_" + vin + "_" + timestamp);
            for (String key : keyValues.keySet()) {
                Put put = new Put(currentRowKey);
                put.addColumn(colFam, BytesUtils.getBytes(key), BytesUtils.getBytes(keyValues.get(key).toString()));
                actions.add(put);
            }
        } catch (Exception e) {
            logger.error("Error in message format!===>{}", message);
            e.printStackTrace();
            return null;
        }
        return actions;
    }

    @Override
    public List<Increment> getIncrements() {
        return new ArrayList<>();
    }

    @Override
    public void close() {

    }

    @Override
    public void configure(Context context) {

    }

    @Override
    public void configure(ComponentConfiguration conf) {

    }
}
