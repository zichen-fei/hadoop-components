package cn.components.flume.hbase.serializer;

import cn.components.utils.BytesUtils;
import cn.components.utils.Utils;
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
import java.util.Map;

public class ForwardSync implements HbaseEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(ForwardSync.class);

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
            Map<String, String> keyValues = Utils.processMessageToMap(message);
            String vid = keyValues.remove("VID");
            String time = keyValues.remove("SENDTIME");
            long timestamp = time == null ? 0L : Utils.convertToUTC(time);

            String recvTime = keyValues.getOrDefault("RECVTIME", "");
            String packetTime = keyValues.getOrDefault("PACKETTIME", "");
            keyValues.put("RECVTIME", String.valueOf(Utils.convertToUTC(recvTime)));
            keyValues.put("PACKETTIME", String.valueOf(Utils.convertToUTC(packetTime)));


            String prefix = Integer.toString(Math.abs(vid.hashCode() % 7));
            byte[] currentRowKey = BytesUtils.getBytes(prefix + "_" + vid + "_" + timestamp);
            for (String key : keyValues.keySet()) {
                //Generate a PutRequest for each column.
                Put put = new Put(currentRowKey);
                put.addColumn(colFam, BytesUtils.getBytes(key), BytesUtils.getBytes(keyValues.get(key)));
                actions.add(put);
            }
        } catch (Exception e) {
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
