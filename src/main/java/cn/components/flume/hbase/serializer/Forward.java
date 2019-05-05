package cn.components.flume.hbase.serializer;

import cn.components.utils.BytesUtils;
import cn.components.utils.Utils;
import com.google.common.base.Charsets;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.conf.ComponentConfiguration;
import org.apache.flume.sink.hbase.AsyncHbaseEventSerializer;
import org.hbase.async.AtomicIncrementRequest;
import org.hbase.async.PutRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Forward implements AsyncHbaseEventSerializer {

    private static final Logger logger = LoggerFactory.getLogger(Forward.class);

    private byte[] table;
    private byte[] colFam;
    private Event currentEvent;
    private byte[] eventCountCol;
    private byte[] eventCountRow;

    @Override
    public void initialize(byte[] table, byte[] cf) {
        this.table = table;
        this.colFam = cf;
    }

    @Override
    public void setEvent(Event event) {
        // Set the event and verify that the rowKey is not present
        this.currentEvent = event;
    }

    @Override
    public List<PutRequest> getActions() {

        List<PutRequest> puts = new ArrayList<>();
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
                PutRequest req = new PutRequest(table, currentRowKey, colFam,
                        BytesUtils.getBytes(key), BytesUtils.getBytes(keyValues.get(key)));
                puts.add(req);
            }
        } catch (Exception e) {
            e.printStackTrace();
            return null;
        }
        return puts;
    }

    @Override
    public List<AtomicIncrementRequest> getIncrements() {
        List<AtomicIncrementRequest> actions = new ArrayList<>();
        //Increment the number of events received
        if (eventCountCol != null) {
            actions.add(new AtomicIncrementRequest(table, eventCountRow, colFam, eventCountCol));
        }
        return actions;
    }

    @Override
    public void cleanUp() {
        table = null;
        colFam = null;
        currentEvent = null;
    }

    @Override
    public void configure(Context context) {
        String iCol = context.getString("incrementColumn", "iCol");
        if (iCol != null && !iCol.isEmpty()) {
            eventCountCol = iCol.getBytes(Charsets.UTF_8);
        }
        eventCountRow = context.getString("incrementRow", "incRow").getBytes(Charsets.UTF_8);
    }

    @Override
    public void configure(ComponentConfiguration conf) {
    }
}
