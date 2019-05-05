package cn.components.flume.interceptor;

import cn.components.utils.BytesUtils;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.interceptor.Interceptor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.SimpleDateFormat;
import java.util.List;
import java.util.Map;

public class CustomSinkInterceptor implements Interceptor {

    private Logger logger = LoggerFactory.getLogger(CustomSinkInterceptor.class);

    @Override
    public void initialize() {

    }

    @Override
    public Event intercept(Event event) {
        Map<String, String> headers = event.getHeaders();
        JSONObject map = JSON.parseObject(BytesUtils.getString(event.getBody()));
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
        long currentTime = System.currentTimeMillis();
        String serverTime = sdf.format(currentTime);
        map.put("serverTime", serverTime);
        event.setBody(BytesUtils.getBytes(JSON.toJSONString(map)));
        event.setHeaders(headers);
        logger.debug("Header======>{}", JSON.toJSONString(event.getHeaders()));
        return event;
    }

    @Override
    public List<Event> intercept(List<Event> events) {
        for (Event event : events) {
            intercept(event);
        }
        return events;
    }

    @Override
    public void close() {

    }

    public static class Builder implements Interceptor.Builder {

        @Override
        public Interceptor build() {
            return new CustomSinkInterceptor();
        }

        @Override
        public void configure(Context context) {

        }

    }
}
