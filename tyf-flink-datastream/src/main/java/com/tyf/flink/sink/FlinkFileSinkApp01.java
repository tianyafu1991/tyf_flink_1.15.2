package com.tyf.flink.sink;

import com.tyf.flink.bean.Access;
import com.tyf.flink.source.TyfAccessSource;
import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.core.fs.Path;
import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;

import java.time.Duration;

/**
 * 写出到文件
 */
public class FlinkFileSinkApp01 {

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // 需要开启chk
        env.enableCheckpointing(5*1000L);

        DataStreamSource<Access> stream = env.addSource(new TyfAccessSource());

//        stream.print();

        StreamingFileSink fileSink = StreamingFileSink.forRowFormat(new Path("out"), new SimpleStringEncoder())
                .withRollingPolicy(
                        DefaultRollingPolicy
                                .builder()
                                .withRolloverInterval(Duration.ofSeconds(10 * 1000))  // 根据时间滚动
                                .withMaxPartSize(new MemorySize(5 * 1024 * 1024)) // 根据大小滚动
                                .withInactivityInterval(Duration.ofMinutes(3L)) // 根据空闲时间滚动
                                .build()
                )
                .build();

        stream.addSink(fileSink);

        env.execute("FlinkSinkApp");
    }
}
