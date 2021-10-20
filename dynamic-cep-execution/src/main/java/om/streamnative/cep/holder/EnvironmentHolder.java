package om.streamnative.cep.holder;

import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.api.common.time.Time;
import org.apache.flink.api.java.utils.ParameterTool;
import org.apache.flink.shaded.curator4.com.google.common.base.Preconditions;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import java.util.Map;
import java.util.concurrent.TimeUnit;

import static om.streamnative.cep.ExecutionDescriptor.*;

public interface EnvironmentHolder {

    default StreamExecutionEnvironment createExecutionEnv(Map<String, String> userConfig) {
        Preconditions.checkNotNull(userConfig, "use config can't be null");
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        // set time characteristic
        String timeCharacteristicStr = userConfig.computeIfAbsent(TIMECHARACTERISTIC, key -> TIMECHARACTERISTIC_PROCTIME);
        TimeCharacteristic timeCharacteristic = null;
        switch (timeCharacteristicStr.toLowerCase()) {
            case TIMECHARACTERISTIC_PROCTIME:
                timeCharacteristic = TimeCharacteristic.ProcessingTime;
                break;
            case TIMECHARACTERISTIC_EVENT_TIME:
                timeCharacteristic = TimeCharacteristic.EventTime;
                long waterMarkInterval = Long.parseLong(userConfig.getOrDefault(WATERMARK_INTERVAL,
                        String.valueOf(WATERMARK_INTERVAL_DEFAULT)));
                //set watermark interval
                env.getConfig().setAutoWatermarkInterval(waterMarkInterval);
                break;
            case TIMECHARACTERISTIC_INGESTION_TIME:
                timeCharacteristic = TimeCharacteristic.IngestionTime;
                break;
            default:
                throw new RuntimeException("unsupport time type " + timeCharacteristicStr);
        }
        env.setStreamTimeCharacteristic(timeCharacteristic);
        // checkpoint
        CheckpointConfig config = env.getCheckpointConfig();
        config.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.RETAIN_ON_CANCELLATION);
        String checkpointInterval = userConfig.getOrDefault(CHECKPOINT_INTERVAL,
                String.valueOf(CHECKPOINT_INTERVAL_DEFAULT));
        String checkpointTimeout = userConfig.getOrDefault(CHECKPOINT_TIMEOUT,
                String.valueOf(CHECKPOINT_TIMEOUT_DEFAULT));
        env.getCheckpointConfig().setCheckpointInterval(Long.parseLong(checkpointInterval));
        env.getCheckpointConfig().setCheckpointTimeout(Long.valueOf(checkpointTimeout));
        env.getCheckpointConfig().setCheckpointingMode(org.apache.flink.streaming.api.CheckpointingMode.EXACTLY_ONCE);
        env.getConfig().setGlobalJobParameters(ParameterTool.fromMap(userConfig));
        env.setRestartStrategy(
                RestartStrategies.fixedDelayRestart(
                        3, Time.of(10, TimeUnit.SECONDS)));
        return env;
    }
}
