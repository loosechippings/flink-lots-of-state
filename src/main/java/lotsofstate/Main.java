package lotsofstate;


import org.apache.flink.api.common.restartstrategy.RestartStrategies;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

public class Main {

    private static final Long NUMBER_OF_KEYS = 1000L;
    private static final int STATE_SIZE_BYTES = 100000;

    public static void main(String[] args) throws Exception {
        StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setRestartStrategy(RestartStrategies.fixedDelayRestart(10, 500L));
        env.enableCheckpointing(2000L);
        SequenceSource source = new SequenceSource(50);
        env.addSource(source)
                .map(new RandomlyFailingMap())
                .map(i -> i % NUMBER_OF_KEYS)
                .keyBy(i -> i)
                .map(new LotsOfStateMap(STATE_SIZE_BYTES))
                .print();
        env.execute();
    }
}
