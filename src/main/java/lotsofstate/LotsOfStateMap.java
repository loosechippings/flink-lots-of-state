package lotsofstate;

import org.apache.flink.api.common.functions.RichMapFunction;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.configuration.Configuration;

import java.nio.charset.StandardCharsets;
import java.util.Random;

public class LotsOfStateMap extends RichMapFunction<Long, Long> {

    private int stateSizeInBytes;
    private transient MapState<Long, String> mapState;

    public LotsOfStateMap(int stateSizeInBytes) {
        this.stateSizeInBytes = stateSizeInBytes;
    }

    @Override
    public void open(Configuration config) {
        MapStateDescriptor<Long, String> mapStateDescriptor = new MapStateDescriptor<>(
                "map-state",
                Long.class,
                String.class
        );
        mapState = getRuntimeContext().getMapState(mapStateDescriptor);
    }

    @Override
    public Long map(Long value) throws Exception {
        byte[] array = new byte[stateSizeInBytes];
        new Random().nextBytes(array);
        mapState.put(value, new String(array, StandardCharsets.UTF_8));
        return value;
    }
}
