package lotsofstate;

import org.apache.flink.api.common.functions.MapFunction;

import static java.lang.Math.random;

public class RandomlyFailingMap implements MapFunction<Long, Long> {
    @Override
    public Long map(Long value) throws Exception {
        if (random() > 0.99) {
            System.out.println("FAILED");
            throw new RuntimeException("Randomly failed");
        }
        return value;
    }
}
