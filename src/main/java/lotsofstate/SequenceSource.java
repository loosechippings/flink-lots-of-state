package lotsofstate;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.source.SourceFunction;

public class SequenceSource implements SourceFunction<Long>, CheckpointedFunction {
    private boolean running = true;
    private long sleepTime;
    private Long sequence = 0L;
    private transient ListState<Long> sequenceState;

    public SequenceSource(long sleepTime) {
        this.sleepTime = sleepTime;
    }

    @Override
    public void run(SourceContext<Long> ctx) throws Exception {
        while(running) {
            Thread.sleep(sleepTime);
            synchronized (ctx.getCheckpointLock()) {
                ctx.collect(sequence);
                sequence = sequence + 1;
            }
        }
    }

    @Override
    public void cancel() {
        running = false;
    }

    @Override
    public void snapshotState(FunctionSnapshotContext context) throws Exception {
        this.sequenceState.clear();
        this.sequenceState.add(sequence);
    }

    @Override
    public void initializeState(FunctionInitializationContext context) throws Exception {
        sequenceState = context.getOperatorStateStore().getListState(new ListStateDescriptor<>("sequence", Long.class));
        if (context.isRestored()) {
            for (Long s:sequenceState.get()) {
                sequence = s;
            }
        }
    }
}
