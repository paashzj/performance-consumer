package com.github.shoothzj.pf.consumer.pulsar;

import com.github.shoothzj.pf.consumer.common.AbstractPullThread;
import lombok.extern.slf4j.Slf4j;
import org.apache.pulsar.client.api.Consumer;
import org.apache.pulsar.client.api.Message;
import org.apache.pulsar.client.api.Messages;

import java.util.List;

/**
 * @author hezhangjian
 */
@Slf4j
public class PulsarPullThread extends AbstractPullThread {

    private final List<Consumer<byte[]>> consumers;

    private final PulsarConfig pulsarConfig;

    public PulsarPullThread(int i, List<Consumer<byte[]>> consumers, PulsarConfig pulsarConfig) {
        super(i);
        this.consumers = consumers;
        this.pulsarConfig = pulsarConfig;
    }

    @Override
    protected void pull() throws Exception {
        for (Consumer<byte[]> consumer : consumers) {
            if (pulsarConfig.consumeBatch) {
                final Messages<byte[]> messages = consumer.batchReceive();
                consumer.acknowledge(messages);
            } else {
                final Message<byte[]> message = consumer.receive();
                consumer.acknowledge(message);
            }
        }
    }

}
