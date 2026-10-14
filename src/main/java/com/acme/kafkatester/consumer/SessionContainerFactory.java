package com.acme.kafkatester.consumer;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import org.springframework.kafka.config.KafkaListenerContainerFactory;
import org.springframework.kafka.config.KafkaListenerEndpoint;
import org.springframework.kafka.core.DefaultKafkaConsumerFactory;
import org.springframework.kafka.listener.ContainerProperties;
import org.springframework.kafka.listener.KafkaMessageListenerContainer;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.kafka.support.TopicPartitionOffset;
import org.springframework.kafka.support.converter.MessageConverter;
import org.springframework.stereotype.Component;

/**
 * {@link KafkaListenerContainerFactory} used with
 * {@code KafkaListenerEndpointRegistry.registerListenerContainer(endpoint, factory, true)}: the
 * registry asks it for a container for a session {@link Endpoint}, which carries the listener and the
 * consumer properties (fresh group id, offset reset, security). The registry registers/starts the
 * container; teardown uses {@code unregisterListenerContainer(id)} + {@code stop()}.
 */
@Component
class SessionContainerFactory implements KafkaListenerContainerFactory<MessageListenerContainer> {

    /** Minimal programmatic endpoint: id = group id, one topic, a ready-made listener. */
    static final class Endpoint implements KafkaListenerEndpoint {

        private final String id;
        private final String topic;
        private final Map<String, Object> consumerProps;
        private final SessionRecordListener listener;

        Endpoint(String id, String topic, Map<String, Object> consumerProps, SessionRecordListener listener) {
            this.id = id;
            this.topic = topic;
            this.consumerProps = consumerProps;
            this.listener = listener;
        }

        SessionRecordListener listener() {
            return listener;
        }

        Map<String, Object> consumerProps() {
            return consumerProps;
        }

        @Override
        public String getId() {
            return id;
        }

        @Override
        public String getGroupId() {
            return id;
        }

        @Override
        public String getGroup() {
            return null;
        }

        @Override
        public Collection<String> getTopics() {
            return List.of(topic);
        }

        @Override
        public TopicPartitionOffset[] getTopicPartitionsToAssign() {
            return new TopicPartitionOffset[0];
        }

        @Override
        public Pattern getTopicPattern() {
            return null;
        }

        @Override
        public String getClientIdPrefix() {
            return id;
        }

        @Override
        public Integer getConcurrency() {
            return 1;
        }

        @Override
        public Boolean getAutoStartup() {
            return Boolean.TRUE;
        }

        @Override
        public void setupListenerContainer(MessageListenerContainer listenerContainer, MessageConverter converter) {
            listenerContainer.setupMessageListener(listener);
        }

        @Override
        public boolean isSplitIterables() {
            return false;
        }

        @Override
        public String getAckMode() {
            return null;
        }
    }

    @Override
    public MessageListenerContainer createListenerContainer(KafkaListenerEndpoint endpoint) {
        Endpoint e = (Endpoint) endpoint;
        ContainerProperties props = new ContainerProperties(e.getTopics().toArray(String[]::new));
        props.setGroupId(e.getGroupId());
        props.setClientId(e.getClientIdPrefix());
        // MANUAL + never acknowledging: throwaway groups must not leave committed offsets behind
        props.setAckMode(ContainerProperties.AckMode.MANUAL);
        props.setMessageListener(e.listener());
        KafkaMessageListenerContainer<String, byte[]> container =
                new KafkaMessageListenerContainer<>(new DefaultKafkaConsumerFactory<>(e.consumerProps()), props);
        container.setBeanName(e.getId());
        return container;
    }

    @Override
    public MessageListenerContainer createContainer(TopicPartitionOffset... topicPartitions) {
        throw new UnsupportedOperationException("Only endpoint-based creation is supported");
    }

    @Override
    public MessageListenerContainer createContainer(String... topics) {
        throw new UnsupportedOperationException("Only endpoint-based creation is supported");
    }

    @Override
    public MessageListenerContainer createContainer(Pattern topicPattern) {
        throw new UnsupportedOperationException("Only endpoint-based creation is supported");
    }
}
