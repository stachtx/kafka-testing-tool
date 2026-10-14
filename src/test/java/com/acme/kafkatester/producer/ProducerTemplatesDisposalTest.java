package com.acme.kafkatester.producer;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.Map;

import org.junit.jupiter.api.Test;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;

import com.acme.kafkatester.codec.MessageFormat;
import com.acme.kafkatester.producer.ProducerConfig.ProducerTemplates;

/** W4: producer factories are destroyed when the templates bean is destroyed. */
class ProducerTemplatesDisposalTest {

    @Test
    @SuppressWarnings("unchecked")
    void destroyClosesEveryProducerFactoryEvenIfOneFails() throws Exception {
        DefaultKafkaProducerFactory<String, Object> avroFactory = mock(DefaultKafkaProducerFactory.class);
        DefaultKafkaProducerFactory<String, Object> protoFactory = mock(DefaultKafkaProducerFactory.class);
        KafkaTemplate<String, Object> avro = mock(KafkaTemplate.class);
        KafkaTemplate<String, Object> proto = mock(KafkaTemplate.class);
        when(avro.getProducerFactory()).thenReturn(avroFactory);
        when(proto.getProducerFactory()).thenReturn(protoFactory);
        org.mockito.Mockito.doThrow(new IllegalStateException("boom")).when(avroFactory).destroy();

        new ProducerTemplates(Map.of(MessageFormat.AVRO, avro, MessageFormat.PROTOBUF, proto)).destroy();

        verify(avroFactory).destroy();
        verify(protoFactory).destroy();
    }
}
