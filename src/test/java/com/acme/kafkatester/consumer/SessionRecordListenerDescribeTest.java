package com.acme.kafkatester.consumer;

import static org.assertj.core.api.Assertions.assertThat;

import org.junit.jupiter.api.Test;

class SessionRecordListenerDescribeTest {

    @Test
    void nullMessageYieldsClassNameOnly() {
        assertThat(SessionRecordListener.describe(new IllegalStateException())).isEqualTo("IllegalStateException");
        assertThat(SessionRecordListener.describe(new IllegalStateException("boom")))
                .isEqualTo("IllegalStateException: boom");
    }
}
