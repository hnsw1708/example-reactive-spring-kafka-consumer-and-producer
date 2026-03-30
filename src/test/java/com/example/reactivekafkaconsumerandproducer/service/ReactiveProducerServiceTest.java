package com.example.reactivekafkaconsumerandproducer.service;

import com.example.reactivekafkaconsumerandproducer.dto.FakeProducerDTO;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.reactive.ReactiveKafkaProducerTemplate;
import org.springframework.test.util.ReflectionTestUtils;
import reactor.core.publisher.Mono;
import reactor.kafka.sender.SenderResult;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReactiveProducerServiceTest {

    @Mock
    private ReactiveKafkaProducerTemplate<String, FakeProducerDTO> reactiveKafkaProducerTemplate;

    @InjectMocks
    private ReactiveProducerService reactiveProducerService;

    private static final String TEST_TOPIC = "test-fake-producer-topic";

    @BeforeEach
    void setUp() {
        ReflectionTestUtils.setField(reactiveProducerService, "topic", TEST_TOPIC);
    }

    // ---------------------------------------------------------------------------
    // Helper: build a mock SenderResult with a real RecordMetadata
    // ---------------------------------------------------------------------------
    @SuppressWarnings("unchecked")
    private SenderResult<Void> buildSenderResult(long offset) {
        RecordMetadata recordMetadata = new RecordMetadata(
                new TopicPartition(TEST_TOPIC, 0),
                offset, 0, System.currentTimeMillis(), 0, 0);
        SenderResult<Void> senderResult = mock(SenderResult.class);
        when(senderResult.recordMetadata()).thenReturn(recordMetadata);
        return senderResult;
    }

    private FakeProducerDTO createFakeProducerDTO() {
        return mock(FakeProducerDTO.class);
    }

    // ---------------------------------------------------------------------------
    // Tests
    // ---------------------------------------------------------------------------

    @Test
    @DisplayName("send() should call reactiveKafkaProducerTemplate.send with correct topic and DTO")
    void send_shouldInvokeTemplateWithCorrectTopicAndDto() {
        // Arrange
        var dto = createFakeProducerDTO();
        SenderResult<Void> senderResult = buildSenderResult(42L);
        when(reactiveKafkaProducerTemplate.send(eq(TEST_TOPIC), eq(dto)))
                .thenReturn(Mono.just(senderResult));

        // Act
        reactiveProducerService.send(dto);

        // Assert
        verify(reactiveKafkaProducerTemplate, times(1)).send(TEST_TOPIC, dto);
    }

    @Test
    @DisplayName("send() should complete without throwing even when DTO is null")
    void send_withNullDto_shouldNotThrow() {
        // Arrange
        SenderResult<Void> senderResult = buildSenderResult(0L);
        when(reactiveKafkaProducerTemplate.send(eq(TEST_TOPIC), (FakeProducerDTO) isNull()))
                .thenReturn(Mono.just(senderResult));

        // Act & Assert
        assertDoesNotThrow(() -> reactiveProducerService.send(null));
        verify(reactiveKafkaProducerTemplate, times(1)).send(eq(TEST_TOPIC), (FakeProducerDTO) isNull());
    }

    @Test
    @DisplayName("send() should still subscribe even when doOnSuccess logs the offset")
    void send_shouldSubscribeAndLogOffset() {
        // Arrange
        var dto = createFakeProducerDTO();
        SenderResult<Void> senderResult = buildSenderResult(100L);
        when(reactiveKafkaProducerTemplate.send(anyString(), any(FakeProducerDTO.class)))
                .thenReturn(Mono.just(senderResult));

        // Act
        assertDoesNotThrow(() -> reactiveProducerService.send(dto));

        // Assert: recordMetadata().offset() must have been reachable (no NPE / subscription happened)
        verify(senderResult, atLeastOnce()).recordMetadata();
    }

    @Test
    @DisplayName("send() should handle an empty Mono (no SenderResult emitted) without throwing")
    void send_withEmptyMono_shouldNotThrow() {
        // Arrange
        var dto = createFakeProducerDTO();
        when(reactiveKafkaProducerTemplate.send(anyString(), any(FakeProducerDTO.class)))
                .thenReturn(Mono.empty());

        // Act & Assert
        assertDoesNotThrow(() -> reactiveProducerService.send(dto));
        verify(reactiveKafkaProducerTemplate, times(1)).send(TEST_TOPIC, dto);
    }

    @Test
    @DisplayName("send() should not propagate errors when the Mono errors out")
    void send_withMonoError_shouldNotPropagateException() {
        // Arrange
        var dto = createFakeProducerDTO();
        when(reactiveKafkaProducerTemplate.send(anyString(), any(FakeProducerDTO.class)))
                .thenReturn(Mono.error(new RuntimeException("Kafka broker unavailable")));

        // Act & Assert — subscribe() swallows the error by default (logs it); must not bubble up
        assertDoesNotThrow(() -> reactiveProducerService.send(dto));
        verify(reactiveKafkaProducerTemplate, times(1)).send(TEST_TOPIC, dto);
    }

    @Test
    @DisplayName("send() should use the injected topic name, not a hard-coded value")
    void send_shouldUseInjectedTopicName() {
        // Arrange — override the topic to verify it is forwarded correctly
        String customTopic = "custom-override-topic";
        ReflectionTestUtils.setField(reactiveProducerService, "topic", customTopic);

        var dto = createFakeProducerDTO();
        SenderResult<Void> senderResult = buildSenderResult(7L);
        when(reactiveKafkaProducerTemplate.send(eq(customTopic), eq(dto)))
                .thenReturn(Mono.just(senderResult));

        // Act
        reactiveProducerService.send(dto);

        // Assert
        verify(reactiveKafkaProducerTemplate, times(1)).send(customTopic, dto);
        verify(reactiveKafkaProducerTemplate, never()).send(eq(TEST_TOPIC), any(FakeProducerDTO.class));
    }

    @Test
    @DisplayName("send() called multiple times should invoke the template the same number of times")
    void send_calledMultipleTimes_shouldInvokeTemplateEachTime() {
        // Arrange
        var dto1 = createFakeProducerDTO();
        var dto2 = createFakeProducerDTO();
        var dto3 = createFakeProducerDTO();

        SenderResult<Void> sr1 = buildSenderResult(1L);
        SenderResult<Void> sr2 = buildSenderResult(2L);
        SenderResult<Void> sr3 = buildSenderResult(3L);

        when(reactiveKafkaProducerTemplate.send(eq(TEST_TOPIC), eq(dto1))).thenReturn(Mono.just(sr1));
        when(reactiveKafkaProducerTemplate.send(eq(TEST_TOPIC), eq(dto2))).thenReturn(Mono.just(sr2));
        when(reactiveKafkaProducerTemplate.send(eq(TEST_TOPIC), eq(dto3))).thenReturn(Mono.just(sr3));

        // Act
        reactiveProducerService.send(dto1);
        reactiveProducerService.send(dto2);
        reactiveProducerService.send(dto3);

        // Assert
        verify(reactiveKafkaProducerTemplate, times(3)).send(eq(TEST_TOPIC), any(FakeProducerDTO.class));
    }
}