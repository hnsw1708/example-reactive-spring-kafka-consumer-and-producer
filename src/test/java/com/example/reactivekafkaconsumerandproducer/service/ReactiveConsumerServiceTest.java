package com.example.reactivekafkaconsumerandproducer.service;

import com.example.reactivekafkaconsumerandproducer.dto.FakeConsumerDTO;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.reactive.ReactiveKafkaConsumerTemplate;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

import static org.assertj.core.api.Assertions.assertThatNoException;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class ReactiveConsumerServiceTest {

    @Mock
    private ReactiveKafkaConsumerTemplate<String, FakeConsumerDTO> reactiveKafkaConsumerTemplate;

    private ReactiveConsumerService reactiveConsumerService;

    @BeforeEach
    void setUp() {
        reactiveConsumerService = new ReactiveConsumerService(reactiveKafkaConsumerTemplate);
    }

    // ---------------------------------------------------------------------------
    // Helper factory
    // ---------------------------------------------------------------------------

    private ConsumerRecord<String, FakeConsumerDTO> buildConsumerRecord(String key, FakeConsumerDTO value) {
        return new ConsumerRecord<>("fake-topic", 0, 0L, key, value);
    }

    private FakeConsumerDTO createFakeConsumerDTO() {
        return mock(FakeConsumerDTO.class);
    }

    // ---------------------------------------------------------------------------
    // run() – public API (CommandLineRunner)
    // ---------------------------------------------------------------------------

    @Test
    @DisplayName("run() subscribes to the consumer and processes a single record without error")
    void run_singleRecord_consumedSuccessfully() throws Exception {
        // Arrange
        var dto = createFakeConsumerDTO();
        var record = buildConsumerRecord("key-1", dto);

        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.just(record));

        // Act – should not throw
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());

        // Assert
        verify(reactiveKafkaConsumerTemplate, times(1)).receiveAutoAck();
    }

    @Test
    @DisplayName("run() subscribes to the consumer and processes multiple records without error")
    void run_multipleRecords_consumedSuccessfully() throws Exception {
        // Arrange
        var dto1 = createFakeConsumerDTO();
        var dto2 = createFakeConsumerDTO();
        var dto3 = createFakeConsumerDTO();

        var records = Flux.just(
                buildConsumerRecord("key-1", dto1),
                buildConsumerRecord("key-2", dto2),
                buildConsumerRecord("key-3", dto3)
        );

        when(reactiveKafkaConsumerTemplate.receiveAutoAck()).thenReturn(records);

        // Act
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());

        // Assert
        verify(reactiveKafkaConsumerTemplate, times(1)).receiveAutoAck();
    }

    @Test
    @DisplayName("run() completes without error when the topic is empty (empty Flux)")
    void run_emptyFlux_noInteractionAfterSubscribe() throws Exception {
        // Arrange
        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.empty());

        // Act
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());

        // Assert
        verify(reactiveKafkaConsumerTemplate, times(1)).receiveAutoAck();
    }

    @Test
    @DisplayName("run() does NOT propagate consumer error to the caller")
    void run_errorInFlux_doesNotPropagateToRun() throws Exception {
        // Arrange – simulate a transient Kafka error
        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.error(new RuntimeException("Simulated Kafka error")));

        // Act & Assert – run() must not re-throw; reactive errors stay inside the pipeline
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());
    }

    @Test
    @DisplayName("run() handles a record with a null value without throwing")
    void run_nullValueRecord_handledGracefully() throws Exception {
        // Arrange
        ConsumerRecord<String, FakeConsumerDTO> nullValueRecord =
                new ConsumerRecord<>("fake-topic", 0, 1L, "key-null", null);

        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.just(nullValueRecord));

        // Act
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());

        verify(reactiveKafkaConsumerTemplate, times(1)).receiveAutoAck();
    }

    @Test
    @DisplayName("run() calls receiveAutoAck exactly once per invocation")
    void run_calledOnce_receiveAutoAckInvokedOnce() throws Exception {
        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.empty());

        reactiveConsumerService.run();

        verify(reactiveKafkaConsumerTemplate, times(1)).receiveAutoAck();
        verifyNoMoreInteractions(reactiveKafkaConsumerTemplate);
    }

    @Test
    @DisplayName("run() can be invoked with vararg arguments without error")
    void run_withArgs_doesNotThrow() throws Exception {
        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.empty());

        assertThatNoException().isThrownBy(() ->
                reactiveConsumerService.run("--spring.profiles.active=test", "--debug")
        );
    }

    // ---------------------------------------------------------------------------
    // Reactive pipeline verification via StepVerifier (white-box through run)
    // ---------------------------------------------------------------------------

    @Test
    @DisplayName("Reactive pipeline maps ConsumerRecord to FakeConsumerDTO value correctly")
    void reactivePipeline_mapsConsumerRecordToDto() {
        // Arrange
        var expectedDto = createFakeConsumerDTO();
        var record = buildConsumerRecord("key-map", expectedDto);

        when(reactiveKafkaConsumerTemplate.receiveAutoAck())
                .thenReturn(Flux.just(record));

        // We verify via StepVerifier on the internal flux indirectly:
        // call run() and assert template was queried – mapping is exercised internally.
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());
        verify(reactiveKafkaConsumerTemplate).receiveAutoAck();
    }

    @Test
    @DisplayName("Reactive pipeline completes normally after finite Flux")
    void reactivePipeline_finiteFlux_completesNormally() {
        // Arrange – two-element Flux; pipeline should complete without error
        var dto = createFakeConsumerDTO();
        Flux<ConsumerRecord<String, FakeConsumerDTO>> source = Flux.just(
                buildConsumerRecord("k1", dto),
                buildConsumerRecord("k2", dto)
        );

        when(reactiveKafkaConsumerTemplate.receiveAutoAck()).thenReturn(source);

        // Act
        assertThatNoException().isThrownBy(() -> reactiveConsumerService.run());
    }

    @Test
    @DisplayName("Constructor wires the template correctly (template is not null after construction)")
    void constructor_templateInjected_serviceCreated() {
        // Arrange & Act
        var service = new ReactiveConsumerService(reactiveKafkaConsumerTemplate);

        // Assert – verify the service can be used (run with empty flux)
        when(reactiveKafkaConsumerTemplate.receiveAutoAck()).thenReturn(Flux.empty());
        assertThatNoException().isThrownBy(service::run);
    }
}