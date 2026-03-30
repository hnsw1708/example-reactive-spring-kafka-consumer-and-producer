package com.example.reactivekafkaconsumerandproducer.dto;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.NullSource;
import org.junit.jupiter.params.provider.ValueSource;

import static org.assertj.core.api.Assertions.assertThat;

@DisplayName("FakeConsumerDTO Tests")
class FakeConsumerDTOTest {

    // -------------------------------------------------------------------------
    // Constructor & Getter
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Constructor and getId()")
    class ConstructorAndGetter {

        @Test
        @DisplayName("Should store id provided via constructor")
        void shouldStoreIdFromConstructor() {
            var dto = new FakeConsumerDTO("abc-123");

            assertThat(dto.getId()).isEqualTo("abc-123");
        }

        @Test
        @DisplayName("Should accept null id in constructor")
        void shouldAcceptNullId() {
            var dto = new FakeConsumerDTO(null);

            assertThat(dto.getId()).isNull();
        }

        @Test
        @DisplayName("Should accept empty string id in constructor")
        void shouldAcceptEmptyId() {
            var dto = new FakeConsumerDTO("");

            assertThat(dto.getId()).isEmpty();
        }

        @ParameterizedTest(name = "id=''{0}''")
        @ValueSource(strings = {"1", "uuid-value", "  spaces  ", "special!@#$%"})
        @DisplayName("Should preserve various id values as-is")
        void shouldPreserveVariousIdValues(String id) {
            var dto = new FakeConsumerDTO(id);

            assertThat(dto.getId()).isEqualTo(id);
        }
    }

    // -------------------------------------------------------------------------
    // Setter
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("setId()")
    class SetId {

        @Test
        @DisplayName("Should update id via setter")
        void shouldUpdateIdViaSetter() {
            var dto = new FakeConsumerDTO("original");
            dto.setId("updated");

            assertThat(dto.getId()).isEqualTo("updated");
        }

        @Test
        @DisplayName("Should allow setting id to null")
        void shouldAllowSettingIdToNull() {
            var dto = new FakeConsumerDTO("some-id");
            dto.setId(null);

            assertThat(dto.getId()).isNull();
        }

        @Test
        @DisplayName("Should allow setting id to empty string")
        void shouldAllowSettingIdToEmpty() {
            var dto = new FakeConsumerDTO("some-id");
            dto.setId("");

            assertThat(dto.getId()).isEmpty();
        }

        @Test
        @DisplayName("Should allow multiple successive set calls")
        void shouldAllowMultipleSetCalls() {
            var dto = new FakeConsumerDTO("first");
            dto.setId("second");
            dto.setId("third");

            assertThat(dto.getId()).isEqualTo("third");
        }
    }

    // -------------------------------------------------------------------------
    // toString()
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("toString()")
    class ToStringTests {

        @Test
        @DisplayName("Should produce expected format with non-null id")
        void shouldProduceExpectedFormatWithNonNullId() {
            var dto = new FakeConsumerDTO("xyz-789");

            String expected = "FakeConsumerDTO{id='xyz-789'}";
            assertThat(dto.toString()).isEqualTo(expected);
        }

        @Test
        @DisplayName("Should produce expected format with null id")
        void shouldProduceExpectedFormatWithNullId() {
            var dto = new FakeConsumerDTO(null);

            String expected = "FakeConsumerDTO{id='null'}";
            assertThat(dto.toString()).isEqualTo(expected);
        }

        @Test
        @DisplayName("Should produce expected format with empty id")
        void shouldProduceExpectedFormatWithEmptyId() {
            var dto = new FakeConsumerDTO("");

            String expected = "FakeConsumerDTO{id=''}";
            assertThat(dto.toString()).isEqualTo(expected);
        }

        @Test
        @DisplayName("Should contain class name in toString output")
        void shouldContainClassNameInToString() {
            var dto = new FakeConsumerDTO("test");

            assertThat(dto.toString()).contains("FakeConsumerDTO");
        }

        @Test
        @DisplayName("Should contain id value in toString output")
        void shouldContainIdValueInToString() {
            String id = "my-unique-id";
            var dto = new FakeConsumerDTO(id);

            assertThat(dto.toString()).contains(id);
        }
    }

    // -------------------------------------------------------------------------
    // JSON serialisation (Jackson annotations smoke tests)
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("JSON serialisation")
    class JsonSerialisationTests {

        private final ObjectMapper mapper = new ObjectMapper();

        @Test
        @DisplayName("Should serialise id field with @JsonProperty name 'id'")
        void shouldSerialiseIdFieldWithCorrectJsonPropertyName() throws Exception {
            var dto = new FakeConsumerDTO("ser-001");

            String json = mapper.writeValueAsString(dto);

            assertThat(json).contains("\"id\":\"ser-001\"");
        }

        @Test
        @DisplayName("Should deserialise id from JSON")
        void shouldDeserialiseIdFromJson() throws Exception {
            String json = "{\"id\":\"deser-002\"}";

            FakeConsumerDTO dto = mapper.readValue(json, FakeConsumerDTO.class);

            assertThat(dto.getId()).isEqualTo("deser-002");
        }

        @Test
        @DisplayName("Should serialise null id as null in JSON")
        void shouldSerialiseNullIdAsNull() throws Exception {
            var dto = new FakeConsumerDTO(null);

            String json = mapper.writeValueAsString(dto);

            assertThat(json).contains("\"id\":null");
        }

        @Test
        @DisplayName("Should wrap object under root name when WRAP_ROOT_VALUE enabled")
        void shouldWrapObjectUnderRootNameWhenEnabled() throws Exception {
            ObjectMapper wrappingMapper = new ObjectMapper()
                    .enable(SerializationFeature.WRAP_ROOT_VALUE);

            var dto = new FakeConsumerDTO("root-test");
            String json = wrappingMapper.writeValueAsString(dto);

            assertThat(json).contains("FakeConsumer");
        }

        @Test
        @DisplayName("Should round-trip serialize and deserialize correctly")
        void shouldRoundTripSerializeAndDeserialize() throws Exception {
            var original = new FakeConsumerDTO("round-trip-id");

            String json = mapper.writeValueAsString(original);
            FakeConsumerDTO restored = mapper.readValue(json, FakeConsumerDTO.class);

            assertThat(restored.getId()).isEqualTo(original.getId());
        }
    }

    // -------------------------------------------------------------------------
    // Java 21 pattern matching / instanceof smoke test
    // -------------------------------------------------------------------------

    @Nested
    @DisplayName("Java 21 pattern matching with FakeConsumerDTO")
    class PatternMatchingTests {

        @Test
        @DisplayName("Should match FakeConsumerDTO instance via pattern matching instanceof")
        void shouldMatchInstanceViaPatternMatchingInstanceof() {
            Object obj = new FakeConsumerDTO("pm-id");

            if (obj instanceof FakeConsumerDTO dto) {
                assertThat(dto.getId()).isEqualTo("pm-id");
            } else {
                throw new AssertionError("Expected obj to be an instance of FakeConsumerDTO");
            }
        }

        @Test
        @DisplayName("Should not match unrelated type via pattern matching instanceof")
        void shouldNotMatchUnrelatedType() {
            Object obj = "just a string";

            if (obj instanceof FakeConsumerDTO dto) {
                throw new AssertionError("Should not have matched FakeConsumerDTO");
            } else {
                assertThat(obj).isInstanceOf(String.class);
            }
        }

        @Test
        @DisplayName("Should classify objects using if-else instanceof chain")
        void shouldClassifyObjectsUsingIfElseInstanceof() {
            Object value = new FakeConsumerDTO("classify-id");
            String result;
            if (value instanceof FakeConsumerDTO dto) {
                result = "FakeConsumerDTO with id: " + dto.getId();
            } else if (value instanceof String s) {
                result = "string: " + s;
            } else {
                result = "other";
            }
            assertThat(result).isEqualTo("FakeConsumerDTO with id: classify-id");
        }
    }
}