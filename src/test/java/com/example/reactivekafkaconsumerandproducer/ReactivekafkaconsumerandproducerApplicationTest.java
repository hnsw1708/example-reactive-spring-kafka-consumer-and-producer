package com.example.reactivekafkaconsumerandproducer;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.boot.SpringApplication;
import org.springframework.context.ConfigurableApplicationContext;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;

@ExtendWith(MockitoExtension.class)
class ReactivekafkaconsumerandproducerApplicationTest {

    /**
     * Verifies that the main() method invokes SpringApplication.run() with the
     * correct application class and the supplied arguments, without throwing any
     * exception.
     */
    @Test
    void main_shouldInvokeSpringApplicationRun_withCorrectArguments() {
        // Arrange
        var args = new String[]{"--server.port=8080", "--spring.profiles.active=test"};

        try (MockedStatic<SpringApplication> mockedSpringApplication = mockStatic(SpringApplication.class)) {

            ConfigurableApplicationContext mockContext =
                    Mockito.mock(ConfigurableApplicationContext.class);

            mockedSpringApplication
                    .when(() -> SpringApplication.run(
                            eq(ReactivekafkaconsumerandproducerApplication.class),
                            any(String[].class)))
                    .thenReturn(mockContext);

            // Act & Assert
            assertDoesNotThrow(() ->
                    ReactivekafkaconsumerandproducerApplication.main(args));

            mockedSpringApplication.verify(
                    () -> SpringApplication.run(
                            eq(ReactivekafkaconsumerandproducerApplication.class),
                            eq(args)),
                    times(1));
        }
    }

    /**
     * Verifies that the main() method can be called with an empty args array
     * without throwing any exception.
     */
    @Test
    void main_shouldHandleEmptyArgs_withoutException() {
        // Arrange
        var emptyArgs = new String[]{};

        try (MockedStatic<SpringApplication> mockedSpringApplication = mockStatic(SpringApplication.class)) {

            ConfigurableApplicationContext mockContext =
                    Mockito.mock(ConfigurableApplicationContext.class);

            mockedSpringApplication
                    .when(() -> SpringApplication.run(
                            eq(ReactivekafkaconsumerandproducerApplication.class),
                            any(String[].class)))
                    .thenReturn(mockContext);

            // Act & Assert
            assertDoesNotThrow(() ->
                    ReactivekafkaconsumerandproducerApplication.main(emptyArgs));

            mockedSpringApplication.verify(
                    () -> SpringApplication.run(
                            eq(ReactivekafkaconsumerandproducerApplication.class),
                            eq(emptyArgs)),
                    times(1));
        }
    }

    /**
     * Verifies that instantiation of the application class itself does not throw
     * and that the instance is non-null (covers the default constructor generated
     * by the compiler).
     */
    @Test
    void applicationClass_shouldBeInstantiable() {
        assertDoesNotThrow(() -> {
            var instance = new ReactivekafkaconsumerandproducerApplication();
            assertNotNull(instance);
        });
    }

    /**
     * Uses a Java 21 text block to describe intent and verifies that the
     * @SpringBootApplication annotation is present on the class, ensuring the
     * Spring Boot auto-configuration and component-scan are enabled.
     */
    @Test
    void applicationClass_shouldBeAnnotatedWithSpringBootApplication() {
        // Java 21 text block used for the descriptive message
        var description = """
                The main application class must carry @SpringBootApplication so that
                Spring Boot's auto-configuration, component scanning and configuration
                property binding are all activated automatically.
                """;

        var annotation = ReactivekafkaconsumerandproducerApplication.class
                .getAnnotation(org.springframework.boot.autoconfigure.SpringBootApplication.class);

        assertNotNull(annotation, description);
    }
}
