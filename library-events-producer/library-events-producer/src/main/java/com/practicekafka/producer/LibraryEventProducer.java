package com.practicekafka.producer;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.practicekafka.domain.LibraryEvent;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Component;

import java.util.concurrent.CompletableFuture;

@Component
@Slf4j
public class LibraryEventProducer {

    @Value("${spring.kafka.topic}")
    private String topic;

    private final KafkaTemplate<Integer, String> kafkaTemplate;

    private final ObjectMapper objectMapper;


    public LibraryEventProducer(KafkaTemplate<Integer, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    public CompletableFuture<SendResult<Integer, String>> sendLibraryEvent(LibraryEvent libraryEvent) throws JsonProcessingException {
        Integer key = libraryEvent.libraryEventId();
        String value = objectMapper.writeValueAsString(libraryEvent);

        CompletableFuture<SendResult<Integer,String>> completableFuture = kafkaTemplate.send(topic,key,value);

        return completableFuture.whenComplete(
                (sendResult, throwable) -> {
                    if(throwable!=null){
                        handleOnFailure(key,value,throwable);
                    } else {
                        handleOnSuccess(key,value,sendResult);
                    }
                }
        );
    }

    private void handleOnSuccess(Integer key, String value, SendResult<Integer, String> sendResult) {
        log.info("Message sent successfully with message id {} and message data {} into the partition {}"
                , key,value, sendResult.getRecordMetadata().partition());
    }

    private void handleOnFailure(Integer key, String value, Throwable throwable) {
        log.error("Message Sending Failed {}" , throwable.getMessage(), throwable);
    }
}
