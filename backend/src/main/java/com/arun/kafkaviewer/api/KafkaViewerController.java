package com.arun.kafkaviewer.api;

import com.arun.kafkaviewer.service.KafkaViewerService;
import java.util.List;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/api/topics")
public class KafkaViewerController {

    private final KafkaViewerService service;

    public KafkaViewerController(KafkaViewerService service) {
        this.service = service;
    }

    @GetMapping
    public List<TopicSummary> topics(
            @RequestParam(defaultValue = "false") boolean includeInternal) {
        return service.topics(includeInternal);
    }

    @GetMapping("/{topic}/partitions")
    public List<PartitionSummary> partitions(@PathVariable String topic) {
        return service.partitions(topic);
    }

    @GetMapping("/{topic}/messages")
    public TopicMessages messages(
            @PathVariable String topic,
            @RequestParam(required = false) Integer partition,
            @RequestParam(defaultValue = "100") int limit) {
        return service.latestMessages(topic, partition, limit);
    }
}
