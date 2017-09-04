package io.devsohi.flink;

import org.apache.flink.streaming.api.datastream.DataStreamSource;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.PrintSinkFunction;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaProducer010;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartition;
import org.apache.flink.streaming.util.serialization.SimpleStringSchema;

import java.util.HashMap;
import java.util.Map;
import java.util.Properties;

public class ReplayKafkaMessages {

	private ReplayKafkaMessages() {
	}

	public static void main(String[] args) throws Exception {

		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setParallelism(1);
		env.enableCheckpointing(1000);


		final FlinkKafkaConsumer010<String> kafkaSource = getKafkaSource();
		final DataStreamSource<String> in = env.addSource(kafkaSource);

		in.addSink(new PrintSinkFunction<>());
		in.addSink(getKafkaSink());

		env.execute();
	}

	private static FlinkKafkaConsumer010<String> getKafkaSource() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		properties.setProperty("zookeeper.connect", "localhost:8081");
		properties.setProperty("group.id", "test11");
		final FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("test", new SimpleStringSchema(), properties);

		// Reset topic with partition and offset
		Map<KafkaTopicPartition, Long> specificStartOffsets = new HashMap<>();
		specificStartOffsets.put(new KafkaTopicPartition("test", 0), 1L);
		//consumer.setStartFromSpecificOffsets(specificStartOffsets);

		return consumer;
	}


	private static FlinkKafkaProducer010<String> getKafkaSink() {
		Properties properties = new Properties();
		properties.setProperty("bootstrap.servers", "localhost:9092");
		return new FlinkKafkaProducer010<>("test2", new SimpleStringSchema(), properties);
	}

}
