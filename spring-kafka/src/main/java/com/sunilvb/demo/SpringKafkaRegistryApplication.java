package com.sunilvb.demo;

import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Arrays;
import java.util.Properties;

import jdk.nashorn.internal.ir.debug.JSONWriter;
import jdk.nashorn.internal.parser.JSONParser;
import org.apache.avro.data.Json;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.log4j.Logger;
import org.codehaus.jackson.map.ObjectMapper;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import io.confluent.kafka.serializers.KafkaAvroSerializer;

@SpringBootApplication
@RestController
public class SpringKafkaRegistryApplication {

	final static Logger logger = Logger.getLogger(SpringKafkaRegistryApplication.class);
	@Value("${bootstrap.url}")
	String bootstrap;
	@Value("${registry.url}")
	String registry;

	private ObjectMapper mapper = new ObjectMapper();
	
	public static void main(String[] args) {
		SpringApplication.run(SpringKafkaRegistryApplication.class, args);
			
	}
	
	@RequestMapping("/price")
	public String doItPrice(@RequestParam(value="name", defaultValue="PriceList-avro") String name,@RequestParam(value=
			"jsonFile",	defaultValue="priceList1") String jsonFile)
	{
		
		String ret=name;
		try
		{
			ret += "<br>Using Bootstrap : " + bootstrap;
			ret += "<br>Using Bootstrap : " + registry;
			

			ret += sendMsgPrice(configureProperties(), name,jsonFile);
		}
		catch(Exception ex){ ret+="<br>"+ex.getMessage();}
		
		return ret;
	}

	@RequestMapping("/item")
	public String doItItem(@RequestParam(value="name", defaultValue="CanonicalItem-avro") String name,@RequestParam(value=
			"jsonFile",	defaultValue="canonicalItem1") String jsonFile)
	{

		String ret=name;
		try
		{
			ret += "<br>Using Bootstrap : " + bootstrap;
			ret += "<br>Using Bootstrap : " + registry;



			ret += sendMsgItem(configureProperties(), name,jsonFile);
		}
		catch(Exception ex){ ret+="<br>"+ex.getMessage();}

		return ret;
	}

	private Properties configureProperties(){
		Properties properties = new Properties();
		// Kafka Properties
		properties.setProperty("bootstrap.servers", bootstrap);
		properties.setProperty("acks", "all");
		properties.setProperty("retries", "10");
		// Avro properties
		properties.setProperty("key.serializer", StringSerializer.class.getName());
		properties.setProperty("value.serializer", KafkaAvroSerializer.class.getName());
		properties.setProperty("schema.registry.url", registry);
		return properties;
	}



	private PriceList sendMsgPrice(Properties properties, String topic, String jsonFile) throws IOException {
		Producer<String, Order> producer = new KafkaProducer<String, Order>(properties);

				Order order = (Order) readJson(jsonFile,"order");

        ProducerRecord<String, PriceList> producerRecord = new ProducerRecord<String, PriceList>(topic, priceList);

        
        producer.send(producerRecord, new Callback() {
            @Override
            public void onCompletion(RecordMetadata metadata, Exception exception) {
                if (exception == null) {
                    logger.info(metadata); 
                } else {
                	logger.error(exception.getMessage());
                }
            }
        });

        producer.flush();
        producer.close();
        
        return priceList;
	}
	private CanonicalItem sendMsgItem(Properties properties, String topic, String jsonFile) throws IOException {
		Producer<String, CanonicalItem> producer = new KafkaProducer<String, CanonicalItem>(properties);
		CanonicalItem item = (CanonicalItem) readJson(jsonFile,"item");

		ProducerRecord<String, CanonicalItem> producerRecord = new ProducerRecord<String, CanonicalItem>(topic, item);


		producer.send(producerRecord, new Callback() {
			@Override
			public void onCompletion(RecordMetadata metadata, Exception exception) {
				if (exception == null) {
					logger.info(metadata);
				} else {
					logger.error(exception.getMessage());
				}
			}
		});

		producer.flush();
		producer.close();

		return item;
	}

	private Object readJson(String jsonFile,String type) throws IOException {

		if (type.equalsIgnoreCase("price")){
			String path = getClass().getClassLoader().getResource("json/"+jsonFile+".json").getPath();
			return mapper.readValue(new File(path), Price.class);
		}
		if (type.equalsIgnoreCase("order")){
			String path = getClass().getClassLoader().getResource("json/"+jsonFile+".json").getPath();
			return mapper.readValue(new File(path), Order.class);
		}
		return null;
	}

	
	
}
