package org.mvb.oms.orders.processor;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonDeserializer;
import org.apache.kafka.connect.json.JsonSerializer;
import org.apache.kafka.streams.*;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Properties;

import static org.junit.Assert.*;
import static org.junit.Assert.assertTrue;

public class OrderProcessorSpec {

    private OrderProcessor app;
    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> productTopic;
    private TestInputTopic<String, String> customerTopic;
    private TestInputTopic<String, String> ordersTopic;
    private TestOutputTopic<String, JsonNode> orderWithProduct;
    private TestOutputTopic<String, JsonNode> orderEnriched;
    private ObjectMapper mapper;

    @Before
    public void setup() {
        Properties properties = new Properties();
        properties.put(StreamsConfig.APPLICATION_ID_CONFIG, "test-app");
        properties.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "127.0.0.1:9092");
        properties.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        mapper = new ObjectMapper();
        app = new OrderProcessor();
        testDriver = new TopologyTestDriver(app.createTopology(), properties);

        productTopic     = testDriver.createInputTopic("products", new StringSerializer(), new StringSerializer());
        customerTopic    = testDriver.createInputTopic("customers", new StringSerializer(), new StringSerializer());
        ordersTopic      = testDriver.createInputTopic("orders", new StringSerializer(), new StringSerializer());

        testDriver.createInputTopic("orders-with-product", new StringSerializer(), new JsonSerializer());

        orderWithProduct = testDriver.createOutputTopic("orders-with-product", new StringDeserializer(), new JsonDeserializer());
        orderEnriched    = testDriver.createOutputTopic("orders-enriched", new StringDeserializer(), new JsonDeserializer());

        String product = " {\"id\":\"ade9da56-d8f6-43ab-a79b-17d9b8091c96\",\"skuCode\":24,\"description\":\"Test Product\"}";
        String customer = "{\"id\":\"c93f7c73-bebf-4738-898e-2ce346f038b6\",\"first_name\":\"Hilary\",\"last_name\":\"Tipling\",\"email\":\"htipling6@mit.edu\"}";

        productTopic.pipeInput("ade9da56-d8f6-43ab-a79b-17d9b8091c96", product);
        customerTopic.pipeInput("c93f7c73-bebf-4738-898e-2ce346f038b6", customer);
    }

    @After
    public void shutdown() {
        testDriver.close();
    }

    @Test
    public void addCustomer() throws JsonProcessingException {
        JsonNode order    = mapper.readTree("{\"id\":\"1\",\"customerId\":\"99\",\"items\":[{\"id\":\"x\",\"quantity\":2}]}");
        JsonNode customer = mapper.readTree("{\"id\":\"99\",\"first_name\":\"FOO\",\"last_name\":\"BAR\",\"email\":\"email@mockme.com\"}");
        JsonNode result = this.app.addCustomers(order, customer);
        assertTrue(result.has("orderInfo"));
        assertTrue(result.has("customer"));
    }
    @Test
    public void addProductItem() throws JsonProcessingException  {
        JsonNode baseProduct    = mapper.readTree("{\"id\":\"8f226d2a-d5d2-411a-b3ed-a85407f0c4ef\",\"quantity\":5}");
        JsonNode product = mapper.readTree("{\"id\":\"8f226d2a-d5d2-411a-b3ed-a85407f0c4ef\",\"skuCode\":27,\"description\":\"Bacardi Mojito\"}");
        JsonNode result = this.app.addProductItem(baseProduct, product);
        assertTrue(result.has("skuCode"));
        assertTrue(result.has("description"));
        assertTrue(result.has("quantity"));
        assertEquals(result.get("skuCode").asInt(), 27);
        assertEquals(result.get("quantity").asInt(), 5);
        assertEquals(result.get("description").asText(), "Bacardi Mojito");
    }
    @Test
    public void addProduct() throws JsonProcessingException {
        JsonNode order   = mapper.readTree("{\"id\":\"1\",\"customerId\":\"99\",\"items\":[{\"id\":\"x\",\"quantity\":2}]}");
        JsonNode product = mapper.readTree("{\"id\":\"x\",\"skuCode\":27,\"description\":\"Bacardi Mojito\"}");
        JsonNode result = this.app.addProduct(order, product);
        assertTrue(result.has("orderInfo"));
        assertTrue(result.has("customer"));
        assertTrue(result.has("products"));

        JsonNode withMoreItems = this.app.addProduct(result, product);
        ArrayList<JsonNode> products = new ArrayList<>();
        withMoreItems.withArray("products").elements().forEachRemaining(products::add);
        assertTrue(withMoreItems.has("orderInfo"));
        assertTrue(withMoreItems.has("customer"));
        assertTrue(withMoreItems.has("products"));
        assertTrue((products.size() == 2));
    }

    @Test
    public void ensureProductJoin() {
        String dummyOrder = "{\"id\":\"f3183934-b1e2-40f1-b1c6-72453fdb5e5a\",\"customerId\":\"c93f7c73-bebf-4738-898e-2ce346f038b6\",\"items\":[{\"id\":\"ade9da56-d8f6-43ab-a79b-17d9b8091c96\",\"quantity\":3}]}";
        assertTrue(orderWithProduct.isEmpty());
        ordersTopic.pipeInput("f3183934-b1e2-40f1-b1c6-72453fdb5e5a", dummyOrder);
        assertFalse(orderWithProduct.isEmpty());
        JsonNode node = orderWithProduct.readValue();
        assertEquals(node.get("description").asText(), "Test Product");
        assertEquals(node.get("quantity").asInt(), 3);
        assertEquals(node.get("skuCode").asInt(), 24);
    }

    @Test
    public void ensureFinalJoin() {
        String dummyOrder = "{\"id\":\"f3183934-b1e2-40f1-b1c6-72453fdb5e5a\",\"customerId\":\"c93f7c73-bebf-4738-898e-2ce346f038b6\",\"items\":[{\"id\":\"ade9da56-d8f6-43ab-a79b-17d9b8091c96\",\"quantity\":3}]}";
        assertTrue(orderEnriched.isEmpty());
        ordersTopic.pipeInput("f3183934-b1e2-40f1-b1c6-72453fdb5e5a", dummyOrder);
        ordersTopic.pipeInput("f3183934-b1e2-40f1-b1c6-72453fdb5e5a", dummyOrder);
        //KeyValue<String,JsonNode> keyValue = orderWithProduct.readKeyValue();
        JsonNode node = orderEnriched.readValue();
        System.out.println(node.toPrettyString());
        assertTrue(node.has("orderId"));
        assertTrue(node.has("customer"));
        assertTrue(node.has("products"));
        //assertFalse(orderEnriched.isEmpty());
    }
}
