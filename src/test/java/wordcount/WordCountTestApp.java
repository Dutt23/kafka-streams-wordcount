package wordcount;

import static org.junit.Assert.assertEquals;

import java.util.Arrays;
import java.util.List;
import java.util.Properties;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.test.TestRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import com.sd.shatyaki.wordcount.WordCountApp;

public class WordCountTestApp {

    private TopologyTestDriver driver;

    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    StringSerializer stringSerializer = new StringSerializer();

    @Before
    public void setUp() {
        Properties properties = new Properties();
        properties.setProperty(StreamsConfig.APPLICATION_ID_CONFIG, "test-01");
        properties.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        properties.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        properties.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        WordCountApp app = new WordCountApp();
        System.out.println(app.createTopology().describe());
        driver = new TopologyTestDriver(app.createTopology(), properties);

        inputTopic = driver.createInputTopic("word-count-input", Serdes.String().serializer(),
                Serdes.String().serializer());

        outputTopic = driver.createOutputTopic("word-count-output", Serdes.String().deserializer(),
                Serdes.Long().deserializer());
    }

    @After
    public void destroy() {
        driver.close();
    }

    @Test
    public void dummyTest() {
        String dummy = "Du" + "mmy";
        assertEquals(dummy, "Dummy");
    }

    // Use this as ref
    //https://kafka-tutorials.confluent.io/create-stateful-aggregation-count/kstreams.html#create-a-test-configuration-file
    @Test
    public void testCounts() {
        String firstExample = "testing kafka kafka streams";
        pushNewInputRecord(firstExample);
        
        List<KeyValue<String, Long>> output = readRecord();

        List<String> keys = Arrays.asList(firstExample.split("\\s+"));
        List<Long> values = Arrays.asList(1L, 1L, 2L, 1L);
        
        int i = 0;
        for(KeyValue<String, Long> record : output) {
            checkKeyValue(record.value, values.get(i));
            checkKeyValue(record.key, keys.get(i));
            i++;
        }
       
    }

    private void pushNewInputRecord(String value) {
        inputTopic.pipeInput(new TestRecord<String, String>(null, value));
    }
    
    private void checkKeyValue(Long key, Long value) {
        assertEquals(key, value);
    }
    
    private void checkKeyValue(String key, String value) {
        assertEquals(key, value);
    }

    private List<KeyValue<String, Long>> readRecord() {
        return outputTopic.readKeyValuesToList();
    }
}
