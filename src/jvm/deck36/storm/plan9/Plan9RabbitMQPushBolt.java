/**
* General Storm bolt to push data to RabbitMQ.
*
* @author Stefan Schadwinkel <stefan.schadwinkel@deck36.de>
* @copyright Copyright (c) 2013 DECK36 GmbH & Co. KG (http://www.deck36.de)
*
* For the full copyright and license information, please view the LICENSE
* file that was distributed with this source code.
*
*/

package deck36.storm.plan9;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Tuple;

import java.nio.charset.StandardCharsets;
import java.util.Map;

// RabbitMQ client driver
import com.jayway.jsonpath.JsonPath;
import com.rabbitmq.client.AMQP.BasicProperties;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


@SuppressWarnings("unchecked")
public class Plan9RabbitMQPushBolt extends BaseRichBolt {

    private static final Logger log = LoggerFactory.getLogger(Plan9RabbitMQPushBolt.class);

    // RabbitMQ client driver
    private Connection _conn;
    private Channel _channel;
  
    OutputCollector _collector;

    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {

        // Setup output collector
        _collector = collector;

        // connect to RabbitMQ

        String host  = (String)    JsonPath.read(stormConf, "$.deck36_storm.rabbitmq.host");
        int port     = ((Long)      JsonPath.read(stormConf, "$.deck36_storm.rabbitmq.port")).intValue();
        String user  = (String)    JsonPath.read(stormConf, "$.deck36_storm.rabbitmq.user");
        String pass  = (String)    JsonPath.read(stormConf, "$.deck36_storm.rabbitmq.pass");
        String vhost = (String)    JsonPath.read(stormConf, "$.deck36_storm.rabbitmq.vhost");

        ConnectionFactory factory = new ConnectionFactory();
            
        try {

            factory.setUsername(user);
            factory.setPassword(pass);
            factory.setVirtualHost(vhost);
            factory.setHost(host);
            factory.setPort(port);
            _conn = factory.newConnection();

            _channel = _conn.createChannel();     

        } catch (Exception e) {
            log.error(e.toString());
        }

    }

    @Override
    public void execute(Tuple tuple) {

        String msg = (String)tuple.getValueByField("msg");
        String exchange = (String)tuple.getValueByField("exchange");
        String routingKey = (String)tuple.getValueByField("routingKey");

        byte[] msgBodyBytes = msg.getBytes(StandardCharsets.UTF_8);

        try {
            _channel.basicPublish(exchange, routingKey,
                         new BasicProperties.Builder().contentEncoding("UTF-8").contentType("text/plain;charset=utf-8").deliveryMode(2).build(),
                         msgBodyBytes);
           
            _collector.ack(tuple);
        } catch (Exception e) {
            _collector.fail(tuple);
            log.error(e.toString());
        }

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {        
    }

    @Override
    public void cleanup() {
        
        try {
            _channel.close();
            _conn.close();        
        } catch (Exception e) {
            log.error(e.toString());
        }

    }
    
}