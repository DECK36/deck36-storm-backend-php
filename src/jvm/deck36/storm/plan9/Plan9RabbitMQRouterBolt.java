/**
* Storm bolt to add additional fields to received tuples that indicate the 
* RabbitMQ exchange and the routing key onto which the message shall be 
* forwarded.
*
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
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

@SuppressWarnings("unchecked")
public class Plan9RabbitMQRouterBolt extends BaseRichBolt {

    private static final Logger log = LoggerFactory.getLogger(Plan9RabbitMQRouterBolt.class);

    private String _exchange;
    private String _routingKey;
  
    OutputCollector _collector;

    public Plan9RabbitMQRouterBolt(String exchange, String routingKey) {
        _exchange = exchange;
        _routingKey = routingKey;
    }


    @Override
    public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
        // Setup output collector
        _collector = collector;
    }

    @Override
    public void execute(Tuple tuple) {

        String msg = JSONValue.toJSONString(tuple.getValueByField("badge"));

        _collector.emit(tuple, new Values(msg, _exchange, _routingKey));
        _collector.ack(tuple);
    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("msg", "exchange", "routingKey"));
    }

    @Override
    public void cleanup() {
    }
    
}