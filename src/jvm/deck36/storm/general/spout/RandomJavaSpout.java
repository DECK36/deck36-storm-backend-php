/**
* Example bolt to emit random names.
*
* @author Stefan Schadwinkel <stefan.schadwinkel@deck36.de>
* @copyright Copyright (c) 2013 DECK36 GmbH & Co. KG (http://www.deck36.de)
*
* For the full copyright and license information, please view the LICENSE
* file that was distributed with this source code.
*
*/


package deck36.storm.general.spout;

import backtype.storm.spout.SpoutOutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichSpout;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import java.util.Map;
import java.util.Random;



public class RandomJavaSpout extends BaseRichSpout {

    private final String[] setOfNames = {"Norberto","Britteny","Wyatt","Emogene","Graham","Bertha","Nicole","Sasha",
            "Sherry","Linette","Sibyl","Shari","Annis","Anja","Polly","Walter","Geri","Ema","Celia","Hilda"};

    Random _spoutRandom;

    // Storm
    SpoutOutputCollector _collector;

    @Override
    public void open(Map conf, TopologyContext context, SpoutOutputCollector collector) {
        _collector = collector;

        _spoutRandom = new Random(System.currentTimeMillis());
    }

    @Override
    public void nextTuple() {

        int idx = _spoutRandom.nextInt(setOfNames.length);

        _collector.emit(new Values("java", setOfNames[idx]));

    }

    @Override
    public void declareOutputFields(OutputFieldsDeclarer declarer) {
        declarer.declare(new Fields("src", "random"));
    }

}
