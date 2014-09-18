/**
 * Storm topology for the "Plan9 From Outer Space" tutorial game.
 * Consumes game messages from RabbitMQ, forwards them to the
 * respective bolt implemented in PHP and forwards the
 * resulting messages back to RabbitMQ.
 *
 *
 * @author Stefan Schadwinkel <stefan.schadwinkel@deck36.de>
 * @copyright Copyright (c) 2013 DECK36 GmbH & Co. KG (http://www.deck36.de)
 *
 * For the full copyright and license information, please view the LICENSE
 * file that was distributed with this source code.
 *
 */

package deck36.storm.plan9.php;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.topology.TopologyBuilder;
import com.jayway.jsonpath.JsonPath;
import deck36.storm.general.bolt.MultilangAdapterTickTupleBolt;
import deck36.storm.plan9.Plan9RabbitMQPushBolt;
import deck36.storm.plan9.Plan9RabbitMQRouterBolt;
import deck36.yaml.YamlLoader;
import org.json.simple.JSONValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class KittenRobbersTopology {

    private static final Logger log = LoggerFactory.getLogger(KittenRobbersTopology.class);

    public static void main(String[] args) throws Exception {

        String env = null;

        if (args != null && args.length > 0) {
            env = args[0];
        }

        if (! "dev".equals(env))
            if (! "prod".equals(env)) {
                System.out.println("Usage: $0 (dev|prod)\n");
                System.exit(1);
            }


        // Topology config
        Config conf = new Config();

        // Load parameters and add them to the Config
        Map configMap = YamlLoader.loadYamlFromResource("config_" + env + ".yml");

        conf.putAll(configMap);

        log.info(JSONValue.toJSONString((conf)));


        // Set topology loglevel to DEBUG
        conf.put(Config.TOPOLOGY_DEBUG, JsonPath.read(conf, "$.deck36_storm.debug"));

        // Create Topology builder
        TopologyBuilder builder = new TopologyBuilder();

        // if there are not special reasons, start with parallelism hint of 1
        // and multiple tasks. By that, you can scale dynamically later on.
        int parallelism_hint = JsonPath.read(conf, "$.deck36_storm.default_parallelism_hint");
        int num_tasks = JsonPath.read(conf, "$.deck36_storm.default_num_tasks");

        String badgeName = KittenRobbersTopology.class.getSimpleName();

        // construct command to invoke the external bolt implementation
        ArrayList<String> command = new ArrayList(15);

        // Add main execution program (php, hhvm, zend, ..) and parameters
        command.add((String) JsonPath.read(conf, "$.deck36_storm.php.executor"));
        command.addAll((List<String>) JsonPath.read(conf, "$.deck36_storm.php.executor_params"));

        // Add main command to be executed (app/console, the phar file, etc.) and global context parameters (environment etc.)
        command.add((String) JsonPath.read(conf, "$.deck36_storm.php.main"));
        command.addAll((List<String>) JsonPath.read(conf, "$.deck36_storm.php.main_params"));

        // Add main route to be invoked and its parameters
        command.add((String) JsonPath.read(conf, "$.deck36_storm.KittenRobbersFromOuterSpaceBolt.main"));
        List boltParams = (List<String>) JsonPath.read(conf, "$.deck36_storm.KittenRobbersFromOuterSpaceBolt.params");
        if (boltParams != null)
            command.addAll(boltParams);

        // Log the final command
        log.info("Command to start bolt for KittenRobbersFromOuterSpace: " + Arrays.toString(command.toArray()));

        // Add constructed external bolt command to topology using MultilangAdapterBolt
        builder.setBolt("badge",
                new MultilangAdapterTickTupleBolt(
                        command,
                        (Integer) JsonPath.read(conf, "$.deck36_storm.KittenRobbersFromOuterSpaceBolt.robber_frequency"),
                        "badge"
                ),
                parallelism_hint)
                .setNumTasks(num_tasks);


        builder.setBolt("rabbitmq_router",
                new Plan9RabbitMQRouterBolt(
                        (String) JsonPath.read(conf, "$.deck36_storm.KittenRobbersFromOuterSpaceBolt.rabbitmq.target_exchange"),
                        "KittenRobbers" // RabbitMQ routing key
                ),
                parallelism_hint)
                .setNumTasks(num_tasks)
                .shuffleGrouping("badge");

        builder.setBolt("rabbitmq_producer",
                new Plan9RabbitMQPushBolt(),
                parallelism_hint)
                .setNumTasks(num_tasks)
                .shuffleGrouping("rabbitmq_router");

        if ("dev".equals(env)) {
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology(badgeName + System.currentTimeMillis(), conf, builder.createTopology());
            Thread.sleep(2000000);
        }

        if ("prod".equals(env)) {
            StormSubmitter.submitTopology(badgeName + "-" + System.currentTimeMillis(), conf, builder.createTopology());
        }

    }
}
