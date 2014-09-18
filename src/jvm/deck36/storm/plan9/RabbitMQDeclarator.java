/**
* General Declarator to declare RabbitMQ topic exchanges.
*
* @author Stefan Schadwinkel <stefan.schadwinkel@deck36.de>
* @copyright Copyright (c) 2013 DECK36 GmbH & Co. KG (http://www.deck36.de)
*
* For the full copyright and license information, please view the LICENSE
* file that was distributed with this source code.
*
*/


package deck36.storm.plan9;

import com.rabbitmq.client.Channel;
import io.latent.storm.rabbitmq.Declarator;

import java.io.IOException;

public class RabbitMQDeclarator implements Declarator {
    private final String exchange;
    private final String queue;
    private final String routingKey;

    public RabbitMQDeclarator(String exchange, String queue, String routingKey) {
        this.exchange = exchange;
        this.queue = queue;
        this.routingKey = routingKey;
    }

    @Override
    public void execute(Channel channel) {

        try {

            channel.exchangeDeclare(
                    exchange,        // name
                    "topic",        // type
                    true            // durable?
            );

            channel.queueDeclare(
                    queue, // name
                    true,           // durable?
                    false,          // exclusive?
                    false,          // autoDelete
                    null            // Map(<String, Object> arguments
            );

            channel.queueBind(
                    queue,
                    exchange,
                    routingKey
            );

        } catch (IOException e) {
            throw new RuntimeException("Error executing rabbitmq declarations.", e);
        }
    }
}
