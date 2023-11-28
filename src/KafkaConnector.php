<?php

namespace Kafka;

use Illuminate\Queue\Connectors\ConnectorInterface;

class KafkaConnector implements ConnectorInterface
{
    /**
     * Add connector to Kafka
     * @param array $config
     * @return KafkaQueue
     */
    public function connect(array $config)
    {
        try {
            $conf = new \RdKafka\Conf();
            $conf->set('bootstrap.servers', $config['bootstrap_servers']);
            $conf->set('security.protocol', $config['security_protocol']);
            $conf->set('sasl.mechanism', $config['sasl_mechanisms']);
            $conf->set('sasl.username', $config['sasl_username']);
            $conf->set('sasl.password', $config['sasl_password']);

            $producer = new \RdKafka\Producer($conf);

            $conf->set('group.id', $config['group_id']);
            $conf->set('auto.offset.reset', 'earliest');
            $conf->set('enable.partition.eof', 'true');

            $consumer = new \RdKafka\Consumer($conf);
//            $consumer = new \RdKafka\KafkaConsumer($conf);
            $consumer->addBrokers("127.0.0.1");

            return new KafkaQueue($consumer, $producer);
        } catch (\Exception $exception) {
            var_dump($exception);
        }

    }
}
