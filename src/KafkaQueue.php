<?php

namespace Kafka;

use Illuminate\Queue\Queue;
use Illuminate\Contracts\Queue\Queue as QueueContract;

class KafkaQueue extends Queue implements QueueContract
{

    public function __construct(protected $consumer, protected $producer){}

    public function size($queue = null)
    {
        // TODO: Implement size() method.
    }

    public function push($job, $data = '', $queue = null)
    {
        echo "Kafka Queue: PUSH\n";
        $topic = $this->producer->newTopic($queue ?? getenv('KAFKA_QUEUE'));
        $topic->produce(RD_KAFKA_PARTITION_UA, 0,serialize($job));
        $this->producer->flush(1000);
    }

    public function pushRaw($payload, $queue = null, array $options = [])
    {
        // TODO: Implement pushRaw() method.
    }

    public function later($delay, $job, $data = '', $queue = null)
    {
        // TODO: Implement later() method.
    }

    public function pop($queue = null)
    {
        echo "Kafka Queue: POP\n";

        $this->consumer->subscribe($queue ?? getenv('KAFKA_QUEUE'));

        try {

            $message = $this->consumer->consume(120*1000);

            var_dump($message);
            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    var_dump($message->payload);
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages; will wait for more\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    throw new \Exception($message->errstr(), $message->err);
            }

        } catch (\Exception $exception) {
            var_dump($exception);
        }

    }
}
