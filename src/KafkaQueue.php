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
        try {
            $topic = $this->producer->newTopic($queue ?? getenv('KAFKA_QUEUE'));
            $topic->produce(RD_KAFKA_PARTITION_UA, 0,serialize($job));
            $this->producer->flush(1000);
        } catch (\Exception $exception) {
            var_dump($exception);
        }
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

//        try {

            $topicConf = new \RdKafka\TopicConf();
            $topicConf->set('auto.commit.interval.ms', 100);
            $topicConf->set('auto.offset.reset', 'smallest');

            $topic = $this->consumer->newTopic("default", $topicConf);
            $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
            $message = $topic->consume(0, 120*10000);

//            $this->consumer->subscribe($queue ?? getenv('KAFKA_QUEUE'));
//            $message = $this->consumer->consume(120*1000);

            switch ($message->err) {
                case RD_KAFKA_RESP_ERR_NO_ERROR:
                    $job = unserialize($message->payload);
                    $job->handle();
                    break;
                case RD_KAFKA_RESP_ERR__PARTITION_EOF:
                    var_dump("No more messages; will wait for more\n");
                    break;
                case RD_KAFKA_RESP_ERR__TIMED_OUT:
                    echo "Timed out\n";
                    break;
                default:
                    echo "exception\n";
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }

//        } catch (\Exception $exception) {
//            var_dump($exception);
//        }

    }
}
