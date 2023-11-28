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

        try {

            $topicConf = new \RdKafka\TopicConf();
            $topicConf->set('auto.commit.interval.ms', 100);
            $topicConf->set('auto.offset.reset', 'smallest');

            $topic = $this->consumer->newTopic("test", $topicConf);
            $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
            $message = $topic->consume(0, 120*10000);
//            $topic = $this->consumer->newTopic("default");
//            $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);
//            $message = $topic->consume(0, 120*10000);

//            $topicConf = new \RdKafka\TopicConf();
//            $topicConf->set('auto.commit.interval.ms', 100);

// Set the offset store method to 'file'
//            $topicConf->set('offset.store.method', 'broker');

// Alternatively, set the offset store method to 'none'
// $topicConf->set('offset.store.method', 'none');

// Set where to start consuming messages when there is no initial offset in
// offset store or the desired offset is out of range.
// 'earliest': start from the beginning
//            $topicConf->set('auto.offset.reset', 'earliest');
//            $topic = $this->consumer->newTopic($queue ?? getenv('KAFKA_QUEUE'),$topicConf);
//            $message = $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

//            $this->consumer->subscribe($queue ?? getenv('KAFKA_QUEUE'));
//            $message = $this->consumer->consume(120*1000);

//            $message = $topic->consume(120*1000);
//            $topic->consumeStart(0, RD_KAFKA_OFFSET_STORED);

//            $message = $topic->consume(120*1000);
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
                    echo "exception\n";
                    throw new \Exception($message->errstr(), $message->err);
                    break;
            }

        } catch (\Exception $exception) {
            var_dump($exception);
        }

    }
}
