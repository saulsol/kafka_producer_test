from confluent_kafka import Producer
import sys
import time

BROKER_LST = 'kafka01:9092,kafka02:9092,kafka03:9092'


class SimpleProducer:


    # 파이선 생성자
    def __init__(self, topic, duration=None):
        self.topic = topic # self는 this의 개념이라고 이해하면 됩니다. 
        self.duration = duration if duration is not None else 60
        self.conf = {'bootstrap.servers': BROKER_LST}

        self.producer = Producer(self.conf)

    # Optional per-message delivery callback (triggered by poll() or flush())
    # when a message has been successfully delivered or permanently
    # failed delivery (after retries).
    def delivery_callback(self, err, msg):
        if err:
            sys.stderr.write('%% Message failed delivery: %s\n' % err)
        else:
            sys.stderr.write('%% Message delivered to %s [%d] @ %d\n' %
                             (msg.topic(), msg.partition(), msg.offset()))

    def produce(self):
        cnt = 0
        while cnt < self.duration:
            try:
                # Produce line (without newline)
                self.producer.produce(
                    topic=self.topic,
                    key=str(cnt),
                    value=f'hello world: {cnt}',
                    on_delivery=self.delivery_callback)
                # Async(비동기 식 저장)
                # _ack 응답을 받지 않아도 바로 다음 메세지를 처리할 수 있는 방식
                # on_delivery 옵션에 지정한 콜백 함수를 (ack응답에 따라 분기 처리 가능) 통하 처리 가능

                ## 메세지 생성자는 버퍼에 메세지를 바로 바로 보내는 것이 아닌
                # 버퍼에 쌓아 두었다가 보내는 방식으로 진행합니다.

                # 즉 메모리에 쌓아 두었다가 넘기는 방식

            except BufferError:
                sys.stderr.write('%% Local producer queue is full (%d messages awaiting delivery): try again\n' %
                                 len(self.producer))

            # Serve delivery callback queue.
            # NOTE: Since produce() is an asynchronous API this poll() call
            #       will most likely not serve the delivery callback for the
            #       last produce()d message.
            self.producer.poll(0)
            cnt += 1
            time.sleep(1)  # 1초 대기

        # Wait until all messages have been delivered
        sys.stderr.write('%% Waiting for %d deliveries\n' % len(self.producer))
        self.producer.flush()


if __name__ == '__main__':
    simple_producer = SimpleProducer(topic='lesson.ch5-1.simple.producer', duration=60)
    simple_producer.produce()