import string, random, time
from kafka import KafkaProducer



if __name__ == '__main__':
    producer = KafkaProducer(bootstrap_servers=['localhost:9092'])

    while True:
        s2 = (random.choice(string.ascii_lowercase) for _ in range(2))
        word = ''.join(s2)
        value = bytearray(word, 'utf-8')

        producer.send('word_count', value=value).get(timeout=10)

        time.sleep(0.1)
