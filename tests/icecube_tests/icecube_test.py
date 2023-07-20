from gcn_kafka import Consumer

# Connect as a consumer.
# Warning: don't share the client secret with others.
consumer = Consumer(client_id='7ahi9jiq1uv2bgou91epk4c0fj',
                    client_secret='114i0qoo8qp8ju16vd73s0vburhqrqgbrqhctn17og2qkk0f6iro')

# List all topics
topics = consumer.list_topics().topics
for t in topics.keys():
    if "icecube" in t or 'igwn' in t:
        print(t)

# Subscribe to topics and receive alerts
consumer.subscribe(['gcn.notices.icecube.lvk_nu_track_search'])
while True:
    for message in consumer.consume(timeout=1):
        # Print the topic and message ID
        print(f'topic={message.topic()}, offset={message.offset()}')
        value = message.value()
        print(value)