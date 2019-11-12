const kafka = require('kafka-node');
const bp = require('body-parser');

try {
  const Producer = kafka.Producer;
  const client = new kafka.KafkaClient({sslOptions:{ rejectUnauthorized: false } ,kafkaHost: 'b-2.microservicesmsk.njmctn.c4.kafka.ap-southeast-1.amazonaws.com:9094,b-3.microservicesmsk.njmctn.c4.kafka.ap-southeast-1.amazonaws.com:9094,b-1.microservicesmsk.njmctn.c4.kafka.ap-southeast-1.amazonaws.com:9094'});
  const producer = new Producer(client);
  const kafka_topic = 'AWSKafkaTutorialTopic';
  console.log(kafka_topic);
  let payloads = [
    {
      topic: kafka_topic,
      messages: JSON.stringify({
        'event':'neworder',
        'orderid':'order123',
        'product':'iphone',
        'quantity':'1'
        })
    }
  ];

  producer.on('ready', async function() {
    console.log("producer is ready !!!");
    let push_status = producer.send(payloads, (err, data) => {
      if (err) {
        console.log('[kafka-producer -> '+kafka_topic+']: broker update failed');
      } else {
        console.log('[kafka-producer -> '+kafka_topic+']: broker update success');
      }
    });
  });

  producer.on('error', function(err) {
    console.log(err);
    console.log('[kafka-producer -> '+kafka_topic+']: connection errored');
    throw err;
  });
}
catch(e) {
  console.log(e);
}
