const kafka = require('kafka-node');
const bp = require('body-parser');
var AWS = require('aws-sdk');
AWS.config.update({region:'ap-southeast-1'});

try {
  const Consumer = kafka.Consumer;
  const kafka_topic = 'AWSKafkaTutorialTopic';
  const client = new kafka.KafkaClient({sslOptions:{ rejectUnauthorized: false } ,kafkaHost: 'b-2.microservicesmsk.njmctn.c4.kafka.ap-southeast-1.amazonaws.com:9094,b-3.microservicesmsk.njmctn.c4.kafka.ap-southeast-1.amazonaws.com:9094,b-1.microservicesmsk.njmctn.c4.kafka.ap-southeast-1.amazonaws.com:9094'});
  let consumer = new Consumer(
    client,
    [{ topic: kafka_topic, partition: 0 }],
    {
      autoCommit: true,
      fetchMaxWaitMs: 1000,
      fetchMaxBytes: 1024 * 1024,
      encoding: 'utf8',
      fromOffset: false
    }
  );
  
  consumer.on('message', async function(message) {
    console.log('here');
    var msg = JSON.parse(message.value);
    
    
    if (msg.event == 'neworder'){
      var newquantity = '';
      //update dynamodb on product quantity
      //var ddb = new AWS.DynamoDB({apiVersion: '2012-08-10'});
      var docClient = new AWS.DynamoDB.DocumentClient({apiVersion: '2012-08-10'});
      var inventoryid = '123';
      var params = {
        TableName: 'InventoryTable',
        Key: {
          'inventoryid': inventoryid
        }
      };
      
      // Call DynamoDB to read the quantity from the table
      docClient.get(params, function(err, data) {
        if (err) {
          console.log("Error", err);
        } else {
          JSON.stringify(data, null, 2);
          console.log("retrieve Success");
          console.log("Existing quantity", data.Item.quantity);
          newquantity = data.Item.quantity - 1;
          console.log("New quantity", newquantity);
          
          // Set DynamoDB with the new data
          params = {
            TableName: 'InventoryTable',
            Key: {
              'inventoryid': inventoryid
            },
            UpdateExpression: "set quantity = :q",
            ExpressionAttributeValues: {
              ':q' : newquantity,
            }
          };
          
          // Call DynamoDB to update the item to the table
          docClient.update(params, function(err, data) {
            if (err) {
              console.log("Error", err);
            } else {
              console.log("Update Success", data);
            }
          });
          
        }
      });
        
        
        
      }
      
    }

  )
  
  consumer.on('error', function(err) {
    console.log('error', err);
  });
}
catch(e) {
  console.log(e);
}
