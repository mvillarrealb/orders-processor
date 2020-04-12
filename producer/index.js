const { promisify } = require('util')
const { readFile } =  require('fs');
const kafka = require('kafka-node');
const path =  require('path');
const {Producer, KeyedMessage, KafkaClient } = kafka;
const readFilePromise = promisify(readFile);

let processedSources = 0;
    
const readJsonFile = async (file) => {
  const content = await readFilePromise(file, 'utf-8');
  console.log('Readed file '+file)
  return JSON.parse(content);
}

const sendMessages = (producer, topic, key, messages, done) => {
 const transformed = messages.map((message) => {
    const keyedMessage = new KeyedMessage(message[key], JSON.stringify(message));
    return keyedMessage;
  });
  const payloads = [
    { topic, messages: transformed }
  ];
  console.log(payloads)
  producer.send(payloads, done);
}
const checkClose = ()=> {
  processedSources++
  if(processedSources == 3) {
    process.exit(0);
  }
}
(async function main() {
  try {
    const client   = new KafkaClient({ kafkaHost: '0.0.0.0:9092',connectTimeout: 3000 });
    const producer = new Producer(client);
    const customers = await readJsonFile(path.join(__dirname , '../data/customers.json'));
    const products  = await readJsonFile(path.join(__dirname , '../data/products.json'));
    const orders    = await readJsonFile(path.join(__dirname , '../data/orders.json'));
    sendMessages(producer, 'customers', 'id', customers, checkClose);
    sendMessages(producer, 'products', 'id', products, checkClose);
    sendMessages(producer, 'orders', 'id', orders, checkClose);
    producer.on('error', console.error);
    
  } catch(error) {
    console.error(error);
  }
})();