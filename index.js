'use strict';

const {logError} = require('logger');
const type = 'fanout';
const exchange = 'events';

let channel = null;

const connect = () => {
  const amqp = require('amqplib');
  const host =
    process.env.OPENSHIFT_RABBITMQ_SERVICE_HOST || 'localhost';
  const port =
    process.env.OPENSHIFT_RABBITMQ_PORT_5672_TCP_PORT || 5672;

  try {
    return amqp.connect(`amqp://${host}:${port}`);
  } catch (err) {
    logError(err);
  }
};

const getChannel = async () => {
  if (!channel) {
    const conn = await connect();
    // eslint-disable-next-line require-atomic-updates
    channel = await conn.createChannel();
    channel.on('close', (err) => {
      logError('Channel closed');
      // setTimeout(() => this.connect(uri), 10000);
      process.exit(1);
    });
  }
  return channel;
};

const publish = async (message) => {
  const channel = await getChannel();
  message = JSON.stringify(message || {});
  return channel.publish(exchange, '', Buffer.from(message));
};

const subscribe = async (callback) => {
  const channel = await getChannel();
  channel.assertExchange(exchange, type, {durable: false});
  const qok = await channel.assertQueue('', {exclusive: true});
  channel.bindQueue(qok.queue, exchange, '');
  channel.consume(qok.queue, (msg) => {
    const content = JSON.parse(msg.content.toString());
    callback(content);
  });
};

connect();

module.exports = {
  publish: publish,
  subscribe: subscribe,
};
