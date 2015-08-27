'use strict';

const persistentAMQP = require('persistent-amqp');
const bson = require('bson');
const BSON = bson.BSONPure.BSON;
// const VError = require('verror');

let exchanges = {};

class ExchangeWrapper {

    constructor(amqpConnection, exchangeName, options) {
        const self = this;

        this.options = options;
        this.exchangeName = exchangeName;
        this.pendingPosts = {};

        this.channel = amqpConnection.createChannel();
        this.channel.addOpenHook(this.registerQueues.bind(this));
        this.channel.addCloseHook(this.cleanupQueues.bind(this));
        // this.channel.on('close', this.rejectPendingProcedures.bind(this));
        // this.channel.on('open', this.processPendingProcedures.bind(this));
        amqpConnection.on('disconnect', () => {
            if (self.resolveShutdown) {
                self.resolveShutdown();
            }
        });

    }

    registerQueues() {
        const self = this;

        return this.channel.assertExchange(this.exchangeName, 'topic', {durable: false})
            .then(() => self.channel.assertQueue('', { exclusive: true }))
            .then(messageQueueData => {
                self.messageQueueName = messageQueueData.queue;
                self.channel.consume(self.messageQueueName, self.processMessage.bind(self), {noAck: true});
            });
    }

    cleanupQueues() {
        return this.channel.deleteQueue(this.messageQueueName);
    }

    processMessage(message) {
        let data;

        try {
            data = new BSON().deserialize(message.content);
        } catch(err) {
            console.error(`AMQP Broadcast message parsing error in ${this.messageQueueName}`);
            return;
        }

        console.log('Mesage:', message.fields.routingKey, data);
    }

    post() {

    }

    on() {

    }

    checkShutdown() {
        if (this.resolveShutdown && Object.keys(this.pendingPosts).length === 0) {
            this.channel.close();
        }
    }

    shutdown() {

    }

}

module.exports = function (options, exchangeName) {
    options = options || {};
    options.host = options.host || 'amqp://localhost';
    exchangeName = exchangeName || 'messages';

    if (!exchanges[exchangeName]) {
        const amqpConnection = persistentAMQP.connection(options);
        exchanges[exchangeName] = new ExchangeWrapper(amqpConnection, exchangeName, options);
    }

    return exchanges[exchangeName];
};
