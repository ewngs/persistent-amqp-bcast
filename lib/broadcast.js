'use strict';

const EventEmitter = require('events').EventEmitter;
const persistentAMQP = require('persistent-amqp');
const bson = require('bson');
const BSON = bson.BSONPure.BSON;
const VError = require('verror');

const debug = require('debug');
const logError = debug('bcast:error');
const logDebug = debug('bcast:debug');
logDebug.log = console.log.bind(console);

let exchanges = {};
let clientCount = 0;

class ExchangeWrapper extends EventEmitter {

    constructor(amqpConnection, options) {
        super();
        const self = this;

        this.options = options;
        this.pendingPosts = [];
        this.binds = [];
        this.topicRegexps = {};

        this.lastMessages = {};

        this.channel = amqpConnection.createChannel();
        this.channel.addOpenHook(this.registerQueues.bind(this));
        this.channel.addCloseHook(this.cleanupQueues.bind(this));
        this.channel.on('open', this.processPendingPosts.bind(this));
        amqpConnection.on('disconnect', () => {
            if (self.resolveShutdown) {
                self.resolveShutdown();
            }
        });
        logDebug('Broadcast', `Exchange wrapper to "${self.options.exchangeName}" created`);
    }

    registerQueues() {
        const self = this;

        logDebug('Broadcast', `Creating exchange "${self.options.exchangeName}" with options:`, self.options.exchangeOptions);
        return self.channel
            .assertExchange(self.options.exchangeName, 'topic', self.options.exchangeOptions)
            .then(() => self.bindQueues());
    }

    cleanupQueues() {
        if (!this.options.queueOptions.exclusive) {
            return Promise.resolve();
        }
        logDebug('Broadcast', `Deleting temporary queue "${this.messageQueueName}"`);
        return this.channel.deleteQueue(this.messageQueueName);
    }

    bindQueues() {
        const self = this;

        if (this.binds.length === 0) {
            return undefined;
        }

        return self.channel
            .assertQueue(self.options.queueName, self.options.queueOptions)
            .then(messageQueueData => {
                if (self.options.prefetch) {
                    self.prefetch(self.options.prefetch);
                }
                self.messageQueueName = messageQueueData.queue;
                logDebug('Broadcast', `Qeueue "${self.messageQueueName}" created, consuming messages with options:`, self.options.consumeOptions);
                return self.channel.consume(self.messageQueueName, self.processMessage.bind(self), self.options.consumeOptions);
            })
            .then(() => {
                logDebug('Broadcast', `Binding ${self.binds.length} topic patterns from exchange "${self.options.exchangeName}" to queue "${self.messageQueueName}"`);
                const promises = self.binds.map(topicPattern => self.channel.bindQueue(self.messageQueueName, self.options.exchangeName, topicPattern));
                return Promise.all(promises);
            });
    }

    processMessage(message) {
        let data;
        const self = this;
        const routingKey = message.fields.routingKey;

        try {
            data = new BSON().deserialize(message.content);
        } catch(err) {
            logError(`AMQP Broadcast message parsing error in ${this.messageQueueName}`);
            this.checkShutdown();
            return;
        }

        this.binds.forEach(topicPattern => {
            if (routingKey.match(self.topicRegexps[topicPattern])) {
                self.lastMessages[topicPattern] = message;
                self.emit(topicPattern, routingKey, data);
            }
        });

        this.checkShutdown();
    }

    post(topic, rawData) {
        let data;
        try {
            data = new BSON().serialize(rawData, false, true);
        } catch (err) {
            throw new VError(err, 'Broadcast message preparation error.');
        }
        this.pendingPosts.push({ topic, data });
        this.processPendingPosts();
    }

    ack(topic) {
        const message = this.lastMessages[topic];
        if (message) {
            delete this.lastMessages[topic];
            this.channel.ack(message);
        }
    }

    processPendingPosts() {
        if (!this.channel.open) {
            return;
        }

        const self = this;

        this.pendingPosts.forEach(post => {
            try {
                self.channel.publish(self.options.exchangeName, post.topic, post.data);
            } catch (err) {
                throw new VError(err, 'Broadcast message cannot be delivered.');
            }
        });

        this.pendingPosts = [];
    }

    on(topicPattern, callback) {
        EventEmitter.prototype.on.call(this, topicPattern, callback);

        if (this.binds.indexOf(topicPattern) === -1) {
            this.binds.push(topicPattern);
            this.topicRegexps[topicPattern] = this.createRegexpFromPattern(topicPattern);
        }

        if (this.channel.open) {
            this.channel.bindQueue(this.messageQueueName, this.exchangeName, topicPattern);
        }
    }

    createRegexpFromPattern(pattern) {
        const expr = '^' + pattern.replace(/\./g, '\\.').replace(/\*/g, '([\\w|-]+)').replace(/#/g, '([\\w|\\.|-]*)') + '$';
        // console.log(pattern, '->', expr);
        return new RegExp(expr, 'gi');
    }

    checkShutdown() {
        if (this.resolveShutdown && this.pendingPosts.length === 0) {
            this.channel.close();
        }
    }

    shutdown() {
        const self = this;

        logDebug('Broadcast', 'Shutdown requested');
        return new Promise(resolve => {
            self.resolveShutdown = resolve;

            if (!self.channel.open || self.pendingPosts.length === 0) {
                self.channel.close();
                return;
            }
        });
    }

}

module.exports = function (connectionOptions, exchangeOptions) {
    connectionOptions = connectionOptions || {};
    connectionOptions.host = connectionOptions.host || 'amqp://localhost';
    exchangeOptions = exchangeOptions || { exchangeName: 'messages' };
    if (typeof exchangeOptions === 'string') {
        exchangeOptions = { exchangeName: exchangeOptions };
    }
    exchangeOptions.exchangeOptions = exchangeOptions.exchangeOptions || { durable: false };
    exchangeOptions.queueName = exchangeOptions.queueName || '';
    exchangeOptions.queueOptions = exchangeOptions.queueOptions || { exclusive: true };
    exchangeOptions.consumeOptions = exchangeOptions.consumeOptions || { noAck: true };

    const queueName = exchangeOptions.queueName.length > 0 ? exchangeOptions.queueName : String(clientCount++);
    const exchangeName = exchangeOptions.exchangeName + '#' + queueName;

    if (!exchanges[exchangeName]) {
        const amqpConnection = persistentAMQP.connection(connectionOptions);
        exchanges[exchangeName] = new ExchangeWrapper(amqpConnection, exchangeOptions);
    }

    return exchanges[exchangeName];
};
