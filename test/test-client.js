'use strict';

const broadcast = require('..')(null, // default connection is localhost
    {
        exchangeName: 'messages'
        // queueName: 'shared.message.queue',
        // queueOptions: {
        //     exclusive: false
        // }
    });

broadcast.on('#', function (topic, message) {
    console.log(`[#] Message to ${topic} received:`, message);
});

broadcast.on('msg.#', function (topic, message) {
    console.log(`[msg.#] Message to ${topic} received:`, message);
});

broadcast.on('msg.*.yolo', function (topic, message) {
    console.log(`[msg.*.yolo] Message to ${topic} received:`, message);
});

broadcast.on('#.yolo', function (topic, message) {
    console.log(`[#.yolo] Message to ${topic} received:`, message);
});

broadcast.on('*.*.yolo', function (topic, message) {
    console.log(`[*.*.yolo] Message to ${topic} received:`, message);
});

broadcast.post('msg.test.yolo', {
    test: 'yolo'
});

process.on('SIGHUP', () => {
    console.log('HUP received. Shutdown...');
    broadcast.shutdown()
        .then(() => {
            console.log('Closed');
        })
        .catch(function (err) {
            console.log(err);
        });
});
