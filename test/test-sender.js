'use strict';

const broadcast = require('..')(null, // default connection is localhost
    {
        exchangeName: 'messages',
        queueName: 'shared.message.queue',
        queueOptions: {
            exclusive: false
        }
    });

broadcast.post('msg.test.lolol', {
    test: 'lolol'
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
