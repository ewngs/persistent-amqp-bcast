'use strict';

const broadcast = require('..')({}, 'messages');

broadcast.on('msg.#', function (message) {
    console.log('Message received:', message);
});

process.on('SIGHUP', () => {
    console.log('HUP received. Shutdown...');
    broadcast.shutdown();
        // .then(() => {
        //     console.log('Closed');
        // })
        // .catch(function (err) {
        //     console.log(err);
        // });
});
