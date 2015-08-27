'use strict';

module.exports = function(options) {
    options = options || {};
    options.host = options.host || 'amqp://localhost';

    return require('./lib/broadcast').bind(undefined, options);
};
