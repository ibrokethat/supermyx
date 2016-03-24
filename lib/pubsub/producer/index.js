'use strict';

const amqp = require('amqp');

const producers = new Map();

function producer(cfg, log) {

    let connection,
        exchange;

    let self = this;

    const logger = require('../../logger')('amqp-producer-pubsub', log);

    const init = (name) => {

        console.log('init', name);

        return new Promise((resolve, reject) => {

            if (!connection) {

                const exchangeName = name.replace(/\//g,'.') + '.pubsub';

                connection = amqp.createConnection(cfg.options, cfg.implOptions);

                connection.on('ready', () => {

                    logger.info({ msg: 'connection ready', data: cfg });
                    logger.info({ msg: 'exchange', data: exchangeName });

                    exchange = connection.exchange(exchangeName, cfg.producer.exchange.options);

                    exchange.on('exchangeDeclareOk', () => {
                        logger.info({ msg: 'exchange ready'});
                        console.log("producer.publish");

                        producers.set(name, self);
                        resolve();
                    });
                });

                connection.on('error', (err) => {
                    console.log('errrrrrrrr', err);
                    if (err.code !== 'ECONNRESET') {
                        logger.error({ msg: err });
                    }
                    producers.delete(name);
                    console.log('removed name from map');
                    close(connection);
                });

                connection.on('heartbeat', () => {
                    logger.info({ msg: 'heartbeat' });
                    console.log('heartbeat');
                });

                connection.on('close', () => {
                    //logger.info({ msg: 'connection closing' });
                    console.log("closeeeeeeeeeeeeeeeeeeeee");
                    connection = null;
                    reject();
                });
            }
        });
    };

    const isConnected = (name) => {

        if (!producers.has(name)) {

            //TODO: Handle errors on failure to create connection or assert exchange
            return init(name);
        } else {

            return Promise.resolve(true);
        }
    };

    const publish = (name, payload) => {

        return isConnected(name)
            .then(() => {

                return new Promise((resolve, reject) => {

                    //TODO: Look for how to handle errors in exchange.publish (to call the 'reject' function)

                    console.log('before published');

                    exchange.publish('', payload, cfg.producer.publish);

                    console.log('published');

                    resolve();
                });
            });
    };

    const close = (connection) => {
        try {
            if (connection && connection.disconnect) {
                console.log('About to disconnect', connection.disconnect)
                connection.disconnect();

            }
        }
        catch (e) {
            //logger.error(logName, 'error', { msg: e });
            console.log('An error ssssssss', e);
        }
    }

    return { publish, init }
}

module.exports = producer;
