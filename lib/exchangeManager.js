'use strict';

const isString = require('lodash').isString;
const amqp = require('amqp');

function ExchangeManager() {

    const exchangeProducers = new Map();

    const createExchange = (cfg, logger, exchangeName) => {

        return new Promise((resolve, reject) => {

            let connection,
                exchange,
                isSetup = true;

            if (cfg.implOptions.reconnect !== true) {

                logger.error({ msg: 'configuration error: implementation options - reconnect must be true'});
                reject(new Error('Configuration Error'));
                return;
            }

            const exchangeFullName = exchangeName.replace(/\//g,'.') + '.pubsub';

            connection = amqp.createConnection(cfg.options, cfg.implOptions);

            connection.on('ready', () => {console.log('connection ready')

                logger.info({ msg: 'connection ready', data: cfg });

                if (isSetup) {
                    exchange = connection.exchange(exchangeFullName, cfg.producer.exchange.options, () => {

                        isSetup = false;

                        exchangeProducers.set(exchangeName, {
                            connection,
                            exchange,
                            exchangeFullName
                        });

                        resolve(exchange);
                    });

                    exchange.once('exchangeDeclareOk', () => {

                        logger.info({ msg: 'exchange ready'});
                        console.log('exchange ready')
                    });

                    exchange.on('error', (err) => {

                        logger.error({ msg: 'exchange error', err });
                        console.log('exchange error')

                        if (isSetup) {

                            reject(err);
                        }
                    });
                }
            });

            connection.on('error', (err) => {console.log('connection error')

                if (err.code !== 'ECONNRESET') {

                    logger.error({ msg: 'connection error', err });
                }

                if (isSetup) {

                    reject(err);
                }
            });

            connection.on('heartbeat', () => {

                logger.info({ msg: 'connection heartbeat' });
            });

            connection.on('close', () => {console.log('connection close')

                logger.info({ msg: 'connection closing' });
            });
        })
    };

    const getExchange = (exchangeName) => {

        return new Promise((resolve,reject) => {

            if (!isString(exchangeName)) {

                reject(new Error("Exchange name must be a string"));
                return;
            }

            let exchangeProducer = exchangeProducers.get(exchangeName);

            if (!exchangeProducer) {

                resolve();
                return;
            }

            resolve(exchangeProducer.exchange);
        });
    }

    return {
        createExchange,
        getExchange
    }
}

const exchangeManager = new ExchangeManager();

module.exports = (() => {

    return exchangeManager;
})();
