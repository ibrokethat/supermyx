'use strict';

const isString = require('lodash').isString;
const amqp = require('amqp');


function ExchangeManager() {

  const exchanges = new Map();

  const _getFullExchangeName = (cfg, exchangeName) => {

    let fullExchangeName;

    switch (cfg.producer.exchange.options.type) {
      case 'fanout':
        fullExchangeName = exchangeName.replace(/\//g, '.') + '.pubsub';
        break;
      case 'direct':
        fullExchangeName = exchangeName.replace(/\//g, '.') + '.workqueue';
        break;
      default:
        break;
    }

    return fullExchangeName;
  };

  const _setupDirectExchange = (cfg, logger, connection, exchange, exchangeName, isSetup) => {

    return new Promise((resolve, reject) => {

      const fullExchangeName = _getFullExchangeName(cfg, exchangeName);

      const deadExchangeName = fullExchangeName + '.dead';
      const queueOptions = Object.assign({}, cfg.producer.queue.options, {arguments: {"x-dead-letter-exchange": deadExchangeName}});
      const queue = connection.queue(exchangeName, queueOptions);

      queue.on('queueDeclareOk', () => {

        logger.info({msg: 'queue created'});

        queue.bind(fullExchangeName, exchangeName);

        queue.once('queueBindOk', () => {

          logger.info({msg: 'queue bound'});

          resolve(true);
        });
      });
    });
  };

  const createExchange = (cfg, logger, exchangeName) => {

    return new Promise((resolve, reject) => {

      let connection,
        exchange,
        isSetup = true;

      if (cfg.implOptions.reconnect !== true) {

        logger.error({msg: 'configuration error: implementation options - reconnect must be true'});
        reject(new Error('Configuration Error'));
        return;
      }

      connection = amqp.createConnection(cfg.options, cfg.implOptions);

      connection.on('ready', () => {

        logger.info({msg: 'connection ready', data: cfg});

        if (isSetup) {

          let exchangeFullName = _getFullExchangeName(cfg, exchangeName),
            promise;

          if (!exchangeFullName) {

            reject(new Error("Unknown Exchange type"));
            return;
          }

          exchange = connection.exchange(exchangeFullName, cfg.producer.exchange.options, () => {

            switch (cfg.producer.exchange.options.type) {
              case 'fanout':
                promise = Promise.resolve();
                break;
              case 'direct':
                promise = _setupDirectExchange(cfg, logger, connection, exchange, exchangeName, isSetup);
                break;
              default:
                reject(new Error("Unknown Exchange type"));
                return;
            }

            promise
              .then(() => {

                isSetup = false;

                //Instance of the connection is available via the exchange, so no need to store it explicitly
                exchanges.set(exchangeName, exchange);

                resolve(exchange);
              })
              .catch((err) => {

                reject(err);
              });
          });

          exchange.once('exchangeDeclareOk', () => {

            logger.info({msg: 'exchange ready'});

          });

          exchange.on('error', (err) => {

            logger.error({msg: 'exchange error', err});

            if (isSetup) {

              reject(err);
            }
          });
        }
      });

      connection.on('error', (err) => {

        if (err.code !== 'ECONNRESET') {

          logger.error({msg: 'connection error', err});
        }

        if (isSetup) {

          reject(err);
        }
      });

      connection.on('heartbeat', () => {

        logger.info({msg: 'connection heartbeat'});
      });

      connection.on('close', () => {

        logger.info({msg: 'connection closing'});
      });
    })
  };

  const getExchange = (exchangeName) => {

    return new Promise((resolve, reject) => {

      if (!isString(exchangeName)) {

        reject(new Error("Exchange name must be a string"));
        return;
      }

      let exchange = exchanges.get(exchangeName);

      if (!exchange) {

        resolve();
        return;
      }

      resolve(exchange);
    });
  };

  return {
    createExchange,
    getExchange
  }
}

const exchangeManager = new ExchangeManager();

module.exports = (() => {

  return exchangeManager;
})();
