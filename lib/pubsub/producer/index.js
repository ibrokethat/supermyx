'use strict';

const amqp = require('amqp');

const exchangeProducers = new Map();

const createExchangeProducer = (cfg, logger, exchangeName) => {

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

          resolve(true);
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

const producer = (cfg, log) => {

  let self = this;

  const logger = require('../../logger')('amqp-producer-pubsub', log);

  const isConnected = (exchangeName) => {

    if (!exchangeProducers.has(exchangeName)) {

      return createExchangeProducer(cfg, logger, exchangeName);
    } else {

      return Promise.resolve(true);
    }
  };

  const doPublish = (exchange, payload) => {

    return new Promise((resolve, reject) => {

      let p = exchange.publish('', payload, cfg.producer.publish);

      p.on('success', () => {console.log('published')

        resolve();
      });

      p.on('error', (err) => {console.log('publish error')

        logger.error({ msg: 'publish error', err });

        reject(err);
      });

      p.emitSuccess();
      p.emitError();
    });
  };

  const publish = (exchangeName, payload) => {

    return isConnected(exchangeName)
      .then(() => {

        let exchange = exchangeProducers.get(exchangeName).exchange;

        return doPublish(exchange, payload);
      })
      .catch((err) => {

        return Promise.reject(new Error('Error publishing: ' + err.message));
      });
  };

  return { publish }
}

module.exports = producer;
