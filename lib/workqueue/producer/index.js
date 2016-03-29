'use strict';

const exchangeManager = require('../../exchangeManager');

const producer = (cfg, log) => {

  const logger = require('../../logger')('amqp-producer-workqueue', log);

  const isConnected = (exchangeName) => {

    return exchangeManager.getExchange(exchangeName).then((exchange) => {

      if (!exchange) {

        return exchangeManager.createExchange(cfg, logger, exchangeName);
      } else {

        return Promise.resolve(exchange);
      }
    });
  };

  const doPublish = (exchange, exchangeName, payload) => {

    return new Promise((resolve, reject) => {

      let p = exchange.publish(exchangeName, payload, cfg.producer.publish);

      p.on('success', () => {

        resolve();
      });

      p.on('error', (err) => {

        logger.error({msg: 'publish error', err});

        reject(err);
      });

      p.emitSuccess();
      p.emitError();
    });
  };

  const publish = (exchangeName, payload) => {

    return isConnected(exchangeName)
      .then((exchange) => {

        return doPublish(exchange, exchangeName, payload);
      })
      .catch((err) => {

        return Promise.reject(new Error('Error publishing: ' + err.message));
      });
  };

  return {publish}
};

module.exports = producer;
