const Ioredis = require('ioredis');
const { default: Redlock } = require('redlock');
const EventEmitter = require('events');

class Stm extends EventEmitter {
  constructor(redisUrl) {
    super();
    this.redis = new Ioredis(redisUrl);
    this.prefixes = {
      topic: 'topic',
      data: 'data',
      channel: 'channel',
      lock: 'lock',
    };
    this.existingChannels = {};
    this.subscribers = [];
    this.locks = {};
    this.redis.on('connect', this.onConnected.bind(this));
    this.redis.on('error', this.onError.bind(this));
    this.redis.on('reconnecting', this.onReconnecting.bind(this));
  }

  onReconnecting(args) {

  }

  onError(error) {

  }

  onConnected() {

  }

  async connect() {
    this.redlock = new Redlock(
      [this.redis],
      {
        driftFactor: 0.01,
        retryCount: 40,
        retryDelay: 200,
        retryJitter: 200,
        automaticExtensionThreshold: 500,
      },
    );

    this.connected = true;
  }

  async get(k) {
    if (!this.connected) {
      await this.connect();
    }
    const dataKey = `${this.prefixes.data}:${k}`;
    const value = await this.redis.get(`${this.prefixes.data}:${k}`);
    if (!value || value == '') {
      return null;
    }
    return JSON.parse(value);
  }

  async lock(k, time = 250) {
    if (!this.connected) {
      await this.connect();
    }
    const lockKey = `${this.prefixes.lockl}:${k}`;
    if (this.locks[lockKey]) {
      return;
    }

    this.locks[lockKey] = await this.redlock.acquire([lockKey], time);
  }

  async withLock(k, process, time = 250) {
    await this.lock(k, time);
    let result = null;
    const currentValue = await this.get(k);
    try {
      result = await process(currentValue);
      await this.set(k, result);
    } catch (e) {
      console.log(e);
    }
    await this.release(k);
    return result;
  }

  async release(k) {
    if (!this.connected) {
      await this.connect();
    }
    const lockKey = `${this.prefixes.lockl}:${k}`;
    if (this.locks[lockKey]) {
      try {
        await this.locks[lockKey].release();
      } catch (e) {

      }
      this.locks[lockKey] = null;
      delete this.locks[lockKey];
    }
  }

  async set(k, v) {
    if (!this.connected) {
      await this.connect();
    }
    const dataKey = `${this.prefixes.data}:${k}`;
    const value = JSON.stringify(v);
    await this.redis.set(dataKey, value);
  }

  async has(k) {
    if (!this.connected) {
      await this.connect();
    }
    const dataKey = `${this.prefixes.data}:${k}`;
    const v = await this.redis.get(dataKey);
    return v && v != '';
  }

  async rem(k) {
    if (!this.connected) {
      await this.connect();
    }
    const dataKey = `${this.prefixes.data}:${k}`;
    await this.redis.set(dataKey, '');
  }

  async disconnect(force = false) {
    await this.redis.quit();
    for (let i = 0; i < this.subscribers.length; i++) {
      try {
        if (!this.subscribers[i].disconnected) {
          this.subscribers[i].disconnected = true;
          await this.subscribers[i].subscriber.quit();
        }
      } catch (e) {

      }
    }
    this.connected = false;
  }

  async push(topic, message) {
    const topicKey = `${this.prefixes.topic}:${topic}`;
    const data = JSON.stringify(message);
    await this.redis.rpush([topicKey, data]);
  }

  async pop(topic) {
    const topicKey = `${this.prefixes.topic}:${topic}`;
    const value = await this.redis.lpop([topicKey]);
    if (value && value != '') {
      return JSON.parse(value);
    }
    return null;
  }

  async broadcast(channel, data) {
    const channelKey = `${this.prefixes.channel}:${channel}`;
    const message = JSON.stringify(data);
    await this.redis.publish(channelKey, message);
  }

  subscription(channel, process, subscriber) {
    return async (err) => {
      // const data = JSON.parse(message);
      if (err) {
        console.log(`Errro when subscribing to ${channel}`, err);
        return;
      }
      subscriber.on('message', async (c, message) => {
        const data = JSON.parse(message);
        await process(data, channel);
      });
    };
  }

  async subscribe(channel, process) {
    if (this.existingChannels[channel]) {
      throw new Error('already subscribed');
    }
    const channelKey = `${this.prefixes.channel}:${channel}`;
    const subscriber = this.redis.duplicate();

    this.subscribers.push({ channel, subscriber });
    await subscriber.subscribe(channelKey, this.subscription(channel, process, subscriber).bind(this));
  }

  async unsubscribe(channel) {
    const channelKey = `${this.prefixes.channel}:${channel}`;
    for (let i = 0; i < this.subscribers[i].length; i++) {
      if (this.subscribers[i].channel == channel && !this.subscribers[i].disconnected) {
        this.subscribers[i].disconnected = true;
        await this.subscribers[i].subscriber.unsubscribe(channelKey);
        await this.subscribers[i].subscriber.quit();
        this.existingChannels[channel] = null;
        delete this.existingChannels[channel];
      }
    }
  }
}

module.exports = Stm;
