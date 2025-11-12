module.exports = {
  apps: [
    {
      name: 'tg-ref-earn-bot',
      script: './bot.js',
      instances: 1,
      restart_delay: 5000,
      env: {
        NODE_ENV: 'production'
      }
    }
  ]
};
