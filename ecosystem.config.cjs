module.exports = {
  apps: [
    {
      name: 'ycbot-ws-relay',
      script: 'app.js',
      instances: 1,
      exec_mode: 'fork',
      autorestart: true,
      max_memory_restart: '200M',
      env: {
        NODE_ENV: 'production',
        PORT: 8080,
        LOG_LEVEL: 'info',
        // Streams kept hot at boot so first subscriber sees messages immediately.
        // Mirrors the SYMBOLS list in src/components/AiHedgeStrategy.tsx — keep in sync.
        WARM_STREAMS: 'btcusdt@markPrice@1s,ethusdt@markPrice@1s,solusdt@markPrice@1s,bnbusdt@markPrice@1s,xauusdt@markPrice@1s,xagusdt@markPrice@1s,clusdt@markPrice@1s',
      },
      error_file: './logs/err.log',
      out_file: './logs/out.log',
      log_date_format: 'YYYY-MM-DDTHH:mm:ss',
      merge_logs: true,
    },
  ],
};
