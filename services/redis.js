import Redis from "ioredis";

const redis = new Redis({
//   host: process.env.REDIS_HOST, // private IP
  host: '10.188.103.27', // private IP
  port: 6379,
  maxRetriesPerRequest: null,
  enableReadyCheck: true,
  lazyConnect: false,
});

redis.on("connect", () => {
  console.log("✅ Redis (GCP Memorystore) connected");
});

redis.on("error", (err) => {
  console.error("❌ Redis error", err);
});

export default redis;


