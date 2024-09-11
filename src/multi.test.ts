import { formatWithOptions } from "util";
import test, { ExecutionContext } from "ava";
import type {
  RedisClientType as Client,
  RedisClusterType as Cluster,
} from "redis";
import { createClient, createCluster } from "redis";
import Redlock, { ExecutionError } from "./index.js";

async function fail(
  t: ExecutionContext<unknown>,
  error: unknown
): Promise<void> {
  if (!(error instanceof ExecutionError)) {
    throw error;
  }

  t.fail(`${error.message}
---
${(await Promise.all(error.attempts))
      .map(
        (s, i) =>
          `ATTEMPT ${i}: ${formatWithOptions(
            { colors: true },
            {
              membershipSize: s.membershipSize,
              quorumSize: s.quorumSize,
              votesForSize: s.votesFor.size,
              votesAgainstSize: s.votesAgainst.size,
              votesAgainstError: s.votesAgainst.values(),
            }
          )}`
      )
      .join("\n\n")}
`);
}

async function redisConnect(
  name: string,
  redis: Client | Cluster
): Promise<void> {
  await redis
    .on("ready", () => {
      console.log(`Redis "${name}" is ready`);
    })
    .on("end", () => {
      console.log(`Redis "${name}" connection ended`);
    })
    .on("error", async (error) => {
      console.log(`Redis "${name}" error:`, error);
    })
    .connect();
}

function run(
  namespace: string,
  redisA: Client | Cluster,
  redisB: Client | Cluster,
  redisC: Client | Cluster
): void {
  test.before(async () => {
    await Promise.all([
      redisConnect("Client A", redisA),
      redisConnect("Client B", redisB),
      redisConnect("Client C", redisC),
    ]);
  });

  test.before(async () => {
    await Promise.all([
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      /* @ts-ignore */
      redisA.flushAll ? redisA.flushAll : null,
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      /* @ts-ignore */
      redisB.flushAll ? redisB.flushAll : null,
      // eslint-disable-next-line @typescript-eslint/ban-ts-comment
      /* @ts-ignore */
      redisC.flushAll ? redisC.flushAll : null,
    ]);
  });

  test(`${namespace} - acquires, extends, and releases a single lock`, async (t) => {
    try {
      const redlock = new Redlock([redisA, redisB, redisC]);

      const duration = Math.floor(Number.MAX_SAFE_INTEGER / 10);

      // Acquire a lock.
      let lock = await redlock.acquire(["{redlock}a"], duration);
      t.is(
        await redisA.get("{redlock}a"),
        lock.value,
        "The lock value was incorrect."
      );
      t.is(
        await redisB.get("{redlock}a"),
        lock.value,
        "The lock value was incorrect."
      );
      t.is(
        await redisC.get("{redlock}a"),
        lock.value,
        "The lock value was incorrect."
      );
      t.is(
        Math.floor((await redisA.pTTL("{redlock}a")) / 200),
        Math.floor(duration / 200),
        "The lock expiration was off by more than 200ms"
      );
      t.is(
        Math.floor((await redisB.pTTL("{redlock}a")) / 200),
        Math.floor(duration / 200),
        "The lock expiration was off by more than 200ms"
      );
      t.is(
        Math.floor((await redisC.pTTL("{redlock}a")) / 200),
        Math.floor(duration / 200),
        "The lock expiration was off by more than 200ms"
      );

      // Extend the lock.
      lock = await lock.extend(3 * duration);
      t.is(
        await redisA.get("{redlock}a"),
        lock.value,
        "The lock value was incorrect."
      );
      t.is(
        await redisB.get("{redlock}a"),
        lock.value,
        "The lock value was incorrect."
      );
      t.is(
        await redisC.get("{redlock}a"),
        lock.value,
        "The lock value was incorrect."
      );
      t.is(
        Math.floor((await redisA.pTTL("{redlock}a")) / 200),
        Math.floor((3 * duration) / 200),
        "The lock expiration was off by more than 200ms"
      );
      t.is(
        Math.floor((await redisB.pTTL("{redlock}a")) / 200),
        Math.floor((3 * duration) / 200),
        "The lock expiration was off by more than 200ms"
      );
      t.is(
        Math.floor((await redisC.pTTL("{redlock}a")) / 200),
        Math.floor((3 * duration) / 200),
        "The lock expiration was off by more than 200ms"
      );

      // Release the lock.
      await lock.release();
      t.is(await redisA.get("{redlock}a"), null);
      t.is(await redisB.get("{redlock}a"), null);
      t.is(await redisC.get("{redlock}a"), null);
    } catch (error) {
      fail(t, error);
    }
  });
}

run(
  "instance",
  createClient({ url: "redis://redis-multi-instance-a" }),
  createClient({ url: "redis://redis-multi-instance-b" }),
  createClient({ url: "redis://redis-multi-instance-c" })
);

run(
  "cluster",
  createCluster({ rootNodes: [{ url: "redis://redis-multi-cluster-a-1" }] }),
  createCluster({ rootNodes: [{ url: "redis://redis-multi-cluster-b-1" }] }),
  createCluster({ rootNodes: [{ url: "redis://redis-multi-cluster-b-1" }] })
);
