import itertools
import logging
import math
import threading
from contextlib import contextmanager
from typing import List, Optional

from mysql.connector import Error
from mysql.connector.pooling import MySQLConnectionPool
from anyio import to_thread, Semaphore

logger = logging.getLogger("mysql")

MAX_POOL_SIZE = 32  # mysql-connector-python hard cap per pool


class MySQL:
    _pools: List[MySQLConnectionPool] = []
    _pool_cycle = None
    _pool_lock = threading.Lock()   # itertools.cycle isn't thread-safe; guard the round-robin
    _semaphore: Semaphore = Semaphore(5)
    _cfg: dict = {}

    @classmethod
    def configure(cls, **cfg):
        cls._cfg.update(cfg)
        total = cls._cfg.get("pool_size", 5)
        cls._semaphore = Semaphore(total)

    @classmethod
    def _build_pools(cls):
        if not cls._cfg:
            raise RuntimeError("MySQL not configured. Call db.configure(**cfg) first.")

        total = cls._cfg.get("pool_size", 5)
        n_pools = math.ceil(total / MAX_POOL_SIZE)
        per_pool = math.ceil(total / n_pools)
        base_name = cls._cfg.get("pool_name", "kopilot")
        base_cfg = {k: v for k, v in cls._cfg.items() if k not in ("pool_name", "pool_size")}

        cls._pools = []
        for i in range(n_pools):
            pool_cfg = {**base_cfg, "pool_name": f"{base_name}_{i}", "pool_size": per_pool}
            cls._pools.append(MySQLConnectionPool(**pool_cfg))

        cls._pool_cycle = itertools.cycle(cls._pools)
        logger.info(
            "Initialized %d MySQL pool(s) of %d connections each (%d total)",
            n_pools, per_pool, n_pools * per_pool,
        )

    @classmethod
    def _get_pool(cls) -> MySQLConnectionPool:
        # Runs in to_thread worker threads (via aexecute_*), so both the lazy build
        # and next(_pool_cycle) must be serialized. The lock covers only the cheap
        # round-robin pick; the blocking pool.get_connection() stays outside it.
        with cls._pool_lock:
            if not cls._pools:
                cls._build_pools()
            return next(cls._pool_cycle)

    @classmethod
    @contextmanager
    def connection(cls):
        pool = cls._get_pool()
        con = None
        try:
            con = pool.get_connection()
            yield con
        except Error as e:
            logger.error(f"Database error: {e}")
            if con:
                con.rollback()
            raise
        finally:
            if con and con.is_connected():
                con.close()

    @classmethod
    def execute_query(cls, query, params=None, fetch_one=False):
        with cls.connection() as con:
            cursor = None
            try:
                cursor = con.cursor(dictionary=True)
                cursor.execute(query, params or ())
                if fetch_one:
                    result = cursor.fetchone()
                    logger.debug(f"Query (fetch_one): {query[:100]}...")
                    return result
                result = cursor.fetchall()
                logger.debug(f"Query: {query[:100]}... | rows: {len(result)}")
                return result
            finally:
                if cursor:
                    cursor.close()

    @classmethod
    def execute_update(cls, query, params=None):
        with cls.connection() as con:
            cursor = None
            try:
                cursor = con.cursor(buffered=True)
                cursor.execute(query, params or ())
                con.commit()
                affected = cursor.rowcount
                logger.debug(f"Update: {query[:100]}... | affected: {affected}")
                return affected
            finally:
                if cursor:
                    cursor.close()

    @classmethod
    def execute_insert(cls, query, params=None):
        with cls.connection() as con:
            cursor = None
            try:
                cursor = con.cursor()
                cursor.execute(query, params or ())
                con.commit()
                last_id = cursor.lastrowid
                logger.debug(f"Insert: {query[:100]}... | last_id: {last_id}")
                return last_id
            finally:
                if cursor:
                    cursor.close()

    @classmethod
    def execute_many(cls, query, params_list):
        with cls.connection() as con:
            cursor = None
            try:
                cursor = con.cursor()
                cursor.executemany(query, params_list)
                con.commit()
                affected = cursor.rowcount
                logger.debug(f"Bulk: {query[:100]}... | affected: {affected}")
                return affected
            finally:
                if cursor:
                    cursor.close()

    @classmethod
    async def aexecute_query(cls, query, params=None, fetch_one=False):
        async with cls._semaphore:
            return await to_thread.run_sync(cls.execute_query, query, params, fetch_one)

    @classmethod
    async def aexecute_update(cls, query, params=None):
        async with cls._semaphore:
            return await to_thread.run_sync(cls.execute_update, query, params)

    @classmethod
    async def aexecute_insert(cls, query, params=None):
        async with cls._semaphore:
            return await to_thread.run_sync(cls.execute_insert, query, params)

    @classmethod
    async def aexecute_many(cls, query, params_list):
        async with cls._semaphore:
            return await to_thread.run_sync(cls.execute_many, query, params_list)


db = MySQL