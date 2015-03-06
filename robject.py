# -*- coding: utf-8 -*-
# ==============================================================================
# Copyright:    Zhen Wang
# Licence:      MIT
#==============================================================================

"""
Redis object mapper
"""

# TODO: Should return ProxyContainer instead of always dirty marking containers

import hashlib
import unittest
import redis
import greenlet
import pickle

from contextlib import contextmanager
from threading import local
from time import sleep
from uuid import uuid1


class Exceptions(object):
    class TimeoutError(Exception):
        pass

    class TransactionExpiredError(Exception):
        pass

    class ReadOnlyError(Exception):
        pass

    class AlreadyAcquiredError(Exception):
        pass


class Entity(object):
    class _Internal(object):
        def __init__(self, db, entity_id, writable=False, autolock=True):
            self.db = db
            self.entity_id = entity_id
            self.writable = writable
            self.autolock = autolock

            self.read_lock = hashlib.sha256(
                entity_id.encode('utf-8')).hexdigest() + 'r'
            self.write_lock = hashlib.sha256(
                entity_id.encode('utf-8')).hexdigest() + 'w'

            self.tx_id = None
            self.dirty = set()

            if self.entity_id in self.db.local.locked:
                raise Exceptions.AlreadyAcquiredError(self.entity_id)

            if autolock:
                self.lock()

        def unlock(self):
            self.db.local.locked.remove(self.entity_id)

        def lock(self):
            self.db.local.locked.add(self.entity_id)

    @classmethod
    def from_ids(cls, f):
        def _f(db, *entity_ids, autolock=True, **kwargs):
            entities = []
            try:
                entities = [
                    Entity(db, i, autolock=autolock) for i in entity_ids]
                yield from f(db, *entities, **kwargs)
            finally:
                if autolock:
                    for e in entities:
                        e._internal.unlock()
        return _f

    def __init__(self, *args, **kwargs):
        object.__setattr__(self, '_internal', Entity._Internal(*args, **kwargs))

    def __getattr__(self, name):
        value = self._internal.db.redis.hget(self._internal.entity_id, name)
        if value:
            value = pickle.loads(value)

            # Always mark collections as dirty
            if isinstance(value, list) or isinstance(value, dict):
                self._internal.dirty.add(name)

        object.__setattr__(self, name, value)
        return value

    def __setattr__(self, name, value):
        if not self._internal.writable:
            raise Exceptions.ReadOnlyError()
        self._internal.dirty.add(name)
        object.__setattr__(self, name, value)


class Database(object):
    def __init__(self, *args, **kwargs):
        self.redis = redis.StrictRedis(*args, **kwargs)
        self.local = local()
        self.local.locked = set()
        self.local.tx_level = 0
        self.local.read_entities = {}
        self.local.write_entities = {}

    @contextmanager
    @Entity.from_ids
    def read(self, *entities, timeout=1000, poll=20, expire=100):
        tx_id = str(uuid1()).encode('utf-8')
        tx = self.redis.pipeline()
        write_locks = [e._internal.write_lock for e in entities]
        g_parent = greenlet.getcurrent().parent

        for _ in range(poll):
            tx.watch(write_locks)

            # Wait for write lock release
            if any(self.redis.mget(*write_locks)):
                sleep_duration = 1.0 * timeout / poll
                tx.reset()
                if g_parent:
                    g_parent.switch(sleep_duration)
                else:
                    sleep(sleep_duration / 1000.0)
                continue

            # Set read locks
            tx.multi()
            for e in entities:
                tx.hset(e._internal.read_lock, tx_id, 1)
                tx.set(tx_id, 1, px=expire)

            try:
                with tx:
                    tx.execute()
                break
            except redis.WatchError:
                # Write lock was acquired elsewhere
                tx.reset()
        else:
            raise Exceptions.TimeoutError()

        # Successfully set locks for read tx
        for e in entities:
            e._internal.tx_id = tx_id

        self.local.read_entities[tx_id] = entities
        self.local.tx_level += 1

        try:
            if len(entities) == 1:
                yield entities[0]
            else:
                yield entities
        except Exception:
            self.release(flush=False)
            raise
        else:
            self.release(flush=True)

    @contextmanager
    @Entity.from_ids
    def write(self, *entities, timeout=1000, poll=20, expire=100):
        for e in entities:
            e._internal.writable = True

        tx_id = str(uuid1()).encode('utf-8')
        tx = self.redis.pipeline()
        write_locks = [e._internal.write_lock for e in entities]
        read_locks = [e._internal.read_lock for e in entities]
        g_parent = greenlet.getcurrent().parent

        for _ in range(poll):
            tx.watch(write_locks + read_locks)

            # Wait for write and read lock release
            wait = write_locks + [
                i
                for e in entities
                for i in self.redis.hgetall(e._internal.read_lock)]

            if any(self.redis.mget(*wait)):
                sleep_duration = 1.0 * timeout / poll
                tx.reset()
                if g_parent:
                    g_parent.switch(sleep_duration)
                else:
                    sleep(sleep_duration / 1000.0)
                continue

            # Set write lock
            tx.multi()
            for lock in write_locks:
                tx.set(lock, tx_id, px=expire)

            # Clear any residual read locks
            tx.delete(*read_locks)

            try:
                with tx:
                    tx.execute()
                break
            except redis.WatchError:
                # Write or read lock was acquired elsewhere
                tx.reset()
        else:
            raise Exceptions.TimeoutError()

        # Successfully acquired locks for tx
        for e in entities:
            e._internal.tx_id = tx_id

        self.local.write_entities[tx_id] = entities
        self.local.tx_level += 1

        try:
            if len(entities) == 1:
                yield entities[0]
            else:
                yield entities
        except Exception:
            self.release(flush=False)
            raise
        else:
            self.release(flush=True)

    def exists(self, *entity_ids):
        return all([self.redis.exists(e) for e in entity_ids])

    def release(self, flush):
        assert self.local.tx_level > 0

        self.local.tx_level -= 1
        if self.local.tx_level >= 1:
            return

        try:
            # Release read locks
            for tx_id, entities in self.local.read_entities.items():
                for e in entities:
                    self.redis.hdel(e._internal.read_lock, tx_id)

                if self.redis.get(tx_id):
                    self.redis.delete(tx_id)
                else:
                    raise Exceptions.TransactionExpiredError()

            # Release write locks and flush data
            write_locks = [
                e._internal.write_lock
                for entities in self.local.write_entities.values()
                for e in entities]

            release_tx = self.redis.pipeline()
            release_tx.watch(write_locks)
            release_tx.multi()

            for tx_id, entities in self.local.write_entities.items():
                if not all(i == tx_id for i in self.redis.mget(*write_locks)):
                    raise Exceptions.TransactionExpiredError()

                if not flush:
                    continue

                # Flush data
                for e in entities:
                    for k in e._internal.dirty:
                        assert hasattr(e, k)
                        value = pickle.dumps(getattr(e, k), 0)
                        release_tx.hset(e._internal.entity_id, k, value)

            try:
                with release_tx:
                    release_tx.execute()
            except redis.WatchError:
                raise Exceptions.TransactionExpiredError()
        finally:
            self.local.read_entities = {}
            self.local.write_entities = {}


class TestRObject(unittest.TestCase):
    def setUp(self):
        self.db = Database()
        with self.db.write('test', 'test_nested') as (x1, x2):
            x1.value = 1
            x2.value = 1

    def test_read(self):
        with self.db.read('test') as x:
            assert x.value == 1

    def test_read_only(self):
        with self.db.write('test') as x:
            x.test = 1
            assert x.test == 1

        with self.assertRaises(Exceptions.ReadOnlyError):
            with self.db.read('test') as x:
                x.test = 2

    def test_write(self):
        with self.db.write('test') as x:
            assert not x.should_be_none
            x.test = 1
            assert x.test == 1

        with self.db.read('test') as x:
            assert x.test == 1

    def test_list_modify_1(self):
        with self.db.write('test') as x:
            x.test = [1, 2, 3]
            assert x.test == [1, 2, 3]
            x.test += [4]
            assert x.test == [1, 2, 3, 4]
            x.test.pop(0)
            assert x.test == [2, 3, 4]

    def test_list_modify_2(self):
        with self.db.write('test') as x:
            x.test = [1, 2, 3]
        with self.db.read('test') as x:
            assert x.test == [1, 2, 3]

        with self.db.write('test') as x:
            x.test += [4]
        with self.db.read('test') as x:
            assert x.test == [1, 2, 3, 4]

        with self.db.write('test') as x:
            x.test.pop(0)
        with self.db.read('test') as x:
            assert x.test == [2, 3, 4]

    def test_dict_modify_1(self):
        with self.db.write('test') as x:
            x.test = {1: 1, 2: 2, 3: 3}
            assert x.test == {1: 1, 2: 2, 3: 3}
            x.test[4] = 4
            assert x.test == {1: 1, 2: 2, 3: 3, 4: 4}
            del x.test[1]
            assert x.test == {2: 2, 3: 3, 4: 4}
            x.test.update({5: 5})
            assert x.test == {2: 2, 3: 3, 4: 4, 5: 5}

    def test_dict_modify_2(self):
        with self.db.write('test') as x:
            x.test = {1: 1, 2: 2, 3: 3}
        with self.db.read('test') as x:
            assert x.test == {1: 1, 2: 2, 3: 3}

        with self.db.write('test') as x:
            x.test[4] = 4
        with self.db.read('test') as x:
            assert x.test == {1: 1, 2: 2, 3: 3, 4: 4}

        with self.db.write('test') as x:
            del x.test[1]
        with self.db.read('test') as x:
            assert x.test == {2: 2, 3: 3, 4: 4}

        with self.db.write('test') as x:
            x.test.update({5: 5})
        with self.db.read('test') as x:
            assert x.test == {2: 2, 3: 3, 4: 4, 5: 5}

    def test_reassign_attr(self):
        with self.db.write('test') as x:
            assert x.value == 1
            x.value = [1]
            assert x.value == [1]

        with self.db.read('test') as x:
            assert x.value == [1]

    def test_read_lock(self):
        with self.db.read('test', autolock=False) as x1:
            assert x1.value == 1

            with self.assertRaises(Exceptions.TimeoutError):
                with self.db.write('test', timeout=25):
                    pass

            with self.db.read('test', timeout=25) as x2:
                assert x2.value == 1

    def test_write_lock(self):
        with self.db.write('test', autolock=False) as x1:
            assert x1.value == 1

            with self.assertRaises(Exceptions.TimeoutError):
                with self.db.write('test', timeout=25):
                    pass

            with self.assertRaises(Exceptions.TimeoutError):
                with self.db.read('test', timeout=25):
                    pass

    def test_read_transaction_expired_1(self):
        with self.assertRaises(Exceptions.TransactionExpiredError):
            with self.db.read('test') as x:
                assert x.value == 1
                sleep(0.15)

    def test_read_transaction_expired_2(self):
        with self.assertRaises(Exceptions.TransactionExpiredError):
            with self.db.read('test', autolock=False) as x:
                assert x.value == 1

                with self.assertRaises(Exceptions.TimeoutError):
                    with self.db.write('test', timeout=25):
                        pass

                with self.db.write('test', timeout=150):
                    pass

    def test_write_transaction_expired_1(self):
        with self.assertRaises(Exceptions.TransactionExpiredError):
            with self.db.write('test') as x:
                assert x.value == 1
                x.value = 2
                sleep(0.15)

        with self.db.read('test') as x:
            assert x.value == 1

    def test_write_transaction_expired_2(self):
        with self.assertRaises(Exceptions.TransactionExpiredError):
            with self.db.write('test', autolock=False) as x:
                assert x.value == 1
                x.value = 2

                with self.assertRaises(Exceptions.TimeoutError):
                    with self.db.write('test', timeout=25):
                        pass

                with self.db.write('test', timeout=150):
                    pass

        with self.db.read('test') as x:
            assert x.value == 1

    def test_read_shared_acquire(self):
        with self.db.read('test', autolock=False) as x1:
            assert x1.value == 1
            with self.db.read('test', timeout=25) as x2:
                assert x2.value == 1

    def test_read_acquire_timeout(self):
        with self.db.write('test', autolock=False) as x:
            assert x.value == 1
            with self.assertRaises(Exceptions.TimeoutError):
                with self.db.read('test', timeout=25):
                    pass

    def test_write_acquire_timeout(self):
        with self.db.read('test', autolock=False) as x:
            assert x.value == 1
            with self.assertRaises(Exceptions.TimeoutError):
                with self.db.write('test', timeout=25):
                    pass

        with self.db.write('test', autolock=False) as x:
            assert x.value == 1
            with self.assertRaises(Exceptions.TimeoutError):
                with self.db.write('test', timeout=25):
                    pass

    def test_already_acquired(self):
        with self.db.read('test') as x:
            assert x.value == 1
            with self.assertRaises(Exceptions.AlreadyAcquiredError):
                with self.db.read('test'):
                    pass
            with self.assertRaises(Exceptions.AlreadyAcquiredError):
                with self.db.write('test'):
                    pass

        with self.db.write('test') as x:
            assert x.value == 1
            with self.assertRaises(Exceptions.AlreadyAcquiredError):
                with self.db.read('test'):
                    pass
            with self.assertRaises(Exceptions.AlreadyAcquiredError):
                with self.db.write('test'):
                    pass

    def test_nested_transaction(self):
        with self.db.read('test') as x1:
            assert x1.value == 1
            with self.db.write('test_nested') as x2:
                assert x2.value == 1
                x2.value = 2

        with self.db.read('test_nested') as x:
            assert x.value == 2

    def test_nested_transaction_expire(self):
        with self.assertRaises(Exceptions.TransactionExpiredError):
            with self.db.read('test') as x1:
                assert x1.value == 1
                with self.db.write('test_nested') as x2:
                    assert x2.value == 1
                    x2.value = 2
                sleep(0.15)

        with self.db.read('test_nested') as x:
            assert x.value == 1

    def test_nested_transaction_timeout(self):
        with self.assertRaises(Exceptions.TimeoutError):
            with self.db.read('test', autolock=False) as x1:
                assert x1.value == 1
                with self.db.write('test_nested', timeout=25) as x2:
                    x2.value = 2
                with self.db.write('test', timeout=25):
                    pass

        with self.db.read('test_nested') as x:
            assert x.value == 1

    def test_error_during_transaction(self):
        raise NotImplementedError()

    def test_error_during_nested_transaction(self):
        raise NotImplementedError()

    def test_locks_change_during_acquire(self):
        raise NotImplementedError()

    def test_flush(self):
        with self.db.write('test') as x:
            assert x.value == 1
            x.value = 2
        assert pickle.loads(self.db.redis.hget('test', 'value')) == 2

    def test_concurrency(self):
        raise NotImplementedError()

    def test_exists(self):
        assert self.db.exists('test')
        self.db.redis.delete('test_exists')
        assert not self.db.exists('test_exists')
        with self.db.write('test_exists') as x:
            x.value = 1
        assert self.db.exists('test_exists')


if __name__ == '__main__':
    unittest.main()
