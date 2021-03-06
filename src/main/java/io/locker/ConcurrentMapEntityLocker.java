package io.locker;

import com.sun.istack.internal.NotNull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * A reentrant implementation of {@link EntityLocker}.
 * A {@code ConcurrentMapEntityLocker} is owned by the thread
 * that last successfully acquired the lock and not released it yet.
 *
 * @param <ID> type of entity id
 */

@Slf4j
public class ConcurrentMapEntityLocker<ID> implements EntityLocker<ID> {

    private final ConcurrentHashMap<ID, ReentrantLock> lockerMap;
    private final ConcurrentHashMap<ID, Integer> lockerCountMap;

    private final ReadWriteLock readWriteLock = new ReentrantReadWriteLock();
    private final Lock readGlobalLock = readWriteLock.readLock();
    private final Lock writeGlobalLock = readWriteLock.writeLock();
    private final AtomicLong globalLockThreadId = new AtomicLong(-1);


    public ConcurrentMapEntityLocker() {
        this.lockerMap = new ConcurrentHashMap<>();
        this.lockerCountMap = new ConcurrentHashMap<>();
    }

    /**
     * Acquire the lock on specified entity id.
     * The hold count is incremented on each locking.
     * If the lock is acquired by other thread then call of this method is blocked
     * until the other thread releases the lock.
     *
     * @param entityId id of the entity that is needed to have exclusive access
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     */
    public void lock(@NonNull ID entityId) throws InterruptedException {
        readGlobalLock.lockInterruptibly();
        log.info("Global READ Lock is acquired for entity with id={}", entityId);
        for (; ; ) {
            ReentrantLock lock = lockerMap.computeIfAbsent(entityId, id -> new ReentrantLock());
            try {
                lock.lockInterruptibly();
                if (lock == lockerMap.get(entityId)) {
                    incrementEntityLockCount(entityId);
                    log.info("Lock for entity with id={} is acquired", entityId);
                    return;
                }
                lock.unlock();
            } catch (InterruptedException e) {
                readGlobalLock.unlock();
            }
        }
    }


    /**
     * Try to acquire the lock on specified entity id within the specified waiting timeout.
     * Acquire the lock immediately if the lock was not acquired yet.
     * The hold count is incremented on each successful locking.
     *
     * @param entityId id of the entity that is needed to have exclusive access
     * @param time     the maximum time to wait lock acquiring
     * @param unit     the time unit of {@code time} argument
     * @return {@code true} if lock is acquired and {@code false} otherwise
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     * @throws NullPointerException if {@code entityId} or {@code unit} is null
     */
    @Override
    public boolean tryLock(@NonNull ID entityId, long time, @NonNull TimeUnit unit) throws InterruptedException {
        long timeout = System.nanoTime() + unit.toNanos(time);

        if (!readGlobalLock.tryLock(timeout - System.nanoTime(), TimeUnit.NANOSECONDS)) {
            return false;
        }
        log.info("Global READ Lock is acquired for entity with id={}", entityId);

        for (; ; ) {
            ReentrantLock lock = lockerMap.computeIfAbsent(entityId, id -> new ReentrantLock());
            if (!lock.tryLock(timeout - System.nanoTime(), TimeUnit.NANOSECONDS)) {
                readGlobalLock.unlock();
                log.info("Global READ Lock is released for entity with id={}", entityId);
                return false;
            }
            if (lock == lockerMap.get(entityId)) {
                incrementEntityLockCount(entityId);
                log.info("Lock for entity with id={} is acquired", entityId);
                return true;
            }
            lock.unlock();
        }
    }

    /**
     * Release the lock on specified entity id.
     * The hold count is decremented on each unlocking.
     * If the hold count is zero then the lock is released.
     *
     * @param entityId id of the entity that is needed to be released
     * @throws IllegalMonitorStateException if the current thread is not holding the lock
     */
    public void unlock(@NotNull ID entityId) {
        ReentrantLock lock = lockerMap.get(entityId);
        if (lock == null || !lock.isHeldByCurrentThread()) {
            throw new IllegalMonitorStateException();
        }

        if (decrementEntityLockCount(entityId) == 0 && !lock.hasQueuedThreads()) {
            lockerCountMap.remove(entityId);
            lockerMap.remove(entityId);
        }

        lock.unlock();
        log.info("Lock for entity with id={} is released", entityId);

        readGlobalLock.unlock();
        log.info("Global READ Lock is released for entity with id={}", entityId);
    }

    /**
     * Acquire global lock to have exclusive access to all entities.
     *
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     */
    @Override
    public void globalLock() throws InterruptedException {
        writeGlobalLock.lockInterruptibly();
        globalLockThreadId.set(Thread.currentThread().getId());
        log.info("Global Lock is acquired");
    }

    /**
     * Release global lock.
     *
     * @throws IllegalMonitorStateException if the current thread is not holding the lock
     */
    @Override
    public void globalUnlock() {
        if (globalLockThreadId.get() != Thread.currentThread().getId()) {
            throw new IllegalMonitorStateException();
        }
        writeGlobalLock.unlock();
        log.info("Global Lock is released");
    }

    private void incrementEntityLockCount(ID entityId) {
        lockerCountMap.putIfAbsent(entityId, 0);
        lockerCountMap.compute(entityId, (id, currentCount) -> currentCount + 1);
    }

    private int decrementEntityLockCount(ID entityId) {
        return lockerCountMap.compute(entityId, (id, currentCount) -> currentCount - 1);
    }
}
