package io.locker;

import com.sun.istack.internal.NotNull;
import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;

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
        for (; ; ) {
            ReentrantLock lock = lockerMap.computeIfAbsent(entityId, id -> new ReentrantLock());
            lock.lockInterruptibly();
            if (lock == lockerMap.get(entityId)) {
                incrementEntityLockCount(entityId);
                log.info("Lock for entity with id={} is acquired", entityId);
                return;
            }
            lock.unlock();
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
        for (; ; ) {
            ReentrantLock lock = lockerMap.computeIfAbsent(entityId, id -> new ReentrantLock());
            if (!lock.tryLock(timeout - System.nanoTime(), TimeUnit.NANOSECONDS)) {
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
            lockerMap.remove(entityId);
        }

        lock.unlock();
        log.info("Lock for entity with id={} is released", entityId);
    }

    private void incrementEntityLockCount(ID entityId) {
        lockerCountMap.putIfAbsent(entityId, 0);
        lockerCountMap.compute(entityId, (id, currentCount) -> currentCount + 1);
    }

    private int decrementEntityLockCount(ID entityId) {
        return lockerCountMap.compute(entityId, (id, currentCount) -> currentCount - 1);
    }
}
