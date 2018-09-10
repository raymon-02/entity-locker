package io.locker;

import java.util.concurrent.TimeUnit;

/**
 * {@code EntityLocker} is an interface that provides
 * operations on entities' ids to ensure exclusive access
 * to shared entities.
 * <p>
 * <p>Using {@code EntityLocker} is similar to {@link java.util.concurrent.locks.Lock}.
 * <pre> {@code
 * EntityLocker el = ...;
 * el.lock();
 * try {
 *   // access the entities protected by this lock
 * } finally {
 *   el.unlock();
 * }}</pre>
 *
 * @param <ID> type of entity id
 */
public interface EntityLocker<ID> {

    /**
     * Acquire the lock on specified entity id.
     *
     * @param entityId id of the entity that is needed to have exclusive access
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     */
    void lock(ID entityId) throws InterruptedException;

    /**
     * Try to acquire the lock on specified entity id within the specified waiting timeout.
     * Acquire the lock immediately if the lock was not acquired yet.
     *
     * @param entityId id of the entity that is needed to have exclusive access
     * @param time     the maximum time to wait lock acquiring
     * @param unit     the time unit of {@code time} argument
     * @return {@code true} if lock is acquired and {@code false} otherwise
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     */
    boolean tryLock(ID entityId, long time, TimeUnit unit) throws InterruptedException;

    /**
     * Release the lock on specified entity id
     *
     * @param entityId id of the entity that is needed to be released
     */
    void unlock(ID entityId);

    /**
     * Acquire global lock to have exclusive access to all entities.
     *
     * @throws InterruptedException if the current thread is interrupted while acquiring the lock
     */
    void globalLock() throws InterruptedException;

    /**
     * Release global lock.
     *
     * @throws IllegalMonitorStateException if the current thread is not holding the lock
     */
    void globalUnlock();
}
