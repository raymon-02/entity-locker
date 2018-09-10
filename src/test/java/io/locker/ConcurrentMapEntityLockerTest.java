package io.locker;

import lombok.extern.slf4j.Slf4j;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.concurrent.*;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

@Slf4j
public class ConcurrentMapEntityLockerTest {

    private static final int THREADS_COUNT = 2;
    private EntityLocker<Integer> locker;
    private CountDownLatch latch;
    private ExecutorService executorService;

    @Before
    public void setUp() {
        locker = new ConcurrentMapEntityLocker<>();
        latch = new CountDownLatch(THREADS_COUNT);
        executorService = Executors.newFixedThreadPool(THREADS_COUNT);
    }

    @After
    public void tearDown() {
        executorService.shutdown();
    }


    @Test(expected = NullPointerException.class)
    public void lockerShouldThrowNullOnLockWhenEntityIdIsNull() throws InterruptedException {
        locker.lock(null);
    }

    @Test(expected = NullPointerException.class)
    public void lockerShouldThrowNullOnTryLockWhenEntityIdIsNull() throws InterruptedException {
        locker.lock(null);
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLock() throws InterruptedException {
        Integer entityId = 129;
        locker.lock(entityId);
        locker.unlock(entityId);
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void lockerShouldThrowMonitorExceptionsOnUnlockWithoutLock() {
        Integer entityId = 129;
        locker.unlock(entityId);
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockOnOneEntityFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId = 129;

        Runnable runnable = createRunnable(entityId);

        Future<?> future1 = executorService.submit(runnable);
        Future<?> future2 = executorService.submit(runnable);
        future1.get();
        future2.get();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockOnTwoEqualEntitiesFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId1 = 129;
        Integer entityId2 = 129;

        Runnable runnable1 = createRunnable(entityId1);
        Runnable runnable2 = createRunnable(entityId2);

        Future<?> future1 = executorService.submit(runnable1);
        Future<?> future2 = executorService.submit(runnable2);
        future1.get();
        future2.get();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockOnTwoDifferentEntitiesFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId1 = 129;
        Integer entityId2 = 130;

        Runnable runnable1 = createRunnable(entityId1);
        Runnable runnable2 = createRunnable(entityId2);

        Future<?> future1 = executorService.submit(runnable1);
        Future<?> future2 = executorService.submit(runnable2);
        future1.get();
        future2.get();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void lockerShouldThrowMonitorExceptionsOnUnlockFromOtherThread() throws ExecutionException, InterruptedException {
        Integer entityId = 129;
        CountDownLatch endLatch = new CountDownLatch(THREADS_COUNT);

        Runnable runnable = createRunnableWithEndLatch(entityId, endLatch);
        Future<?> future = executorService.submit(runnable);
        try {
            latch.countDown();
            latch.await();
            locker.unlock(entityId);
        } finally {
            endLatch.countDown();
            future.get();
        }
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnReentrantLock() throws InterruptedException {
        Integer entityId = 129;
        locker.lock(entityId);
        locker.lock(entityId);
        locker.unlock(entityId);
        locker.unlock(entityId);
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnReentrantLockFromTwoThreads() throws InterruptedException, ExecutionException {
        Integer entityId = 129;

        Runnable runnable = createRunnable(entityId);
        Future<?> future = executorService.submit(runnable);

        locker.lock(entityId);
        locker.lock(entityId);
        locker.unlock(entityId);

        latch.countDown();
        latch.await();

        locker.unlock(entityId);
        future.get();
    }

    @Test
    public void lockerShouldReturnFalseOnTryLockOnOneEntityFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId = 129;
        CountDownLatch endLatch = new CountDownLatch(THREADS_COUNT);

        Runnable runnable = createRunnableWithEndLatch(entityId, endLatch);
        Future<?> future = executorService.submit(runnable);
        latch.countDown();
        latch.await();

        boolean locked = locker.tryLock(entityId, 100, TimeUnit.MILLISECONDS);
        assertFalse(locked);

        endLatch.countDown();
        future.get();
    }

    @Test
    public void lockerShouldReturnTrueOnTryLockOnTwoDifferentEntitiesFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId1 = 129;
        Integer entityId2 = 130;

        Callable<Boolean> callable1 = createCallable(entityId1);
        Callable<Boolean> callable2 = createCallable(entityId2);

        Future<Boolean> future1 = executorService.submit(callable1);
        Future<Boolean> future2 = executorService.submit(callable2);

        assertTrue(future1.get());
        assertTrue(future2.get());
    }


    private Runnable createRunnable(Integer entityId) {
        return () -> {
            try {
                latch.countDown();
                latch.await();
                locker.lock(entityId);
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
            } finally {
                locker.unlock(entityId);
            }
        };
    }

    private Runnable createRunnableWithEndLatch(Integer entityId, CountDownLatch endLatch) {
        return () -> {
            try {
                locker.lock(entityId);
                latch.countDown();
                latch.await();
                endLatch.countDown();
                endLatch.await();
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
            } finally {
                locker.unlock(entityId);
            }
        };
    }

    private Callable<Boolean> createCallable(Integer entityId) {
        return () -> {
            try {
                latch.countDown();
                latch.await();
                return locker.tryLock(entityId, 1, TimeUnit.SECONDS);
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
                return false;
            } finally {
                locker.unlock(entityId);
            }
        };
    }
}
