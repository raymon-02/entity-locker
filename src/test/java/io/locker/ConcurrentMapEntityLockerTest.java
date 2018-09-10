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
    public void lockerShouldThrowIllegalMonitorExceptionsOnUnlockWithoutLock() {
        Integer entityId = 129;
        locker.unlock(entityId);
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockOnOneEntityFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId = 129;

        Future<?> future1 = executorService.submit(createRunnable(entityId));
        Future<?> future2 = executorService.submit(createRunnable(entityId));
        future1.get();
        future2.get();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockOnTwoEqualEntitiesFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId1 = 129;
        Integer entityId2 = 129;

        Future<?> future1 = executorService.submit(createRunnable(entityId1));
        Future<?> future2 = executorService.submit(createRunnable(entityId2));
        future1.get();
        future2.get();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockOnTwoDifferentEntitiesFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId1 = 129;
        Integer entityId2 = 130;

        Future<?> future1 = executorService.submit(createRunnable(entityId1));
        Future<?> future2 = executorService.submit(createRunnable(entityId2));
        future1.get();
        future2.get();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void lockerShouldThrowIllegalMonitorExceptionsOnUnlockFromOtherThread() throws ExecutionException, InterruptedException {
        Integer entityId = 129;
        CountDownLatch endLatch = new CountDownLatch(THREADS_COUNT);

        Future<?> future = executorService.submit(createRunnableWithLockAndEndLatch(entityId, endLatch));
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

        Future<?> future = executorService.submit(createRunnable(entityId));

        locker.lock(entityId);
        locker.lock(entityId);
        locker.unlock(entityId);

        latch.countDown();
        latch.await();

        locker.unlock(entityId);
        future.get();
    }

    @Test
    public void lockerShouldReturnFalseOnLockAndThenTryLockOnOneEntityFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId = 129;

        Future<Boolean> future = executorService.submit(createCallable(entityId));

        locker.lock(entityId);
        latch.countDown();
        latch.await();

        assertFalse(future.get());

        locker.unlock(entityId);
    }

    @Test
    public void lockerShouldReturnTrueOnTryLockOnTwoDifferentEntitiesFromTwoThreads() throws ExecutionException, InterruptedException {
        Integer entityId1 = 129;
        Integer entityId2 = 130;

        Future<Boolean> future1 = executorService.submit(createCallable(entityId1));
        Future<Boolean> future2 = executorService.submit(createCallable(entityId2));

        assertTrue(future1.get());
        assertTrue(future2.get());
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnGlobalLock() throws InterruptedException {
        locker.globalLock();
        locker.globalUnlock();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void lockerShouldThrowIllegalMonitorExceptionsOnGlobalUnlockWithoutGlobalLock() {
        locker.globalUnlock();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnGlobalTwoThreads() throws ExecutionException, InterruptedException {
        Future<?> future1 = executorService.submit(createRunnableWithGlobalLock());
        Future<?> future2 = executorService.submit(createRunnableWithGlobalLock());
        future1.get();
        future2.get();
    }

    @Test(expected = IllegalMonitorStateException.class)
    public void lockerShouldThrowIllegalMonitorExceptionsOnGlobalUnlockFromOtherThread() throws ExecutionException, InterruptedException {
        CountDownLatch endLatch = new CountDownLatch(THREADS_COUNT);

        Future<?> future = executorService.submit(createRunnableWithGlobalLockAndEndLatch(endLatch));
        try {
            latch.countDown();
            latch.await();
            locker.globalUnlock();
        } finally {
            endLatch.countDown();
            future.get();
        }
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnGlobalReentrantLock() throws InterruptedException {
        locker.globalLock();
        locker.globalLock();
        locker.globalUnlock();
        locker.globalUnlock();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockAndGlobalLockFromDifferentThreads() throws InterruptedException, ExecutionException {
        Integer entityId = 129;

        Future<?> future = executorService.submit(createRunnable(entityId));

        latch.countDown();
        latch.await();
        locker.globalLock();
        locker.globalUnlock();

        future.get();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnLockAndThenGlobalLockFromDifferentThreads() throws InterruptedException, ExecutionException {
        Integer entityId = 129;

        Future<?> future = executorService.submit(createRunnableWithLockAndLatch(entityId));

        latch.countDown();
        latch.await();
        locker.globalLock();
        locker.globalUnlock();

        future.get();
    }

    @Test
    public void lockerShouldNotThrowExceptionsOnGlobalLockAndThenLockFromDifferentThreads() throws InterruptedException, ExecutionException {
        Integer entityId = 129;

        Future<?> future = executorService.submit(createRunnable(entityId));

        locker.globalLock();
        latch.countDown();
        latch.await();
        locker.globalUnlock();

        future.get();
    }

    @Test
    public void lockerShouldReturnFalseOnGlobalLockAndThenTryLockFromDifferentThreads() throws InterruptedException, ExecutionException {
        Integer entityId = 129;

        Future<Boolean> future = executorService.submit(createCallable(entityId));

        locker.globalLock();
        latch.countDown();
        latch.await();

        assertFalse(future.get());

        locker.globalUnlock();
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

    private Runnable createRunnableWithLockAndLatch(Integer entityId) {
        return () -> {
            try {
                locker.lock(entityId);
                latch.countDown();
                latch.await();
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
            } finally {
                locker.unlock(entityId);
            }
        };
    }

    private Runnable createRunnableWithLockAndEndLatch(Integer entityId, CountDownLatch endLatch) {
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
            boolean result = false;
            try {
                latch.countDown();
                latch.await();
                result = locker.tryLock(entityId, 1, TimeUnit.SECONDS);
                return result;
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
                return false;
            } finally {
                if (result) {
                    locker.unlock(entityId);
                }
            }
        };
    }

    private Runnable createRunnableWithGlobalLock() {
        return () -> {
            try {
                locker.globalLock();
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
            } finally {
                locker.globalUnlock();
            }
        };
    }

    private Runnable createRunnableWithGlobalLockAndEndLatch(CountDownLatch endLatch) {
        return () -> {
            try {
                locker.globalLock();
                latch.countDown();
                latch.await();
                endLatch.countDown();
                endLatch.await();
            } catch (InterruptedException e) {
                log.error("Will never happen probably");
            } finally {
                locker.globalUnlock();
            }
        };
    }
}
