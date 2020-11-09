package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import edu.berkeley.cs186.database.common.Pair;

import java.util.*;

/**
 * LockManager maintains the bookkeeping for what transactions have what locks
 * on what resources and handles queuing logic. The lock manager should generally
 * NOT be used directly: instead, code should call methods of LockContext to
 * acquire/release/promote/escalate locks.
 *
 * The LockManager is primarily concerned with the mappings between
 * transactions, resources, and locks, and does not concern itself with multiple
 * levels of granularity. Multigranularity is handled by LockContext instead.
 *
 * Each resource the lock manager manages has its own queue of LockRequest
 * objects representing a request to acquire (or promote/acquire-and-release) a
 * lock that could not be satisfied at the time. This queue should be processed
 * every time a lock on that resource gets released, starting from the first
 * request, and going in order until a request cannot be satisfied. Requests
 * taken off the queue should be treated as if that transaction had made the
 * request right after the resource was released in absence of a queue (i.e.
 * removing a request by T1 to acquire X(db) should be treated as if T1 had just
 * requested X(db) and there were no queue on db: T1 should be given the X lock
 * on db, and put in an unblocked state via Transaction#unblock).
 *
 * This does mean that in the case of:
 *    queue: S(A) X(A) S(A)
 * only the first request should be removed from the queue when the queue is
 * processed.
 */
public class LockManager {
    // transactionLocks is a mapping from transaction number to a list of lock
    // objects held by that transaction.
    private Map<Long, List<Lock>> transactionLocks = new HashMap<>();

    // resourceEntries is a mapping from resource names to a ResourceEntry
    // object, which contains a list of Locks on the object, as well as a
    // queue for requests on that resource.
    private Map<ResourceName, ResourceEntry> resourceEntries = new HashMap<>();

    // A ResourceEntry contains the list of locks on a resource, as well as
    // the queue for requests for locks on the resource.
    private class ResourceEntry {
        // List of currently granted locks on the resource.
        List<Lock> locks = new ArrayList<>();
        // Queue for yet-to-be-satisfied lock requests on this resource.
        Deque<LockRequest> waitingQueue = new ArrayDeque<>();

        // Below are a list of helper methods we recommend you implement.
        // You're free to modify their type signatures, delete, or ignore them.

        @Override
        public String toString() {
            return "Active Locks: " + Arrays.toString(this.locks.toArray()) +
                    ", Queue: " + Arrays.toString(this.waitingQueue.toArray());
        }

        public boolean checkConflicts(LockType locktype, TransactionContext transaction) {
            for (Lock lock: locks) {
                if (lock.transactionNum != transaction.getTransNum() && !LockType.compatible(lock.lockType, locktype)) {
                    return true;
                }
            }
            return false;
        }
    }

    // You should not modify or use this directly.
    private Map<Long, LockContext> contexts = new HashMap<>();

    /**
     * Helper method to fetch the resourceEntry corresponding to `name`.
     * Inserts a new (empty) resourceEntry into the map if no entry exists yet.
     */
    private ResourceEntry getResourceEntry(ResourceName name) {
        resourceEntries.putIfAbsent(name, new ResourceEntry());
        return resourceEntries.get(name);
    }

    public Lock checkConflicts(Lock lock) {
        ResourceName name = lock.name;
        List<Lock> locks = getLocks(name);
        if (locks.isEmpty()) {
            return null;
        }
        for (Lock oldLock : locks) {
            if (!LockType.compatible(oldLock.lockType, lock.lockType)) {
                return oldLock;
            }
        }
        return null;
    }

    public Lock getLock(long transactionNum, List<Lock> locks) {
        for (Lock lock: locks) {
            if (lock.transactionNum == transactionNum) {
                return lock;
            }
        }
        return null;
    }

    public boolean contain(TransactionContext transaction, ResourceName name) {
        transactionLocks.putIfAbsent(transaction.getTransNum(), new ArrayList<>());
        List<Lock> locks = transactionLocks.get((transaction.getTransNum()));
        for (Lock lock: locks) {
            if (lock.name.equals(name)) {
                return true;
            }
        }
        return false;
    }

    public boolean checkRelease(TransactionContext transaction, ResourceName name,
                               List<ResourceName> release) {
        transactionLocks.putIfAbsent(transaction.getTransNum(), new ArrayList<>());
        List<Lock> locks = transactionLocks.get(transaction.getTransNum());
        for (Lock lock: locks) {
            if (lock.name.equals(name) && !release.contains(name)) {
                return true;
            }
        }
        return false;
    }

    public boolean checkUpgrade(TransactionContext transaction, Lock newlock) {
        for (Lock lock : transactionLocks.get(transaction.getTransNum())) {
            if (newlock.name.equals(lock.name) && newlock.transactionNum == lock.transactionNum) {
                ResourceEntry entry = getResourceEntry(newlock.name);
                entry.locks.remove(lock);
                entry.locks.add(newlock);
                lock.lockType = newlock.lockType;
                return true;
            }
        }
        return false;
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`, and
     * releases all locks on `releaseNames` held by the transaction after
     * acquiring the lock in one atomic action.
     *
     * Error checking must be done before any locks are acquired or released. If
     * the new lock is not compatible with another transaction's lock on the
     * resource, the transaction is blocked and the request is placed at the
     * FRONT of the resource's queue.
     *
     * Locks on `releaseNames` should be released only after the requested lock
     * has been acquired. The corresponding queues should be processed.
     *
     * An acquire-and-release that releases an old lock on `name` should NOT
     * change the acquisition time of the lock on `name`, i.e. if a transaction
     * acquired locks in the order: S(A), X(B), acquire X(A) and release S(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is already held
     * by `transaction` and isn't being released
     * @throws NoLockHeldException if `transaction` doesn't hold a lock on one
     * or more of the names in `releaseNames`
     */
    public void acquireAndRelease(TransactionContext transaction, ResourceName name,
                                  LockType lockType, List<ResourceName> releaseNames)
            throws DuplicateLockRequestException, NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep
        // all your code within the given synchronized block and are allowed to
        // move the synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            if (checkRelease(transaction, name, releaseNames)) {
                throw new DuplicateLockRequestException("Duplicates!");
            }

            List<Lock> released = new ArrayList<>();
            for (ResourceName n : releaseNames) {
                if (!contain(transaction, n)) {
                    throw new NoLockHeldException("no lock held");
                } else {
                    for (Lock lock: transactionLocks.get(transaction.getTransNum())) {
                        if (lock.name.equals(n)) {
                            released.add(lock);
                            break; }
                    }
                }
            }

            // acquire lock
            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, lockType, transaction.getTransNum());
            if (entry.checkConflicts(lockType, transaction)) {
                entry.waitingQueue.addFirst(new LockRequest(transaction, newLock, released));
                shouldBlock = true;
            } else {
                if (!checkUpgrade(transaction, newLock)) {
                    entry.locks.add(newLock);
                    transactionLocks.get(transaction.getTransNum()).add(newLock);
                }

                for (ResourceName n: releaseNames) {
                    if (n != name) {
                        entry = getResourceEntry(n);
                        boolean flag = false;
                        for (int i = 0; i < entry.locks.size() && !flag; i++) {
                            if (entry.locks.get(i).transactionNum == transaction.getTransNum()) {
                                Lock tep = entry.locks.remove(i);
                                transactionLocks.get(transaction.getTransNum()).remove(tep);
                                flag = true;
                            }
                        }

                        // update queue
                        boolean compatible = true;
                        while (!entry.waitingQueue.isEmpty() && compatible) {
                            LockRequest request = entry.waitingQueue.peek();
                            if (entry.checkConflicts(request.lock.lockType, request.transaction)){
                                compatible = false;
                            } else {
                                entry.waitingQueue.pop();
                                if (!checkUpgrade(transaction, request.lock)) {
                                    entry.locks.add(request.lock);
                                    transactionLocks.get(request.transaction.getTransNum()).add(request.lock);
                                }
                                while (!request.releasedLocks.isEmpty()) {
                                    Lock toremove = request.releasedLocks.remove(0);
                                    release(request.transaction, toremove.name);
                                }
                                request.transaction.unblock();
                            }
                        }
                    }
                }
            }
        }

        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Acquire a `lockType` lock on `name`, for transaction `transaction`.
     *
     * Error checking must be done before the lock is acquired. If the new lock
     * is not compatible with another transaction's lock on the resource, or if there are
     * other transaction in queue for the resource, the transaction is
     * blocked and the request is placed at the **back** of NAME's queue.
     *
     * @throws DuplicateLockRequestException if a lock on `name` is held by
     * `transaction`
     */
    public void acquire(TransactionContext transaction, ResourceName name,
                        LockType lockType) throws DuplicateLockRequestException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method. You are not required to keep all your
        // code within the given synchronized block and are allowed to move the
        // synchronized block elsewhere if you wish.
        boolean shouldBlock = false;
        synchronized (this) {
            if (contain(transaction, name)) {
                throw new DuplicateLockRequestException("Duplicates!");
            }

            ResourceEntry entry = getResourceEntry(name);
            Lock lock = new Lock(name, lockType, transaction.getTransNum());
            if (entry.checkConflicts(lockType, transaction) || !entry.waitingQueue.isEmpty()) {
                entry.waitingQueue.addLast(new LockRequest(transaction, lock));
                shouldBlock = true;
            } else {
                entry.locks.add(lock);
                transactionLocks.get(transaction.getTransNum()).add(lock);
            }
        }
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Release `transaction`'s lock on `name`. Error checking must be done
     * before the lock is released.
     *
     * The resource name's queue should be processed after this call. If any
     * requests in the queue have locks to be released, those should be
     * released, and the corresponding queues also processed.
     *
     * @throws NoLockHeldException if no lock on `name` is held by `transaction`
     */
    public void removeTransLock(Lock lock) {
        List<Lock> locks = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<>());
        locks.remove(lock);
        if (locks.isEmpty()) {
            transactionLocks.remove(lock.transactionNum);
        } else {
            transactionLocks.put(lock.transactionNum, locks);
        }
    }

    public void addTransLock(Lock lock) {
        List<Lock> locks = transactionLocks.getOrDefault(lock.transactionNum, new ArrayList<>());
        locks.add(lock);
        transactionLocks.put(lock.transactionNum, locks);
    }

    public void releaseLockReq(LockRequest lockReq) {
        if (lockReq.releasedLocks.isEmpty()) {
            return;
        }
        for (Lock lock : lockReq.releasedLocks) {
            ResourceEntry entry = getResourceEntry(lock.name);
            LockRequest queReq = entry.waitingQueue.getFirst();
            Lock front = queReq.lock;
            getResourceEntry(lock.name).locks.remove(lock);
            removeTransLock(lock);

            if (checkConflicts(front) == null) {
                getResourceEntry(front.name).locks.add(front);
                addTransLock(front);
                entry.waitingQueue.removeFirst();
                releaseLockReq(queReq);
            }
        }
    }

    public void release(TransactionContext transaction, ResourceName name)
            throws NoLockHeldException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        synchronized (this) {
            if (getLockType(transaction, name) == LockType.NL) {
                throw new NoLockHeldException("no lock held");
            }

            ResourceEntry entry = getResourceEntry(name);
            int count = 0;
            boolean flag = false;
            while (!flag) {
                if (entry.locks.get(count).name.equals(name) && entry.locks.get(count).transactionNum == transaction.getTransNum()) {
                    flag = true;
                } else {
                    count++;
                }
            }
            Lock nextToRemove = entry.locks.remove(count);
            transactionLocks.get(transaction.getTransNum()).remove(nextToRemove);

            boolean compatible = true;
            while (!entry.waitingQueue.isEmpty() && compatible) {
                LockRequest nextReq = entry.waitingQueue.getFirst();
                if (entry.checkConflicts(nextReq.lock.lockType, nextReq.transaction)) {
                    compatible = false;
                } else {
                    entry.waitingQueue.removeFirst();
                    entry.locks.add(nextReq.lock);
                    transactionLocks.get(nextReq.transaction.getTransNum()).add(nextReq.lock);
                    while (!nextReq.releasedLocks.isEmpty()) {
                        Lock toRemove = nextReq.releasedLocks.remove(0);
                        release(nextReq.transaction, toRemove.name);
                    }
                    nextReq.transaction.unblock();
                }
            }
        }
    }

    /**
     * Promote a transaction's lock on `name` to `newLockType` (i.e. change
     * the transaction's lock on `name` from the current lock type to
     * `newLockType`, if its a valid substitution).
     *
     * Error checking must be done before any locks are changed. If the new lock
     * is not compatible with another transaction's lock on the resource, the
     * transaction is blocked and the request is placed at the FRONT of the
     * resource's queue.
     *
     * A lock promotion should NOT change the acquisition time of the lock, i.e.
     * if a transaction acquired locks in the order: S(A), X(B), promote X(A),
     * the lock on A is considered to have been acquired before the lock on B.
     *
     * @throws DuplicateLockRequestException if `transaction` already has a
     * `newLockType` lock on `name`
     * @throws NoLockHeldException if `transaction` has no lock on `name`
     * @throws InvalidLockException if the requested lock type is not a
     * promotion. A promotion from lock type A to lock type B is valid if and
     * only if B is substitutable for A, and B is not equal to A.
     */
    public void promote(TransactionContext transaction, ResourceName name,
                        LockType newLockType)
            throws DuplicateLockRequestException, NoLockHeldException, InvalidLockException {
        // TODO(proj4_part1): implement
        // You may modify any part of this method.
        boolean shouldBlock = false;
        synchronized (this) {
            // find targeting lock
            Lock oldLock = null;
            for (Lock lock : transactionLocks.getOrDefault(transaction.getTransNum(), new ArrayList<>())) {
                if (lock.name.equals(name)) {
                    if (!LockType.substitutable(newLockType, lock.lockType)) {
                        throw new InvalidLockException("Not Substitutable!");
                    } else if (lock.lockType == newLockType) {
                        throw new DuplicateLockRequestException("Duplicates!");
                    } else {
                        oldLock = lock;
                    }
                }
            }
            if (oldLock == null) {
                throw new NoLockHeldException("no lock held");
            }

            // update the lock
            ResourceEntry entry = getResourceEntry(name);
            Lock newLock = new Lock(name, newLockType, transaction.getTransNum());
            if (entry.checkConflicts(newLockType, transaction)) {
                List<Lock> newlocks = new ArrayList<>();
                newlocks.add(oldLock);
                entry.waitingQueue.addFirst(new LockRequest(transaction, newLock, newlocks));
                shouldBlock = true;
            } else {
                entry.locks.remove(oldLock);
                entry.locks.add(newLock);
                transactionLocks.get(transaction.getTransNum()).remove(oldLock);
                transactionLocks.get(transaction.getTransNum()).add(newLock);
            }
        }
        // block if there is conflict
        if (shouldBlock) {
            transaction.prepareBlock();
            transaction.block();
        }
    }

    /**
     * Return the type of lock `transaction` has on `name` or NL if no lock is
     * held.
     */
    public synchronized LockType getLockType(TransactionContext transaction, ResourceName name) {
        // TODO(proj4_part1): implement
        List<Lock> locks = getLocks(name);
        if (locks.isEmpty()) {
            return LockType.NL;
        }
        for (Lock l : locks) {
            if (l.transactionNum == transaction.getTransNum()) {
                return l.lockType;
            }
        }
        return LockType.NL;
    }

    /**
     * Returns the list of locks held on `name`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(ResourceName name) {
        return new ArrayList<>(resourceEntries.getOrDefault(name, new ResourceEntry()).locks);
    }

    /**
     * Returns the list of locks held by `transaction`, in order of acquisition.
     */
    public synchronized List<Lock> getLocks(TransactionContext transaction) {
        return new ArrayList<>(transactionLocks.getOrDefault(transaction.getTransNum(),
                Collections.emptyList()));
    }

    /**
     * Creates a lock context. See comments at the top of this file and the top
     * of LockContext.java for more information.
     */
    public synchronized LockContext context(String readable, long name) {
        if (!contexts.containsKey(name)) {
            contexts.put(name, new LockContext(this, null, new Pair<>(readable, name)));
        }
        return contexts.get(name);
    }

    /**
     * Create a lock context for the database. See comments at the top of this
     * file and the top of LockContext.java for more information.
     */
    public synchronized LockContext databaseContext() {
        return context("database", 0L);
    }
}
