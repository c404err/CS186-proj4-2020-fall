package edu.berkeley.cs186.database.concurrency;

import edu.berkeley.cs186.database.Transaction;
import edu.berkeley.cs186.database.TransactionContext;
import java.util.*;

/**
 * LockUtil is a declarative layer which simplifies multigranularity lock
 * acquisition for the user (you, in the last task of Part 2). Generally
 * speaking, you should use LockUtil for lock acquisition instead of calling
 * LockContext methods directly.
 */
public class LockUtil {
    /**
     * Ensure that the current transaction can perform actions requiring
     * `requestType` on `lockContext`.
     *
     * `requestType` is guaranteed to be one of: S, X, NL.
     *
     * This method should promote/escalate/acquire as needed, but should only
     * grant the least permissive set of locks needed. We recommend that you
     * think about what to do in each of the following cases:
     * - The current lock type can effectively substitute the requested type
     * - The current lock type is IX and the requested lock is S
     * - The current lock type is an intent lock
     * - None of the above
     *
     * You may find it useful to create a helper method that ensures you have
     * the appropriate locks on all ancestors.
     */
    public static void ensureSufficientLockHeld(LockContext lockContext, LockType requestType) {
        // requestType must be S, X, or NL
        assert (requestType == LockType.S || requestType == LockType.X || requestType == LockType.NL);

        // Do nothing if the transaction or lockContext is null
        TransactionContext transaction = TransactionContext.getTransaction();
        if (transaction == null | lockContext == null) return;
        // You may find these variables useful
        LockContext parentContext = lockContext.parentContext();
        LockType effectiveLockType = lockContext.getEffectiveLockType(transaction);
        LockType explicitLockType = lockContext.getExplicitLockType(transaction);

        // TODO(proj4_part2): implement
//        if (!LockType.substitutable(effectiveLockType, requestType)) {
//            //List<LockContext> parent = new ArrayList<>();
//
//            LockType parentLockType = LockType.parentLock(requestType);
//            //LockType childType = LockType.NL;
//            List<LockContext> parent = getParents(parentContext);
//
//            for (LockContext lc : parent) {
//                if (!LockType.substitutable(lc.getExplicitLockType(transaction), parentLockType)) {
//                    if (lc.getExplicitLockType(transaction) == LockType.NL)
//                        lc.acquire(transaction, parentLockType);
//                    else
//                        lc.promote(transaction, parentLockType);
//                }
//            }
//
//
//
//            if (lockContext.getNumChildren(transaction) != 0 &&
//                childType(transaction, lockContext) == requestType) {
//                lockContext.escalate(transaction);
//            } else if (lockContext.getExplicitLockType(transaction) != LockType.NL &&
//                        LockType.substitutable(requestType, lockContext.getExplicitLockType(transaction))){
//                lockContext.promote(transaction, requestType);
//            } else {
//                lockContext.acquire(transaction, requestType);
//            }
//            return;
//        }

//        if (explicitLockType.equals(LockType.IX) && requestType.equals(LockType.S)) {
//            lockContext.parent.promote(transaction, LockType.SIX);
//            ensureSufficientLockHeld(lockContext, requestType);
//
//            lockContext.escalate(transaction);
//            return;
//        }
//
//        if (lockContext.parent != null || lockContext.parent.parent == null) {
//            lockContext.parent.escalate(transaction);
//            ensureSufficientLockHeld(lockContext, requestType);
//            return;
//        }

        if (LockType.substitutable(effectiveLockType, requestType)) return;


        if (parentContext != null) ensureSufficientLockHeld(parentContext, LockType.parentLock(requestType));

        if (effectiveLockType.equals(LockType.NL)) {
            lockContext.acquire(transaction, requestType);
        } else {
            boolean flag = true;
            for (Lock lock : lockContext.lockman.getLocks(transaction)) {
                if (lock.name.equals(lockContext.name)) {
                    if (!LockType.substitutable(requestType, lock.lockType)) {
                        flag = false;
                        lockContext.escalate(transaction);
                        ensureSufficientLockHeld(lockContext, requestType);
                        break;
                    }
                }
            }
            if (flag) {
                lockContext.promote(transaction, requestType);
            }
        }
    }

    // TODO(proj4_part2) add any helper methods you want
    private static boolean checkEscalate(TransactionContext transaction, LockContext lockContext) {
        if (lockContext.parent == null || lockContext.parent.parent == null) {
            return false;
        }
        return lockContext.parent.children.size() >= 10 &&
            5 * lockContext.parent.getNumChildren(transaction) >= lockContext.parent.children.size();
    }

    private static void recAcquire(TransactionContext transaction, LockType locktype, LockContext lockContext) {
        if (lockContext == null) return;
        if (lockContext.getEffectiveLockType(transaction) != LockType.NL) return;
        recAcquire(transaction, locktype, lockContext.parent);
        lockContext.acquire(transaction, locktype);
    }

    private static void recPromote(TransactionContext transaction, LockType locktype, LockContext lockContext) {
        if (lockContext == null) return;
        if (!LockType.substitutable(locktype, lockContext.getEffectiveLockType(transaction))) return;
        recPromote(transaction, locktype, lockContext.parent);
        lockContext.promote(transaction, locktype);
    }

    private static List<LockContext> getParents(LockContext lockContext) {
        List<LockContext> parent = new ArrayList<>();
        while (lockContext != null) {
            parent.add(0, lockContext);
            lockContext = lockContext.parent;
        }
        return parent;
    }
    private static LockType childType(TransactionContext transaction, LockContext lockContext) {
        LockType child = LockType.NL;
        for (LockContext c : lockContext.children.values()) {
            if (!LockType.substitutable(child, c.getExplicitLockType(transaction))) {
                child = c.getExplicitLockType(transaction);
            }
        }
        return child;
    }
}
