# Join

When two ranges (1, 2) are assigned to nodes (a, b), and we want to join them
into a single range (3) assined to a different node (c), we **join** them.

1. Prepare(c, 3)
2. Deactivate
   1. Deactivate(a, 1)
   2. Deactivate(a, 2)
3. Activate(c, 3)
4. Drop
   1. Drop(a, 1)
   2. Drop(a, 2)

[_TestJoin_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoin)

## Failures

If step 1 fails, just abort the join:

1. <strike>Prepare(c, 3)</strike>

[_TestJoinFailure_Prepare_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_Prepare)

---

If step 2 fails, reactivate any of the source placements which were deactivated,
drop the destination placement, and abort the join:

1. Prepare(c, 3)
2. <strike>Deactivate</strike>
   1. <strike>Deactivate(a, 1)</strike>
   2. <strike>Deactivate(a, 2)</strike>
3. Drop(c, 3)

or

1. Prepare(c, 3)
2. <strike>Deactivate</strike>
   1. <strike>Deactivate(a, 1)</strike>
   2. Deactivate(a, 2)
3. Activate
   1. Activate(a, 2)
4. Drop(c, 3)

or

1. Prepare(c, 3)
2. <strike>Deactivate</strike>
   1. Deactivate(a, 1)
   2. <strike>Deactivate(a, 2)</strike>
3. Activate
   1. Activate(a, 1)
4. Drop(c, 3)

[_TestJoinFailure_Deactivate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_Deactivate)

---

If step 3 fails, reactivate source placements, drop the destination placement,
and abort the join:

1. Prepare(c, 3)
2. Deactivate
   1. Deactivate(a, 1)
   2. Deactivate(a, 2)
3. <strike>Activate(c, 3)</strike>
4. Activate
   1. Activate(a, 1)
   2. Activate(a, 2)
4. Drop(c, 3)

[_TestJoinFailure_Activate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_Activate)

---

If step 4 fails, do nothing but keep trying forever until both source placements
are dropped:

1. Prepare(c, 3)
2. Deactivate
   1. Deactivate(a, 1)
   2. Deactivate(a, 2)
3. Activate(c, 3)
4. <strike>Drop</strike>
   1. <strike>Drop(a, 1)</strike>
   2. Drop(a, 2)

[_TestJoinFailure_Drop_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_Drop)
