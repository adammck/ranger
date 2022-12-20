# Move

When a range is assigned to a node (a), and we want it to be assigned to a
different node (b), we **move** it.

1. Prepare(b)
2. Deactivate(a)
3. Activate(b)
4. Drop(a)

[_TestMove_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMove)

## Failures

If step 1 fails, abort the move:

1. <strike>Prepare(b)</strike>

[_TestMoveFailure_Prepare_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_Prepare)

---

If step 2 fails, do nothing. We are stuck until the source placement
relinquishes the range:

1. Prepare(b)
2. <strike>Deactivate(a)</strike>

[_TestMoveFailure_Deactivate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_Deactivate)

---

If step 3 fails, reactivate the source placement, drop the destination
placement, and abort the move:

1. Prepare(b)
2. Deactivate(a)
3. <strike>Activate(b)</strike>
4. Activate(a)
5. Drop(b)

[_TestMoveFailure_Activate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_Activate)

---

If step 4 fails, do nothing but keep trying forever:

1. Prepare(b)
2. Deactivate(a)
3. Activate(b)
4. <strike>Drop(a)</strike>

[_TestMoveFailure_Drop_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_Drop)
