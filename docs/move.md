# Move

When a range is assigned to a node (a), and we want it to be assigned to a
different node (b), we **move** it.

1. PrepareAddRange(b)
2. PrepareDropRange(a)
3. AddRange(b)
4. DropRange(a)

[_TestMove_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMove)

## Failures

If step 1 fails, abort the move:

1. <strike>PrepareAddRange(b)</strike>

[_TestMoveFailure_PrepareAddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_PrepareAddRange)

---

If step 2 fails, drop the destination placement and abort the move:

1. PrepareAddRange(b)
2. <strike>PrepareDropRange(a)</strike>
3. DropRange(b)

[_TestMoveFailure_PrepareDropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_PrepareDropRange)

---

If step 3 fails, revert the source range to ready, drop the destination
placement, and abort the move:

1. PrepareAddRange(b)
2. PrepareDropRange(a)
3. <strike>AddRange(b)</strike>
4. AddRange(a)
5. DropRange(b)

[_TestMoveFailure_AddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_AddRange)

---

If step 4 fails, do nothing but keep trying forever:

1. PrepareAddRange(b)
2. PrepareDropRange(a)
3. AddRange(b)
4. <strike>DropRange(a)</strike>

[_TestMoveFailure_DropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestMoveFailure_DropRange)
