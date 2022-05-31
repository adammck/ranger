# Join

When two ranges (1, 2) are assigned to nodes (a, b), and we want to join them
into a single range (3) assined to a different node (c), we **join** them.

1. PrepareAddRange(c, 3)
2. PrepareDropRange
   1. PrepareDropRange(a, 1)
   2. PrepareDropRange(a, 2)
3. AddRange(c, 3)
4. DropRange
   1. DropRange(a, 1)
   2. DropRange(a, 2)

[_TestJoin_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoin)

## Failures

If step 1 fails, just abort the join:

1. <strike>PrepareAddRange(c, 3)</strike>

[_TestJoinFailure_PrepareAddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_PrepareAddRange)

---

If step 2 fails, revert any source placements which suceeded prepareDrop back to
ready, drop the destination placement, and abort the join:

1. PrepareAddRange(c, 3)
2. <strike>PrepareDropRange</strike>
   1. <strike>PrepareDropRange(a, 1)</strike>
   2. <strike>PrepareDropRange(a, 2)</strike>
3. DropRange(c, 3)

or

1. PrepareAddRange(c, 3)
2. <strike>PrepareDropRange</strike>
   1. <strike>PrepareDropRange(a, 1)</strike>
   2. PrepareDropRange(a, 2)
3. AddRange
   1. AddRange(a, 2)
4. DropRange(c, 3)

or

1. PrepareAddRange(c, 3)
2. <strike>PrepareDropRange</strike>
   1. PrepareDropRange(a, 1)
   2. <strike>PrepareDropRange(a, 2)</strike>
3. AddRange
   1. AddRange(a, 1)
4. DropRange(c, 3)

[_TestJoinFailure_PrepareDropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_PrepareDropRange)

---

If step 3 fails, revert source placements back to ready, drop the destination
placement, and abort the join:

1. PrepareAddRange(c, 3)
2. PrepareDropRange
   1. PrepareDropRange(a, 1)
   2. PrepareDropRange(a, 2)
3. <strike>AddRange(c, 3)</strike>
4. AddRange
   1. AddRange(a, 1)
   2. AddRange(a, 2)
4. DropRange(c, 3)

[_TestJoinFailure_AddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_AddRange)

---

If step 4 fails, do nothing but keep trying forever until both source placements
are dropped:

1. PrepareAddRange(c, 3)
2. PrepareDropRange
   1. PrepareDropRange(a, 1)
   2. PrepareDropRange(a, 2)
3. AddRange(c, 3)
4. <strike>DropRange</strike>
   1. <strike>DropRange(a, 1)</strike>
   2. DropRange(a, 2)

[_TestJoinFailure_DropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestJoinFailure_DropRange)
