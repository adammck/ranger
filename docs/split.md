# Split

When a range (1) is assigned to a node (a), and we want to split it into two
separate ranges (2, 3) assigned to different nodes (b, c), we **split** it.

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. AddRange
   1. AddRange(b, 2)
   2. AddRange(c, 3)
4. DropRange(a, 1)

[_TestSplit_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplit)

## Failures

If step 1 fails, drop any destination placements which succeeded, and abort the
split:

1. <strike>PrepareAddRange</strike>
   1. <strike>PrepareAddRange(b, 2)</strike>
   2. <strike>PrepareAddRange(c, 3)</strike>

or

1. <strike>PrepareAddRange</strike>
   1. <strike>PrepareAddRange(b, 2)</strike>
   2. PrepareAddRange(c, 3)
2. DropRange
   1. DropRange(c, 3)

or

1. <strike>PrepareAddRange</strike>
   1. PrepareAddRange(b, 2)
   2. <strike>PrepareAddRange(c, 3)</strike>
2. DropRange
   1. DropRange(b, 2)

[_TestSplitFailure_PrepareAddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_PrepareAddRange)

Note that cancellation isn't currently part of the Rangelet API (because the
command methods don't include a context param), so we have to dumbly wait until
both sides complete their PrepareAddRange before we can proceed, even if one
fails fast. Not a huge deal, but pointless work.

---

If step 2 fails, drop the destination placements and abort the split:

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. <strike>PrepareDropRange(a, 1)</strike>
3. DropRange
   1. DropRange(b, 2)
   2. DropRange(c, 3)

[_TestSplitFailure_PrepareDropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_PrepareDropRange)

---

If step 3 fails, prepareDrop any destination placements which became ready, drop
both destination placements, revert the source placement to ready, and abort the
split:

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. <strike>AddRange</strike>
   1. <strike>AddRange(b, 2)</strike>
   2. <strike>AddRange(c, 3)</strike>
4. DropRange
   1. DropRange(b, 2)
   2. DropRange(c, 3)
5. AddRange(a, 1)

or

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. <strike>AddRange</strike>
   1. <strike>AddRange(b, 2)</strike>
   2. AddRange(c, 3)
4. PrepareDropRange
   1. PrepareDropRange(c, 3)
5. DropRange
   1. DropRange(b, 2)
   2. DropRange(c, 3)
6. AddRange(a, 1)

or

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. <strike>AddRange</strike>
   1. AddRange(b, 2)
   2. <strike>AddRange(c, 3)</strike>
4. PrepareDropRange
   1. PrepareDropRange(b, 2)
5. DropRange
   1. DropRange(b, 2)
   2. DropRange(c, 3)
6. AddRange(a, 1)

[_TestSplitFailure_AddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_AddRange)

This one is probably the most complex to recover from.

---

If step 4 fails, do nothing but keep trying forever:

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. AddRange
   1. AddRange(b, 2)
   2. AddRange(c, 3)
4. <strike>DropRange(a, 1)</strike>

[_TestSplitFailure_DropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_DropRange)
