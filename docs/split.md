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

If any of the PrepareAddRange commands in step 1 fail, just destroy the failed
placement(s) and try again on some other node. The predecessor range is still
Ready, so there is no particular harm in waiting while we try again.

1. <strike>PrepareAddRange</strike>
   1. <strike>PrepareAddRange(b, 2)</strike>
   2. <strike>PrepareAddRange(c, 3)</strike>
1. PrepareAddRange (retry)
   1. PrepareAddRange(d, 2)
   2. PrepareAddRange(e, 3)

or

1. <strike>PrepareAddRange</strike>
   1. <strike>PrepareAddRange(b, 2)</strike>
   2. PrepareAddRange(c, 3)
2. PrepareAddRange (retry)
   1. PrepareAddRange(d, 2)

or

1. <strike>PrepareAddRange</strike>
   1. PrepareAddRange(b, 2)
   2. <strike>PrepareAddRange(c, 3)</strike>
2. PrepareAddRange (retry)
   1. PrepareAddRange(d, 3)

[_TestSplitFailure_PrepareAddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_PrepareAddRange)

Note that cancellation isn't currently part of the Rangelet API (because the
command methods don't include a context param), so we have to dumbly wait until
both sides complete their PrepareAddRange before we can proceed, even if one
fails fast. Not a huge deal, but pointless work.

---

If step 2 fails -- the source placement failed to PrepareDropRange -- just retry
forever (and probably alert an operator). This isn't an emergency (the source
placement is still ready), but indicates that something is quite broken.

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. <strike>PrepareDropRange(a, 1)</strike>
3. PrepareDropRange(a, 1) (retry)


[_TestSplitFailure_PrepareDropRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_PrepareDropRange)

---

If step 3 fails, prepareDrop any destination placements which became ready (i.e.
the ones which _didn't_ fail), revert the source placement to ready, drop the
placements which failed to become ready, and retry their placement.

1. PrepareAddRange
   1. PrepareAddRange(b, 2)
   2. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. <strike>AddRange</strike>
   1. <strike>AddRange(b, 2)</strike>
   2. <strike>AddRange(c, 3)</strike>
4. AddRange(a, 1)
5. DropRange
   1. DropRange(b, 2)
   2. DropRange(c, 3)
6. PrepareAddRange (retry)
   1. PrepareAddRange(d, 2)
   2. PrepareAddRange(e, 3)

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
5. AddRange(a, 1)
6. DropRange
   1. DropRange(b, 2)
7. PrepareAddRange (retry)
   1. PrepareAddRange(d, 2)

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
5. AddRange(a, 1)
6. DropRange
   1. DropRange(c, 3)
7. PrepareAddRange (retry)
   1. PrepareAddRange(c, 3)

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
