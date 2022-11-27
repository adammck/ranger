# Split

When a range (1) is assigned to a node (a), and we want to split it into two
separate ranges (2, 3) assigned to different nodes (b, c), we **split** it.

1. Prepare
   1. Prepare(b, 2)
   2. Prepare(c, 3)
2. Deactivate(a, 1)
3. Activate
   1. Activate(b, 2)
   2. Activate(c, 3)
4. Drop(a, 1)

[_TestSplit_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplit)

## Failures

If any of the Prepare commands in step 1 fail, just destroy the failed
placement(s) and try again on some other node. The predecessor range is still
active, so there is no particular harm in waiting while we try again.

1. <strike>Prepare</strike>
   1. <strike>Prepare(b, 2)</strike>
   2. <strike>Prepare(c, 3)</strike>
1. Prepare (retry)
   1. Prepare(d, 2)
   2. Prepare(e, 3)

or

1. <strike>Prepare</strike>
   1. <strike>Prepare(b, 2)</strike>
   2. Prepare(c, 3)
2. Prepare (retry)
   1. Prepare(d, 2)

or

1. <strike>Prepare</strike>
   1. Prepare(b, 2)
   2. <strike>Prepare(c, 3)</strike>
2. Prepare (retry)
   1. Prepare(d, 3)

[_TestSplitFailure_Prepare_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_Prepare)

Note that cancellation isn't currently part of the Rangelet API (because the
command methods don't include a context param), so we have to dumbly wait until
both sides complete their Prepare before we can proceed, even if one fails fast.
Not a huge deal, but pointless work.

---

If step 2 fails -- the source placement failed to Deactivate -- just retry
forever (and probably alert an operator). This isn't an emergency (the source
placement is still active), but indicates that something is quite broken.

1. Prepare
   1. Prepare(b, 2)
   2. Prepare(c, 3)
2. <strike>Deactivate(a, 1)</strike>
3. Deactivate(a, 1) (retry)


[_TestSplitFailure_Deactivate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_Deactivate)

---

If step 3 fails, deactivate any destination placements which became active (i.e.
the ones which _didn't_ fail), reactivate the source placement, drop the
placements which failed to activate, and retry their placement.

1. Prepare
   1. Prepare(b, 2)
   2. Prepare(c, 3)
2. Deactivate(a, 1)
3. <strike>Activate</strike>
   1. <strike>Activate(b, 2)</strike>
   2. <strike>Activate(c, 3)</strike>
4. Activate(a, 1)
5. Drop
   1. Drop(b, 2)
   2. Drop(c, 3)
6. Prepare (retry)
   1. Prepare(d, 2)
   2. Prepare(e, 3)

or

1. Prepare
   1. Prepare(b, 2)
   2. Prepare(c, 3)
2. Deactivate(a, 1)
3. <strike>Activate</strike>
   1. <strike>Activate(b, 2)</strike>
   2. Activate(c, 3)
4. Deactivate
   1. Deactivate(c, 3)
5. Activate(a, 1)
6. Drop
   1. Drop(b, 2)
7. Prepare (retry)
   1. Prepare(d, 2)

or

1. Prepare
   1. Prepare(b, 2)
   2. Prepare(c, 3)
2. Deactivate(a, 1)
3. <strike>Activate</strike>
   1. Activate(b, 2)
   2. <strike>Activate(c, 3)</strike>
4. Deactivate
   1. Deactivate(b, 2)
5. Activate(a, 1)
6. Drop
   1. Drop(c, 3)
7. Prepare (retry)
   1. Prepare(c, 3)

[_TestSplitFailure_Activate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_Activate)

This one is probably the most complex to recover from.

---

If step 4 fails, do nothing but keep trying forever:

1. Prepare
   1. Prepare(b, 2)
   2. Prepare(c, 3)
2. Deactivate(a, 1)
3. Activate
   1. Activate(b, 2)
   2. Activate(c, 3)
4. <strike>Drop(a, 1)</strike>

[_TestSplitFailure_Drop_](https://cs.github.com/adammck/ranger?q=symbol%3ATestSplitFailure_Drop)
