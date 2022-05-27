# Move

When a range is assigned to a node (a), and we want it to be assigned to a
different node (b), we **move** it.

1. PrepareAddRange(b)
2. PrepareDropRange(a)
3. AddRange(b)
4. DropRange(a)

## Failures

If step 1 fails, abort the move:

1. <strike>PrepareAddRange(b)</strike>

If step 2 fails, drop the destination placement and abort the move:

1. PrepareAddRange(b)
2. <strike>PrepareDropRange(a)</strike>
3. DropRange(b)

If step 3 fails, revert the source range to ready, drop the destination
placement, and abort the move:

1. PrepareAddRange(b)
2. PrepareDropRange(a)
3. <strike>AddRange(b)</strike>
4. AddRange(a)
5. DropRange(b)

If step 4 fails, do nothing but keep trying forever:

1. PrepareAddRange(b)
2. PrepareDropRange(a)
3. AddRange(b)
4. <strike>DropRange(a)</strike>
