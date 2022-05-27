# Placement

When a range isn't assigned to any node, we **place** it on a node (a).

1. PrepareAddRange(a)
2. AddRange(a)

## Failures

If step 1 fails, abort the place:

1. <strike>PrepareAddRange(a)</strike>

If step 2 fails, drop the placement and abort the place:

1. PrepareAddRange(a)
2. <strike>AddRange(a)</strike>
3. DropRange(a)
