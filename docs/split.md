# Split

When a range (1) is assigned to a node (a), and we want to split it into two
separate ranges (2, 3) assigned to different nodes (b, c), we **split** it.

1. PrepareAddRange(b, 2)
2. PrepareAddRange(c, 3)
3. PrepareDropRange(a, 1)
4. AddRange(b, 2)
5. AddRange(c, 3)
6. DropRange(a, 1)

## Failures

tk
