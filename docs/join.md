# Join

When two ranges (1, 2) are assigned to nodes (a, b), and we want to join them
into a single range (3) assined to a different node (c), we **join** them.

1. PrepareAddRange(c, 3)
2. PrepareDropRange(a, 1)
3. PrepareDropRange(a, 2)
4. AddRange(c, 3)
5. DropRange(a, 1)
6. DropRange(a, 2)

## Failures

tk
