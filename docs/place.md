# Place

When a range isn't assigned to any node, we **place** it on a node (a).

1. PrepareAddRange(a)
2. AddRange(a)

[_TestPlace_](https://cs.github.com/adammck/ranger?q=symbol%3ATestPlace)

## Failures

If step 1 fails, abort the place:

1. <strike>PrepareAddRange(a)</strike>

[_TestPlaceFailure_PrepareAddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestPlaceFailure_PrepareAddRange)

---

If step 2 fails, drop the placement and abort the place:

1. PrepareAddRange(a)
2. <strike>AddRange(a)</strike>
3. DropRange(a)

[_TestPlaceFailure_AddRange_](https://cs.github.com/adammck/ranger?q=symbol%3ATestPlaceFailure_AddRange)
