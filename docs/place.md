# Place

When a range isn't assigned to any node, we **place** it on a node (a).

1. Prepare(a)
2. Activate(a)

[_TestPlace_](https://cs.github.com/adammck/ranger?q=symbol%3ATestPlace)

## Failures

If step 1 fails, abort the place:

1. <strike>Prepare(a)</strike>

[_TestPlaceFailure_Prepare_](https://cs.github.com/adammck/ranger?q=symbol%3ATestPlaceFailure_Prepare)

---

If step 2 fails, drop the placement and abort the place:

1. Prepare(a)
2. <strike>Activate(a)</strike>
3. Drop(a)

[_TestPlaceFailure_Activate_](https://cs.github.com/adammck/ranger?q=symbol%3ATestPlaceFailure_Activate)
