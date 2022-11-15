# Example: Cache

This is a simple distributed read-through cache using ranger. Send a URL, and it
will be hashed and routed to a node. That node will fetch and cache it (if it's
not already cached) and then return it.
