setup_file() {
    go build

    # keys
    export a=$(echo -n a | base64)
    export a1=$(echo -n a1 | base64)
    export a2=$(echo -n a2 | base64)
    export b=$(echo -n b | base64)
    export b1=$(echo -n b1 | base64)
    export b2=$(echo -n b2 | base64)
    export c=$(echo -n c | base64)

    # vals
    export zzz=$(echo -n zzz | base64)
    export yyy=$(echo -n yyy | base64)
    export xxx=$(echo -n xxx | base64)
    export www=$(echo -n www | base64)
}

setup() {
    # TODO: I don't want to vendor bats, but do something less dumb than this
    load '/Users/adammck/code/src/github.com/bats-core/bats-support/load.bash'
    load '/Users/adammck/code/src/github.com/bats-core/bats-assert/load.bash'

    ./kv -node -addr ":8001" >$BATS_TMPDIR/kv-8001.log 2>&1 &
    PID_8001=$!

    ./kv -node -addr ":8002" >$BATS_TMPDIR/kv-8002.log 2>&1 &
    PID_8002=$!

    ./kv -node -addr ":8003" >$BATS_TMPDIR/kv-8003.log 2>&1 &
    PID_8003=$!

    # TODO: Move wait-port tool into a func in this file.
    wait-port 8001
    wait-port 8002
    wait-port 8003
}

teardown() {
    # TODO: do something sensible if the pids are invalid
    kill $PID_8001
    kill $PID_8002
    kill $PID_8003
}

@test "read write to unassigned range" {

    # Try to read a key from node 1 which is not assigned.
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Try to write same.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'
}

@test "assign range" {

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Try to read again. Different error.
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: NotFound'
    assert_line -n 2 '  Message: no such key'

    # Try to write again. Success!
    zzz=$(echo -n zzz | base64)
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    assert_success

    # Read again. Success!
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'
}

@test "dump range" {

    # Dump a range which doesn't exist.
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: InvalidArgument'
    assert_line -n 2 '  Message: range not found'

    # Assign the range [a,b) to node 1.
    run bin/client.sh 8001 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}}'
    assert_success

    # Try to dump the contents of range 1 from node 1.
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only dump ranges in the TAKEN state'

    # Take the range from node 1.
    run bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'
    assert_success

    # Try to dump again. Success!
    run bin/client.sh 8001 kv.KV.Dump '{"range": {"key": 1}}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'
}

@test "move range" {

    # ---- setup

    # Give the range [a,b) to node 1, Put some keys, and Take it.
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}'
    bin/client.sh 8001 ranger.Node.Give '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$zzz'"}'
    bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'

    # ---- test

    # Move that range to node 2
    # TODO: This succeeds even if node 1 refuses to dump it, because the
    #       transfer starts asynchronously after this rpc has returned. That
    #       doesn't seem ideal.
    run bin/client.sh 8002 ranger.Node.Give '{"range": {"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}]}'
    assert_success

    # Read the key from node 1. This still works!
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'

    # Try to write to node 1. This no longer works.
    run bin/client.sh 8001 kv.KV.Put '{"key": "'$a'", "value": "'$yyy'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only PUT to ranges in the READY state'

    # Read the key from node 2. This works now!
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'

    # Try to write to node 2. This doesn't work yet.
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$yyy'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: can only PUT to ranges in the READY state'

    # Drop the range from node 1.
    run bin/client.sh 8001 ranger.Node.Drop '{"range": {"key": 1}}'
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Try to read from node 1. No longer works. The range is gone.
    run bin/client.sh 8001 kv.KV.Get '{"key": "'$a'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Enable writes on node 2. This works.
    # This would have worked at any time after node 2 finished fetching the
    # range from node 1, but one mustn't enable writes until the old node has
    # stopped serving reads, to avoid stale reads.
    run bin/client.sh 8002 ranger.Node.Serve '{"range": {"key": 1}}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Try to write to node 2. Success!
    run bin/client.sh 8002 kv.KV.Put '{"key": "'$a'", "value": "'$yyy'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  '
    assert_line -n 2 '}'

    # Read the new value back. Success!
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$a'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$yyy'"'
    assert_line -n 2 '}'
}

@test "join ranges" {

    # ---- setup

    # Assign a couple of adjacent ranges, write some keys, and TAKE them to
    # prepare for moving.

    # Node 1: range [a,b)
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$b'"}'
    bin/client.sh 8001 ranger.Node.Give '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a1'", "value": "'$zzz'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a2'", "value": "'$yyy'"}'
    bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'

    # Node 2: range [b,c)
    r2='{"ident": {"key": 2}, "start": "'$b'", "end": "'$c'"}'
    bin/client.sh 8002 ranger.Node.Give '{"range": '"$r2"'}'
    bin/client.sh 8002 kv.KV.Put '{"key": "'$b1'", "value": "'$xxx'"}'
    bin/client.sh 8002 kv.KV.Put '{"key": "'$b2'", "value": "'$www'"}'
    bin/client.sh 8002 ranger.Node.Take '{"range": {"key": 2}}'

    # ---- test

    # TODO: Test joining a range where one of the parents is already on the
    #       destination node. Think it should Just Work but things might get
    #       weird when serving reads during the transition.

    # Move both ranges to a single new range on node 3
    run bin/client.sh 8003 ranger.Node.Give '{"range": {"ident": {"key": 3}, "start": "'$a'", "end": "'$c'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}, {"range": '"$r2"', "node": "localhost:8002"}]}'
    assert_success

    # TODO: Can be arbitrary delay here because Give returns before range
    #       recovery is finished (or even started). Need some rpc to wait for
    #       the recovery to succeed or fail. Probably just for testing.
    sleep 0.5

    # Read a key that was on node 1
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$a2'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$yyy'"'
    assert_line -n 2 '}'

    # Read a key that was on node 2
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$b1'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$xxx'"'
    assert_line -n 2 '}'
}

@test "split ranges" {

    # ---- setup

    # Assign a range and write some keys.                                                                                                                                                                                                                                                                             
    # Node 1: range [a,c)
    r1='{"ident": {"key": 1}, "start": "'$a'", "end": "'$c'"}'
    bin/client.sh 8001 ranger.Node.Give '{"range": '"$r1"'}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a1'", "value": "'$zzz'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$a2'", "value": "'$yyy'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$b1'", "value": "'$xxx'"}'
    bin/client.sh 8001 kv.KV.Put '{"key": "'$b2'", "value": "'$www'"}'
    bin/client.sh 8001 ranger.Node.Take '{"range": {"key": 1}}'

    # ---- test

    # Move half of the range to n2
    run bin/client.sh 8002 ranger.Node.Give '{"range": {"ident": {"key": 2}, "start": "'$a'", "end": "'$b'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}]}'
    assert_success

    # Move the other half to n3
    run bin/client.sh 8003 ranger.Node.Give '{"range": {"ident": {"key": 3}, "start": "'$b'", "end": "'$c'"}, "parents": [{"range": '"$r1"', "node": "localhost:8001"}]}'
    assert_success

    # TODO: Can be arbitrary delay here because Give returns before range
    #       recovery is finished (or even started). Need some rpc to wait for
    #       the recovery to succeed or fail. Probably just for testing.
    sleep 0.5

    # Read a key that should have made it to n2 / [a,b)
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$a1'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$zzz'"'
    assert_line -n 2 '}'

    # Read a key that was in the parent range, but should NOT have made it to n2
    run bin/client.sh 8002 kv.KV.Get '{"key": "'$b1'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'

    # Read a key that should have made it to n3 / [b,c)
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$b1'"}'
    assert_success
    assert_line -n 0 '{'
    assert_line -n 1 '  "value": "'$xxx'"'
    assert_line -n 2 '}'

    # Read a key that was in the parent range, but should NOT have made it to n3
    run bin/client.sh 8003 kv.KV.Get '{"key": "'$a1'"}'
    assert_failure
    assert_line -n 0 'ERROR:'
    assert_line -n 1 '  Code: FailedPrecondition'
    assert_line -n 2 '  Message: no valid range'
}
