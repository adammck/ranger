cmds=()

# keys
a=$(echo -n a | base64); export a
a1=$(echo -n a1 | base64); export a1
a2=$(echo -n a2 | base64); export a2
b=$(echo -n b | base64); export b
b1=$(echo -n b1 | base64); export b1
b2=$(echo -n b2 | base64); export b2
c=$(echo -n c | base64); export c

# vals
zzz=$(echo -n zzz | base64); export zzz
yyy=$(echo -n yyy | base64); export yyy
xxx=$(echo -n xxx | base64); export xxx
www=$(echo -n www | base64); export www

# Fail if the given port number is currently in use. This is better than trying
# to run some command and failing with a weird error.
assert_port_available() {
    #>&3 echo "# assert_port_available $@"
    nc -z localhost "$1" && fail "port $1 is already in use"
    return 0
}

# Start a node in the background on the given port.
# Stop it by calling defer_stop_cmds in teardown.
start_node() {
    #>&3 echo "# start_node $@"
    start_cmd "$1" ./kv -node -addr ":$1"
}

# Start a controller in the background on the given port.
# Stop it by calling defer_stop_cmds in teardown.
start_controller() {
    #>&3 echo "# start_controller $@"
    start_cmd "$1" ./kv -controller -addr ":$1"
}

stop_controller() {
    #>&3 echo "# start_controller $@"
    stop_cmd "$PID_9000"
}

# Run a command which is expected to listen on a port in the background. Block
# until the port is open, even if that's forever. Store the PID of the command,
# so it can be killed by calling defer_stop_cmds (probably in teardown).
# 
# Usage: start_cmd port cmd [args...]
# E.g.:  start_cmd 8000 
start_cmd() {
    #>&3 echo "# start_cmd $@"
    local port=$1
    shift

    assert_port_available "$port"
    "$@" &
    local pid=$!

    defer_stop_cmds "$pid"
    wait_port "$port" "$pid"
}

stop_cmd() {
    #>&3 echo "# stop_cmd $@"
    if test -n "$1"; then # if non-empty
        if kill -s 0 "$1" 2>/dev/null; then # and still running
            kill "$1"
        fi
    fi
}

defer_stop_cmds() {
    #>&3 echo "# defer_stop_cmds $1"
    cmds+=("$1")
}

stop_cmds() {
    #>&3 echo "# stop_cmds $1"
    for pid in "${cmds[@]}"; do
        stop_cmd "$pid"
    done
}

# Block until either the given port is listening, or the given PID is no longer
# running.
wait_port() {
    local port=$1
    local pid=$2

    while : ; do
        if ! kill -s 0 "$pid" 2>/dev/null; then
            fail "program terminated while waiting for port $port to listen"
        fi

        if nc -z localhost "$port"; then
            return 0;
        fi

        sleep 0.1
    done
}
