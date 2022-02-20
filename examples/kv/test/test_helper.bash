# map of port -> pid
declare -A cmds

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

# Returns a command which can be `run` to access the ranger client aimed at a
# controller on the given port.
ranger_client() {
    #>&3 echo "# ranger_client $@"
    local port=$1
    shift

    if [[ -z "${RANGER_CLIENT}" ]]; then
        fail "RANGER_CLIENT must be set"
    fi

    echo "$RANGER_CLIENT" -addr "127.0.0.1:${port}"
}

# Fail if the given port number is currently in use. This is better than trying
# to run some command and failing with a weird error.
assert_port_available() {
    #>&3 echo "# assert_port_available $@"
    local port=$1
    nc -z localhost "$port" && fail "port $port is already in use"
    return 0
}

# Start a node in the background on the given port.
# Stop it by calling defer_stop_cmds in teardown.
start_node() {
    #>&3 echo "# start_node $@"
    local port=$1
    start_cmd "$port" ./kv -node -addr "127.0.0.1:$port" -log-reqs
}

# Start a controller in the background on the given port.
# Stop it by calling defer_stop_cmds in teardown.
start_controller() {
    #>&3 echo "# start_controller $@"
    local port=$1
    start_cmd "$port" ./kv -controller -addr "127.0.0.1:$port"
}

start_proxy() {
    #>&3 echo "# start_proxy $@"
    local port=$1
    start_cmd "$port" ./kv -proxy -addr "127.0.0.1:$port" -log-reqs
}

start_consul() {
    #>&3 echo "# start_consul $@"

    # We don't actually set the ports here because I'm too lazy to pass them
    # around. This means that only a single consul-using test can run at once.
    # TODO: Fix this.

    start_cmd 8500 consul agent -dev 1>/dev/null
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

    defer_stop_cmds "$port" "$pid"
    wait_port "$port" "$pid"
}

# Send the given signal to the command serving the given port.
send_signal() {
    #>&3 echo "# send_signal $@"
    local pid=$1
    local signal=$2

    if test -n "$pid"; then # if non-empty
        if kill -s 0 "$pid" 2>/dev/null; then # and still running
            kill -s "$signal" "$pid"
        fi
    fi
}

stop_cmd() {
    #>&3 echo "# stop_cmd $@"
    send_signal "$pid" "INT"
}

# Sends SIGQUIT to the command serving the given port.
crash_cmd() {
    #>&3 echo "# crash_cmd $@"
    local port=$1
    local pid=${cmds[$port]}

    if test "$pid" = ""; then
        fail "no command serving port $port"
    fi

    kill -s QUIT "$pid"

    # Block until the process is no longer running.
    while : ; do
        if ! kill -s 0 "$pid" 2>/dev/null; then
            break
        fi

        sleep 0.1
    done

    unset "cmds[$port]"
}

defer_stop_cmds() {
    #>&3 echo "# defer_stop_cmds $@"
    local port=$1
    local pid=$2

    if test -n "${cmds[$port]}"; then
        fail "already have command on port $port"
        return
    fi

    cmds[$port]=$pid
}

stop_cmds() {
    #>&3 echo "# stop_cmds $@"
    for pid in "${cmds[@]}"; do
        stop_cmd "$pid"
    done
}

# Block until either the given port is listening, or the given PID is no longer
# running.
wait_port() {
    #>&3 echo "# wait_port $@"
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
