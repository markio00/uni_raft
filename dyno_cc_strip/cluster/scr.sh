#!/usr/bin/bash

S="raft_log"
WNAME="cluster"




build_all() {
	./build
}

create_window() {
    if ! tmux has-session -t "$S" 2>/dev/null; then
        tmux new-session -d -s "$S" -n "main"
    fi

    tmux kill-window -t "$S:$WNAME"

    tmux new-window -n "$WNAME"

    tmux select-window -t "$S:$WNAME"

    tmux split-window -v -t "$S:$WNAME"

    tmux select-layout -t "$S:$WNAME" even-horizontal 
    tmux select-layout -t "$S:$WNAME" even-vertical 

    tmux split-window -h -t "$S:$WNAME.0"
    tmux split-window -h -t "$S:$WNAME.2"
}

containers_run() {
	tmux send-keys -t "$S:$WNAME.3" "docker compose up -d" C-m
	sleep 1
	tmux send-keys -t "$S:$WNAME.0" "docker compose logs -f node4" C-m
	tmux send-keys -t "$S:$WNAME.1" "docker compose logs -f node5" C-m
	tmux send-keys -t "$S:$WNAME.2" "docker compose logs -f node6" C-m
}

containers_destroy() {
	tmux send-keys -t "$S:$WNAME.3" "docker compose down" C-m
}

close_session() {
    tmux kill-session -t "$S"
}



if [ $# -eq 0 ]; then
	containers_destroy
	build_all
	create_window
	containers_run
fi


if [[ "$1" == "quit" ]] ; then
	containers_destroy
	close_session
fi

if [[ "$1" == "drop" ]] ; then
	containers_destroy
fi

if [[ "$1" == "run" ]] ; then
	containers_run
fi

if [[ "$1" == "build" ]] ; then
	build_all
fi
