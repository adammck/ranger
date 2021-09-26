package main

import (
	"fmt"
	"io/ioutil"
	"net/http"
	"sync"

	"github.com/gorilla/mux"
)

type Node struct {
	data map[string][]byte
	mu   sync.Mutex
}

type getHandler struct {
	node *Node
}

func (h *getHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k, ok := vars["key"]
	if !ok {
		http.Error(w, "Missing param: key", http.StatusNotFound)
		return
	}

	h.node.mu.Lock()
	v, ok := h.node.data[k]
	h.node.mu.Unlock()

	if !ok {
		http.Error(w, "404: Not found", http.StatusNotFound)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	w.Write(v)
}

type putHandler struct {
	node *Node
}

func (h *putHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	vars := mux.Vars(r)
	k, ok := vars["key"]
	if !ok {
		http.Error(w, "Missing param: key", http.StatusNotFound)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		http.Error(w, fmt.Sprintf("Error reading request body: %s", err), http.StatusBadRequest)
	}

	h.node.mu.Lock()
	h.node.data[k] = body
	h.node.mu.Unlock()

	w.WriteHeader(http.StatusOK)
	w.Header().Set("Content-Type", "text/plain; charset=utf-8")
	fmt.Fprintln(w, "200: OK")
}

func main() {
	n := Node{
		data: make(map[string][]byte),
	}

	gh := getHandler{node: &n}
	ph := putHandler{node: &n}

	r := mux.NewRouter()
	r.Handle("/{key}", &gh).Methods("GET")
	r.Handle("/{key}", &ph).Methods("PUT")

	http.ListenAndServe(":8000", r)
}
