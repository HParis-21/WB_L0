package main

import (
	"database/sql"
	"encoding/json"
	nat "github.com/nats-io/stan.go"
	"html/template"
	"log"
	"net/http"
	"os/user"

	_ "github.com/lib/pq"
)

const (
	durName = "chan_orders"
)

var (
	db    *sql.DB
	sc    nat.Conn
	cache map[string]Order
	tmpl  *template.Template
)

func init() {
	cache = make(map[string]Order)

	_, err := user.Current()
	db, err = sql.Open("postgres",
		"postgres://user_wb:pass@localhost:5432/database_wb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	err = db.Ping()
	if err != nil {
		log.Fatal(err)
	}

	sc, err = nat.Connect("test-cluster", "Lana")
	if err != nil {
		log.Fatal(err)
	}

	restoreCache()
	tmpl, err = template.ParseFiles("index.html")
	if err != nil {
		log.Fatal(err)
	}
}

func main() {
	defer db.Close()

	ins, err := db.Prepare("INSERT INTO orders (uid, data) VALUES ($1, $2);")
	if err != nil {
		log.Fatal(err, "1")
	}
	defer ins.Close()

	upd, err := db.Prepare("UPDATE orders SET data = $2 WHERE uid = $1;")
	if err != nil {
		log.Fatal(err, "2")
	}
	defer upd.Close()

	defer sc.Close()
	sub, err := sc.Subscribe("orders", func(m *nat.Msg) {
		handleMessage(m, ins, upd)
	}, nat.DurableName(durName))
	defer sub.Unsubscribe()

	log.Fatal(http.ListenAndServe(":8000", http.HandlerFunc(handler)))
}

func handler(w http.ResponseWriter, r *http.Request) {
	uid := r.URL.Query().Get("uid")
	order := cache[uid]

	err := tmpl.Execute(w, order)
	if err != nil {
		log.Fatal(err)
	}
}

func restoreCache() {
	sel, err := db.Prepare("SELECT * FROM orders;")
	if err != nil {
		log.Fatal(err, "3")
	}
	defer sel.Close()

	rows, err := sel.Query()
	if err != nil {
		log.Fatal(err, "4")
	}
	var id int
	var uid, raw []byte
	var newOrder Order
	for rows.Next() {
		if err := rows.Scan(&id, &uid, &raw); err != nil {
			log.Fatal(err)
		}
		if err := json.Unmarshal(raw, &newOrder); err != nil {
			log.Fatal(err)
		}
		cache[newOrder.OrderUID] = newOrder
	}
}

func handleMessage(m *nat.Msg, ins, upd *sql.Stmt) {
	var newOrder Order

	if err := json.Unmarshal(m.Data, &newOrder); err != nil {
		log.Println("Received invalid JSON data")
		return
	}

	_, exists := cache[newOrder.OrderUID]
	cache[newOrder.OrderUID] = newOrder
	log.Printf("Received a new order: %s\n", newOrder.OrderUID)

	var op *sql.Stmt
	if exists {
		op = upd
	} else {
		op = ins
	}
	if _, err := op.Exec(newOrder.OrderUID, string(m.Data)); err != nil {
		log.Fatal(err)
	}
}
