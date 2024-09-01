package pgxpoolhasql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/jackc/pgx/v5/stdlib"

	"hasql-pgx/pgxhasql"

	"golang.yandex/hasql"
	"golang.yandex/hasql/checkers"
)
var pool *pgxpool.Pool

func listActive(db *sql.DB) int {
	stats := db.Stats()
	fmt.Println("Stats - Idle:", stats.Idle, "InUse:", stats.InUse)
	stats2 := pool.Stat()
	fmt.Println("Stats2 - Idle:", stats2.IdleConns(), "Acquied:", stats2.AcquiredConns(), "Constructive:", stats2.ConstructingConns())
	return int(stats2.TotalConns())
}

func testPgxPool(n int, cl *hasql.Cluster, ch chan int, waitDur time.Duration) {
	defer func() {
		ch <- 0
	}()
	time.Sleep(300*time.Millisecond)

	node := cl.Primary()
	if node == nil {
		fmt.Println("Fail test ", n, " err: ", cl.Err())
		return
	}
	fmt.Println(n, "- begin wait")
	time.Sleep(500*time.Millisecond)

	_, err := node.DB().Exec("SELECT 1;")
	if err != nil {
		fmt.Printf("exec: %s\n", err.Error())
		return
	}
	count := listActive(node.DB())

	fmt.Println(n, "- begin wait 2. Cons:", count)
	time.Sleep(waitDur)

	fmt.Println(n, "- begin tx")
	tx, err := node.DB().Begin()
	if err != nil {
		fmt.Printf("begin tx: %s\n", err.Error())
		return
	}

	time.Sleep(waitDur)

	fmt.Println(n, "- begin query 2")
	_, err = tx.Exec("SELECT 1;")
	if err != nil {
		fmt.Printf("exec in tx: %s\n", err.Error())
		return
	}
	count = listActive(node.DB())

	fmt.Println(n, "- begin wait 4 Cons: ", count)
	time.Sleep(waitDur)

	fmt.Println(n, "- commit tx")
	if err = tx.Commit(); err != nil {
		fmt.Printf("commit tx: %s\n", err.Error())
		return
	}
	listActive(node.DB())
}

func Run(parallels int, waitDur time.Duration) {
	connStr := "postgresql://mdbuser:H8Zmbrhun9ImM8wySy2zttlpkblyTuba0chnwzPBRDynyHsW@localhost:15433/test"
	config, err := pgxpool.ParseConfig(connStr)
	if err != nil {
		panic(fmt.Sprintf("parse config: %s", err.Error()))
	}
	config.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		fmt.Printf("After connect\n")
		return nil
	}
	config.MaxConns = 10
	
	pool, err = pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic(fmt.Sprintf("new pool: %s", err.Error()))
	}

	dbFoo := stdlib.OpenDBFromPool(pool)
	dbFoo.SetMaxOpenConns(10)
	defer dbFoo.Close()

	listActive(dbFoo)

	cl, err := hasql.NewCluster(
		[]hasql.Node{hasql.NewNode("foo", dbFoo)},
		checkers.PostgreSQL,
	)
	if err != nil {
		panic(fmt.Sprintf("new cluster: %s", err.Error()))
	}

	if err = pgxhasql.TestConn(cl); err != nil {
		panic(fmt.Sprintf("test conn: %s", err.Error()))
	}

	count := listActive(cl.Alive().DB())

	ch := make(chan int)
	fmt.Println("Begin! count: ", count)

	for i := range parallels {
		go testPgxPool(i, cl, ch, waitDur)
	}

	fmt.Println("Started!")
	for _ = range parallels {
		<- ch
	}

	node, _ := cl.WaitForAlive(context.TODO())
	count = listActive(node.DB())
	fmt.Println("Ended! Cons: ", count)

	cl.Close()
	fmt.Println("Done!")
}

