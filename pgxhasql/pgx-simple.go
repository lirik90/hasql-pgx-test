package pgxhasql

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/jackc/pgx/v5/stdlib"

	"golang.yandex/hasql"
	"golang.yandex/hasql/checkers"
)

func TestConn(cl *hasql.Cluster) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	node, err := cl.WaitForPrimaryPreferred(ctx)
	if err != nil {
		fmt.Printf("get node: %s\n", cl.Err().Error())

		return fmt.Errorf("get node: %w", err)
	}

	fmt.Println("Node address", node.Addr())

	ctx, cancel = context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err = node.DB().PingContext(ctx); err != nil {
		return fmt.Errorf("ping node: %w", err)
	}

	listActive(node.DB())

	_, err = node.DB().Exec("SELECT 1;")
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func listActive(db *sql.DB) int {
	stats := db.Stats()
	fmt.Println("Stats - Idle:", stats.Idle, "InUse:", stats.InUse)
	return stats.OpenConnections
}

func testSimple(n int, cl *hasql.Cluster, ch chan int, waitDur time.Duration) {
	defer func() {
		ch <- 0
	}()
	time.Sleep(300*time.Millisecond)

	node := cl.Primary()
	if node == nil {
		fmt.Println("Fail test ", n, " err: ", cl.Err())
		return
	}

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
	dbFoo, err := sql.Open("pgx", "postgresql://mdbuser:H8Zmbrhun9ImM8wySy2zttlpkblyTuba0chnwzPBRDynyHsW@localhost:15433/test")
	if err != nil {
		panic(fmt.Sprintf("open: %s", err.Error()))
	}
	defer dbFoo.Close()
	dbFoo.SetMaxOpenConns(10)

	listActive(dbFoo)

	cl, err := hasql.NewCluster(
		[]hasql.Node{hasql.NewNode("foo", dbFoo)},
		checkers.PostgreSQL,
	)
	if err != nil {
		panic(fmt.Sprintf("new cluster: %s", err.Error()))
	}

	if err = TestConn(cl); err != nil {
		panic(fmt.Sprintf("test conn: %s", err.Error()))
	}

	count := listActive(cl.Alive().DB())

	ch := make(chan int)
	fmt.Println("Begin! count: ", count)

	for i := range parallels {
		go testSimple(i, cl, ch, waitDur)
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

