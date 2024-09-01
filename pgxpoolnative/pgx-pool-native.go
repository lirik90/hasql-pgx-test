package pgxpoolnative

import (
	"context"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
)

func testConn(pool *pgxpool.Pool) error {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	err := pool.Ping(ctx)
	if err != nil {
		return fmt.Errorf("ping: %w", err)
	}

	listActive(pool)

	_, err = pool.Exec(context.TODO(), "SELECT 1;")
	if err != nil {
		return fmt.Errorf("exec: %w", err)
	}
	return nil
}

func listActive(pool *pgxpool.Pool) int {
	stats2 := pool.Stat()
	fmt.Println("Stats2 - Idle:", stats2.IdleConns(), "Acquied:", stats2.AcquiredConns(), "Constructive:", stats2.ConstructingConns())
	return int(stats2.TotalConns())
}

func testPgxPool(n int, pool *pgxpool.Pool, ch chan int, waitDur time.Duration) {
	defer func() {
		ch <- 0
	}()
	time.Sleep(300*time.Millisecond)

	_, err := pool.Exec(context.TODO(), "SELECT 1;")
	if err != nil {
		fmt.Printf("exec: %s\n", err.Error())
		return
	}
	count := listActive(pool)

	fmt.Println(n, "- begin wait 2. Cons:", count)
	time.Sleep(waitDur)

	fmt.Println(n, "- begin tx")
	tx, err := pool.Begin(context.TODO())
	if err != nil {
		fmt.Printf("begin tx: %s\n", err.Error())
		return
	}

	time.Sleep(waitDur)

	fmt.Println(n, "- begin query 2")
	_, err = tx.Exec(context.TODO(), "SELECT 1;")
	if err != nil {
		fmt.Printf("exec in tx: %s\n", err.Error())
		return
	}
	count = listActive(pool)

	fmt.Println(n, "- begin wait 4 Cons: ", count)
	time.Sleep(waitDur)

	fmt.Println(n, "- commit tx")
	if err = tx.Commit(context.TODO()); err != nil {
		fmt.Printf("commit tx: %s\n", err.Error())
		return
	}
	listActive(pool)
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
	
	pool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		panic(fmt.Sprintf("new pool: %s", err.Error()))
	}

	listActive(pool)

	if err = testConn(pool); err != nil {
		panic(fmt.Sprintf("test conn: %s", err.Error()))
	}

	count := listActive(pool)

	ch := make(chan int)
	fmt.Println("Begin! count: ", count)

	for i := range parallels {
		go testPgxPool(i, pool, ch, waitDur)
	}

	fmt.Println("Started!")
	for _ = range parallels {
		<- ch
	}

	count = listActive(pool)
	fmt.Println("Ended! Cons: ", count)

	pool.Close()
	fmt.Println("Done!")
}

