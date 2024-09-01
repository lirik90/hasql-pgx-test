package pgxpoolhasqlraw

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/pkg/errors"

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

	conn, err := node.DB().Conn(context.TODO())
	defer conn.Close()
	if err != nil {
		fmt.Println("Get conn ", n, " err: ", err)
		return
	}

	err = conn.Raw(func(rawConn any) error {
		pgxConn, ok := rawConn.(*stdlib.Conn)
		if !ok {
			return errors.New("Fail convert to raw conn")
		}

		_, err = pgxConn.Conn().Exec(context.TODO(), "SELECT 1;")
		return errors.Wrap(err, "exec")
	})
	if err != nil {
		fmt.Println("raw ", n, "err: ", err)
		return
	}
	conn.Close()

	count := listActive(node.DB())

	fmt.Println(n, "- begin wait 2. Cons:", count)
	time.Sleep(waitDur)

	fmt.Println(n, "- begin tx")

	conn, err = node.DB().Conn(context.TODO())
	defer conn.Close()
	if err != nil {
		fmt.Println("Get conn2 ", n, " err: ", err)
		return
	}

	err = conn.Raw(func(rawConn any) error {
		pgxConn, ok := rawConn.(*stdlib.Conn)
		if !ok {
			return errors.New("Fail convert to raw conn")
		}

		tx, err := pgxConn.Conn().Begin(context.TODO())
		if err != nil {
			return errors.Wrap(err, "begin tx")
		}

		time.Sleep(waitDur)

		fmt.Println(n, "- begin query 2")
		_, err = tx.Exec(context.TODO(), "SELECT 1;")
		if err != nil {
			return errors.Wrap(err, "exec")
		}
		count = listActive(node.DB())

		fmt.Println(n, "- begin wait 4 Cons: ", count)
		time.Sleep(waitDur)

		fmt.Println(n, "- commit tx")
		if err = tx.Commit(context.TODO()); err != nil {
			return errors.Wrap(err, "commit")
		}
		return nil
	})
	if err != nil {
		fmt.Println("rawTx ", n, "err: ", err)
		return
	}
	conn.Close()
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

