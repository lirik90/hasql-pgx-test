package main

import (
	"fmt"
	"hasql-pgx/pgxhasql"
	"hasql-pgx/pgxpoolhasql"
	"hasql-pgx/pgxpoolhasqlraw"
	"hasql-pgx/pgxpoolnative"
	"time"
)

func main() {
	parallels := 255
	waitDur := 100*time.Millisecond

	fmt.Printf("BEGIN pgxhasql\n")
	start := time.Now()
	pgxhasql.Run(parallels, waitDur)
	pgxhasqlElapsed := time.Now().Sub(start)

	fmt.Printf("\n\n----------------------------------\n")
	fmt.Printf("BEGIN pgxpoolhasql\n")
	start = time.Now()
	pgxpoolhasql.Run(parallels, waitDur)
	pgxpoolhasqlElapsed := time.Now().Sub(start)

	fmt.Printf("\n\n----------------------------------\n")
	fmt.Printf("BEGIN pgxpoolhasqlraw\n")
	start = time.Now()
	pgxpoolhasqlraw.Run(parallels, waitDur)
	pgxpoolhasqlrawElapsed := time.Now().Sub(start)

	fmt.Printf("\n\n----------------------------------\n")
	fmt.Printf("BEGIN pgxpoolnative\n")
	start = time.Now()
	pgxpoolnative.Run(parallels, waitDur)
	pgxpoolnativeElapsed := time.Now().Sub(start)

	fmt.Printf("\n\n----------------------------------\n")
	fmt.Printf("END pgxhasql elapsed: %d secs: %f\n", pgxhasqlElapsed, (float64(pgxhasqlElapsed) / float64(time.Second)))
	fmt.Printf("END pgxpoolhasql elapsed: %d secs: %f\n", pgxpoolhasqlElapsed, (float64(pgxpoolhasqlElapsed) / float64(time.Second)))
	fmt.Printf("END pgxpoolhasqlraw elapsed: %d secs: %f\n", pgxpoolhasqlrawElapsed, (float64(pgxpoolhasqlrawElapsed) / float64(time.Second)))
	fmt.Printf("END pgxpoolnative elapsed: %d secs: %f\n", pgxpoolnativeElapsed, (float64(pgxpoolnativeElapsed) / float64(time.Second)))
}

