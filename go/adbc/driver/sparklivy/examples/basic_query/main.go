// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

// Basic query example for ADBC Spark Livy driver with AWS EMR Serverless
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/sparklivy"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

func main() {
	livyURI := os.Getenv("LIVY_ENDPOINT")
	if livyURI == "" {
		livyURI = "http://localhost:8998"
	}

	isLocal := livyURI == "http://localhost:8998"
	authType := "basic"
	if !isLocal {
		authType = "aws_sigv4"
	}

	opts := map[string]string{
		"adbc.sparklivy.uri":          livyURI,
		"adbc.sparklivy.auth_type":    authType,
		"adbc.sparklivy.session_kind": "sql",
		"adbc.connection.db_schema":   "default",
	}

	if !isLocal {
		opts["adbc.sparklivy.aws_region"] = os.Getenv("AWS_REGION")
		opts["adbc.sparklivy.emr-serverless.session.executionRoleArn"] = os.Getenv("EMR_ROLE_ARN")
	}

	queries := []string{
		"SHOW DATABASES",
		"SHOW TABLES",
		"SELECT 1 as test_col",
	}

	drv := sparklivy.NewDriver(memory.DefaultAllocator)
	db, err := drv.NewDatabase(opts)
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()

	ctx := context.Background()
	cnxn, err := db.Open(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer cnxn.Close()

	stmt, err := cnxn.NewStatement()
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()

	for i, query := range queries {
		if err := executeQuery(ctx, stmt, query, i+1); err != nil {
			log.Printf("Query %d failed: %v\n", i+1, err)
		}
	}
}

func executeQuery(ctx context.Context, stmt adbc.Statement, query string, queryNum int) error {
	fmt.Printf("\n[%d] %s\n", queryNum, query)

	if err := stmt.SetSqlQuery(query); err != nil {
		return err
	}

	reader, _, err := stmt.ExecuteQuery(ctx)
	if err != nil {
		return err
	}
	defer reader.Release()

	count := 0
	for reader.Next() {
		record := reader.Record()
		if count == 0 {
			for i := 0; i < int(record.NumCols()); i++ {
				fmt.Printf("%-20s ", record.Schema().Field(i).Name)
			}
			fmt.Println()
		}
		for i := 0; i < int(record.NumCols()); i++ {
			fmt.Printf("%-20v ", record.Column(i))
		}
		fmt.Println()
		count++
	}

	if reader.Err() != nil {
		return reader.Err()
	}

	fmt.Printf("(%d rows)\n", count)
	return nil
}
