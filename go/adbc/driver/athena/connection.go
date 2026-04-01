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

package athena

import (
	"context"
	"fmt"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/internal/driverbase"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	athenaSDK "github.com/aws/aws-sdk-go-v2/service/athena"
)

type connectionImpl struct {
	driverbase.ConnectionImplBase

	athenaClient *athenaSDK.Client
	db           *databaseImpl
}

func (c *connectionImpl) Close() error {
	c.athenaClient = nil
	c.db = nil
	return nil
}

func (c *connectionImpl) NewStatement() (adbc.Statement, error) {
	return &statementImpl{
		conn: c,
	}, nil
}

// CurrentNamespacer interface implementation

func (c *connectionImpl) GetCurrentCatalog() (string, error) {
	return c.db.catalog, nil
}

func (c *connectionImpl) GetCurrentDbSchema() (string, error) {
	return c.db.schema, nil
}

func (c *connectionImpl) SetCurrentCatalog(catalog string) error {
	c.db.catalog = catalog
	return nil
}

func (c *connectionImpl) SetCurrentDbSchema(schema string) error {
	c.db.schema = schema
	return nil
}

// TableTypeLister interface implementation

func (c *connectionImpl) ListTableTypes(_ context.Context) ([]string, error) {
	return []string{"TABLE", "VIEW", "EXTERNAL_TABLE"}, nil
}

// GetTableSchema uses Athena's GetTableMetadata API to return an Arrow schema.
func (c *connectionImpl) GetTableSchema(ctx context.Context, catalog *string, dbSchema *string, tableName string) (*arrow.Schema, error) {
	cat := c.db.catalog
	if catalog != nil && *catalog != "" {
		cat = *catalog
	}
	sch := c.db.schema
	if dbSchema != nil && *dbSchema != "" {
		sch = *dbSchema
	}

	out, err := c.athenaClient.GetTableMetadata(ctx, &athenaSDK.GetTableMetadataInput{
		CatalogName:  &cat,
		DatabaseName: &sch,
		TableName:    &tableName,
	})
	if err != nil {
		return nil, adbc.Error{
			Code: adbc.StatusIO,
			Msg:  fmt.Sprintf("GetTableMetadata failed: %v", err),
		}
	}

	fields := make([]arrow.Field, 0, len(out.TableMetadata.Columns))
	for _, col := range out.TableMetadata.Columns {
		name := ""
		if col.Name != nil {
			name = *col.Name
		}
		dt := athenaTypeToArrow(col.Type)
		fields = append(fields, arrow.Field{Name: name, Type: dt, Nullable: true})
	}

	return arrow.NewSchema(fields, nil), nil
}

// GetObjects uses Athena's ListDatabases / ListTableMetadata APIs.
func (c *connectionImpl) GetObjects(ctx context.Context, depth adbc.ObjectDepth, catalog *string, dbSchema *string, tableName *string, columnName *string, tableType []string) (array.RecordReader, error) {
	cat := c.db.catalog
	if catalog != nil && *catalog != "" {
		cat = *catalog
	}

	var catalogs []string
	if cat != "" {
		catalogs = []string{cat}
	} else {
		listCatInput := &athenaSDK.ListDataCatalogsInput{}
		paginator := athenaSDK.NewListDataCatalogsPaginator(c.athenaClient, listCatInput)
		for paginator.HasMorePages() {
			page, err := paginator.NextPage(ctx)
			if err != nil {
				return nil, adbc.Error{
					Code: adbc.StatusIO,
					Msg:  fmt.Sprintf("ListDataCatalogs failed: %v", err),
				}
			}
			for _, dc := range page.DataCatalogsSummary {
				if dc.CatalogName != nil {
					catalogs = append(catalogs, *dc.CatalogName)
				}
			}
		}
	}

	addCatalogCh := make(chan driverbase.GetObjectsInfo, len(catalogs)+1)
	errCh := make(chan error, 1)

	if depth == adbc.ObjectDepthCatalogs {
		for _, name := range catalogs {
			nameCopy := name
			addCatalogCh <- driverbase.GetObjectsInfo{CatalogName: driverbase.Nullable(nameCopy)}
		}
		close(addCatalogCh)
		close(errCh)
		return driverbase.BuildGetObjectsRecordReader(c.Alloc, addCatalogCh, errCh)
	}

	go func() {
		defer close(addCatalogCh)
		for _, catName := range catalogs {
			info := driverbase.GetObjectsInfo{CatalogName: driverbase.Nullable(catName)}

			schemas, err := c.listSchemas(ctx, catName, dbSchema)
			if err != nil {
				errCh <- err
				return
			}

			dbSchemas := make([]driverbase.DBSchemaInfo, 0, len(schemas))
			for _, schName := range schemas {
				schInfo := driverbase.DBSchemaInfo{DbSchemaName: driverbase.Nullable(schName)}

				if depth >= adbc.ObjectDepthTables {
					tables, err := c.listTables(ctx, catName, schName, tableName)
					if err != nil {
						errCh <- err
						return
					}
					schInfo.DbSchemaTables = tables
				}

				dbSchemas = append(dbSchemas, schInfo)
			}
			info.CatalogDbSchemas = dbSchemas
			addCatalogCh <- info
		}
	}()

	return driverbase.BuildGetObjectsRecordReader(c.Alloc, addCatalogCh, errCh)
}

func (c *connectionImpl) listSchemas(ctx context.Context, catalog string, schemaFilter *string) ([]string, error) {
	input := &athenaSDK.ListDatabasesInput{
		CatalogName: &catalog,
	}
	paginator := athenaSDK.NewListDatabasesPaginator(c.athenaClient, input)
	var schemas []string
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListDatabases failed: %v", err),
			}
		}
		for _, db := range page.DatabaseList {
			if db.Name == nil {
				continue
			}
			if schemaFilter != nil && *schemaFilter != "" && *db.Name != *schemaFilter {
				continue
			}
			schemas = append(schemas, *db.Name)
		}
	}
	return schemas, nil
}

func (c *connectionImpl) listTables(ctx context.Context, catalog, schema string, tableFilter *string) ([]driverbase.TableInfo, error) {
	input := &athenaSDK.ListTableMetadataInput{
		CatalogName:  &catalog,
		DatabaseName: &schema,
	}
	if tableFilter != nil && *tableFilter != "" {
		input.Expression = tableFilter
	}

	paginator := athenaSDK.NewListTableMetadataPaginator(c.athenaClient, input)
	var tables []driverbase.TableInfo
	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			return nil, adbc.Error{
				Code: adbc.StatusIO,
				Msg:  fmt.Sprintf("ListTableMetadata failed: %v", err),
			}
		}
		for _, tbl := range page.TableMetadataList {
			if tbl.Name == nil {
				continue
			}
			tableType := "TABLE"
			if tbl.TableType != nil {
				tableType = *tbl.TableType
			}

			var cols []driverbase.ColumnInfo
			for i, col := range tbl.Columns {
				colName := ""
				if col.Name != nil {
					colName = *col.Name
				}
				typeName := ""
				if col.Type != nil {
					typeName = *col.Type
				}
				pos := int32(i + 1)
				cols = append(cols, driverbase.ColumnInfo{
					ColumnName:      colName,
					OrdinalPosition: &pos,
					XdbcTypeName:    &typeName,
				})
			}

			tables = append(tables, driverbase.TableInfo{
				TableName:    *tbl.Name,
				TableType:    tableType,
				TableColumns: cols,
			})
		}
	}
	return tables, nil
}

// athenaTypeToArrow converts an Athena column type string to an Arrow DataType.
func athenaTypeToArrow(t *string) arrow.DataType {
	if t == nil {
		return arrow.BinaryTypes.String
	}
	switch *t {
	case "varchar", "string", "char":
		return arrow.BinaryTypes.String
	case "bigint":
		return arrow.PrimitiveTypes.Int64
	case "integer", "int":
		return arrow.PrimitiveTypes.Int32
	case "smallint":
		return arrow.PrimitiveTypes.Int16
	case "tinyint":
		return arrow.PrimitiveTypes.Int8
	case "double":
		return arrow.PrimitiveTypes.Float64
	case "float", "real":
		return arrow.PrimitiveTypes.Float32
	case "boolean":
		return arrow.FixedWidthTypes.Boolean
	case "date":
		return arrow.FixedWidthTypes.Date32
	case "timestamp", "timestamp with time zone":
		return arrow.FixedWidthTypes.Timestamp_us
	case "varbinary", "binary":
		return arrow.BinaryTypes.Binary
	default:
		// array, map, row, decimal, json — stringify
		return arrow.BinaryTypes.String
	}
}
