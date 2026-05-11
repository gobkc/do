### examples

````go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"

	"github.com/gobkc/do/exporter/excel"
	"github.com/gobkc/do/exporter"
)

// UserExport 定义导出的格式约束和映射规则
type UserExport struct {
	ID        int    `db:"id" xlsx:"-"`          // 仅用于中间查询，不导出到 Excel
	Username  string `db:"username" xlsx:"用户名称"`
	RoleID    int    `db:"role_id" xlsx:"-"`     // 凭据字段，不直接展示
	RoleName  string `db:"-" xlsx:"所属角色"`        // 数据库没有此列，由 mapper 补全
	CreatedAt string `db:"created_at" xlsx:"注册时间"`
}

func main() {
	// 连接 PostgreSQL
	db, err := sql.Open("postgres", "postgres://user:pass@localhost:5432/mydb?sslmode=disable")
	if err != nil {
		log.Fatal(err)
	}

	r := gin.Default()

	// 导出路由
	r.GET("/api/export/users", exportUsersHandler(db))

	r.Run(":8080")
}

func exportUsersHandler(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		exp := excelizev2.New()

		logicFn := func(exp exporter.DocumentExporter) error {
			opts := &sql.TxOptions{Isolation: sql.LevelRepeatableRead, ReadOnly: true}
			tx, err := db.BeginTx(context.Background(), opts)
			if err != nil {
				return err
			}
			defer tx.Rollback()

			rows, err := tx.QueryContext(context.Background(), "SELECT id, username, role_id, created_at FROM users")
			if err != nil {
				return err
			}

			// 关键修改：在这里传入匿名函数处理额外逻辑
			return exporter.StreamFromRows[UserExport](rows, exp, func(u *UserExport) error {
				// 模拟调用其他服务或查询其他表
				u.RoleName = mockGetRoleName(u.RoleID)
				
				// 你甚至可以在这里做格式化处理
				u.Username = "Processed: " + u.Username
				return nil
			})
		}

		handler := exporter.BuildStreamHandler("users_export.xlsx", exp, logicFn)
		handler.ServeHTTP(c.Writer, c.Request)
	}
}
````
