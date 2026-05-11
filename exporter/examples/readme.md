### examples

````go
package main

import (
	"context"
	"database/sql"
	"log"

	"github.com/gin-gonic/gin"
	_ "github.com/lib/pq"

	"your_project/pkg/excelizev2"
	"your_project/pkg/exporter"
)

// UserExport 定义导出的格式约束和映射规则
type UserExport struct {
	ID        int    `db:"id" xlsx:"用户ID"`
	Name      string `db:"username" xlsx:"用户名称"`
	Status    int    `db:"status" xlsx:"状态"`
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

// exportUsersHandler 包装 Gin 处理器
func exportUsersHandler(db *sql.DB) gin.HandlerFunc {
	return func(c *gin.Context) {
		
		// 注入具体实现的 Exporter（这里用 excelizev2）
		exp := excelizev2.New()
		
		// 构造匿名逻辑函数
		logicFn := func(exp exporter.DocumentExporter) error {
			// 1. 开启 REPEATABLE READ 只读事务 (确保导出过程中数据的一致快照)
			// 注意：PostgreSQL 的长只读事务会阻碍 VACUUM 清理死元组，建议通过业务侧限制最大导出时间或条数
			opts := &sql.TxOptions{
				Isolation: sql.LevelRepeatableRead,
				ReadOnly:  true,
			}
			tx, err := db.BeginTx(context.Background(), opts)
			if err != nil {
				return err
			}
			defer tx.Rollback() // 只读事务，直接回滚即可

			// 2. 执行查询，拿到 Rows() (不使用切片装载，保证内存不炸)
			query := `SELECT id, username, status, created_at FROM users WHERE status = $1`
			rows, err := tx.QueryContext(context.Background(), query, 1)
			if err != nil {
				return err
			}
			// rows.Close() 已在 StreamFromRows 内部保证被调用

			// 3. 将 Rows 桥接给 Exporter，以 UserExport 为模板格式化
			return exporter.StreamFromRows[UserExport](rows, exp)
		}

		// 通过 exporter.BuildStreamHandler 生成原生的 http.HandlerFunc
		nativeHandler := exporter.BuildStreamHandler("users_export.xlsx", exp, logicFn)
		
		// Gin 对原生 HTTP Handler 的无缝转接
		nativeHandler.ServeHTTP(c.Writer, c.Request)
	}
}
````
