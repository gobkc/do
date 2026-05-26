# do

用 Go 语言泛型实现的若干实用功能和组件。

## 功能特性

### 核心工具函数

| 函数 | 说明 |
|------|------|
| `OneOf` | 根据条件返回两个值中的一个 |
| `OneOr` | 返回第一个非零值 |
| `ErrorOr` | 如果存在错误返回错误信息，否则返回指定值 |
| `AnyTrue` | 任意一个为 `true` 则返回 `true` |
| `AllTrue` | 所有值都为 `true` 才返回 `true` |
| `InList` | 检查元素是否在列表中 |
| `Unique` | 去重（支持正则过滤） |
| `Diff` | 计算两个集合的差集（新增、删除、相同） |
| `RegexpCheck` | 正则表达式匹配 |
| `RegexpConvertSnake` | 驼峰转下划线命名 |
| `GetFieldList/GetFieldMap/GetFieldMaps` | 通用字段提取工具 |

### 并发与异步

| 函数 | 说明 |
|------|------|
| `BatchCall` | 批量执行函数，支持分片 |
| `BatchCallPagination` | 分页拉取数据 |
| `RetryCall` | 重试调用，支持指数退避和随机抖动 |

### 分布式任务调度

| 组件 | 说明 |
|------|------|
| `Lock` | 分布式锁，支持单次执行或周期性执行 |
| `LeaderPoller` | 领导者轮询，用于多实例选举和任务分发 |
| `Task` | 分布式任务调度器，支持依赖传递、任务分片、自动重试 |

### 数据导出

| 组件 | 说明 |
|------|------|
| `DBMapper` | 从数据库结果集自动映射到任意结构体 |
| `Excel` | 导出 Excel 文件（支持 `xlsx` 标签映射） |

## 快速开始

### 安装

```bash
go get -u github.com/gobkc/do
```

### 使用示例

```go
package main

import (
	"fmt"
	"github.com/gobkc/do"
)

func main() {
	// 核心工具函数
	fmt.Println(do.OneOf(1==1, "yes", "no"))          // yes
	fmt.Println(do.AnyTrue(true, false, false))        // true
	fmt.Println(do.Unique([]int{1,2,2,3}))              // [1 2 3]
	fmt.Println(do.RegexpConvertSnake("camelCase"))   // camel_case

	// 并发工具
	// 批量调用
	// 带重试的 API 调用

	// 分布式任务
	// 使用 Redis 作为锁存储
	// 配置任务依赖链
}
```

## 单元测试

运行所有测试：

```bash
go test -v ./...
```

运行特定测试：

```bash
go test -v -run Poller
```

## 许可证

Apache License 2.0

---

本项目欢迎贡献，欢迎提交 Pull Request。
