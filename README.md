## DataLoader

一个自动反射数据表结构以生成模拟数据并加载到DB的测试辅助框架；设计原则：

1. 简单可配置，约定优于配置；
2. 自动反射表结构和生成模拟数据；
3. 开发者只需开发LoadSession定义表关系即可。



* 运行DEMO（CPL-SERVICE)：`make init-demo && make run-demo`

##  一、配置

至少需要在配置当中指定数据库的连接地址，目前支持的数据库类型为Postgres和MySQL：

**MySQL**: `mysql://{username}:{password}@{hostname}:{port}/{database}`

**Postgres**: `postgresql://{username}:{password}@{hostname}:{port}/{database}`



`DATABASE_URL`：以配置类属性的形式存在，类型为字符串，值如上所示，为数据库连接schema；

`DATABASE_URLS`：以配置类属性的形式存在，类型为字符串列表，列表值如上所示，为数据库连接schema。



## 二、例子

假定我们拥有一个名为`dltest`的数据库，其中有如下两张表

`books`

| bid  | author | isbn              | name   | created             |
| ---- | ------ | ----------------- | ------ | ------------------- |
| 1    | Abby   | 978-3-16-148410-0 | Book X | 2020/01/01 00:12:12 |
| 2    | Beata  | 978-3-16-148410-1 | Book Y | 2020/01/01 00:12:12 |

`order`

| oid  | buyer_id | bid  | total | created             |
| ---- | -------- | ---- | ----- | ------------------- |
| 1    | 1        | 2    | 5     | 2020/05/01 00:12:12 |

由于表之间存在关系`order.bid <=> books.bid`，所以在构造测试数据的时候就需要保证存在这个关系



### 使用`dataloader`快速构造测试数据：

建立项目，名为loader，结构如下：

```
loader/
    —— __init__.py        # 模块定义与入口
    —— config.py          # 配置文件
    —— app.py             # 应用定义
    —— load_session.py    # 数据量及数据关系定义
```

`config.py`

```python
class Config(object):
    # 你可以指定单个数据库以进行操作
    DATABASE_URL = "postgresql://postgres:123456@localhost:5432/dltest"
    
    # 也可以同时指定多个数据库连接以进行同时操作，类似这样：
    DATABASE_URLS = [
        "mysql://postgres:123456@anotherdb:3306/dltest",
        "postgresql://postgres:123456@localhost:5432/dltest",
    ]
```

`load_session.py`

```python
from dataloader import LoadSession

# 代码自动生成，按约定使用即可：
# from target.<database> import iter_<table1>, iter_<table2>
from target.dltest import iter_books, iter_order

bs = LoadSession(__name__)


@bs.regist_for("dltest")            # data base name
def load_books():
    for book in iter_books(
        5,                          # 最少为1，生成的记录条数：50
        auto_incr_cols=['bid'],     # 声明自增字段, 框架将从DB的最大值开始自增
        name = xxx()
    ):
        yield book

        for order in iter_order(
            20,                         # 最少为1，生成的记录条数：10 x 50
            auto_incr_cols=['oid'],
            bid=book.bid,               # 覆盖默认生成的数据,以实现表数据关系关联
            total = random.choice([10,20,30])
        ):
            yield order
```

`app.py`

```python
from dataloader import DataLoader

from . import Config
from .load_session import bs

loader = DataLoader(__name__, Config)

loader.register_session(bs)

if __name__ == "__main__":
    loader.load()
```

执行：

```python
$ python app.py
```



## 三、当前问题

目前遇到的问题点：

1. 非自增主键/唯一约束的冲突问题
2. 随机生成的数据不是很快，需优化