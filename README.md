## DataLoader

一个自动反射数据表结构以生成模拟数据并加载到DB的测试辅助框架；设计原则：

1. 简单可配置，约定优于配置；
2. 自动反射表结构和生成模拟数据；
3. 开发者只需开发LoadSession定义表关系即可。

## DEMO

代码库自带了两个Demo，分别对应MySQL和Postgres，请在Clone代码之后通过如下命令运行：

```shell
$ make init-demo
$ make run-mysql-demo
$ make run-postgres-demo
```

## 安装

这是个Private Repo，请自行替换`token` 和 `version`；version版本[请看这里](https://github.com/artsalliancemedia/producer2-stress-testing/releases)。

```shell
$ pip3 install git+https://<token>@github.com/artsalliancemedia/producer2-stress-testing.git@<version>#egg=dataloader
```

##  配置

目前有如下配置项：

`DATABASE_URL`：以配置类属性的形式存在，类型为字符串，值如上所示，为数据库连接schema；

`DATABASE_URLS`：以配置类属性的形式存在，类型为字符串列表，列表值如上所示，为数据库连接schema。



`FLUSH_BUFF_SIZE`：多少条记录做一次IO提交到DB, 默认值: 5w

`ITER_CHUNK_SIZE`：每个批次生成多少条记录, 这个值影响占用内存的大小，默认值: 10w

* Postgres的数据在入库之前存在内存中，如果是Postgres，则可以调整大一点
* MySQL的数据在入库之前会先刷到磁盘的CSV文件中，因此每次加载相比PG要多2倍I/O



在配置当中指定数据库的连接地址，目前支持的数据库类型为Postgres和MySQL：

**MySQL**:    `mysql://{username}:{password}@{hostname}:{port}/{database}`

**Postgres**: `postgresql://{username}:{password}@{hostname}:{port}/{database}`



## 例子

以cpl-service为例来写个小demo：`app.py`

```python
from dataloader import fast_rand
from dataloader import DataLoader, LoadSession

cs = LoadSession(__name__)   # 定义Load Session


class Config(object):
    """ 配置类，目前支持如下三个配置项 """
    DATABASE_URL = "postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/cpl_service"
    # 多少条记录做一次IO提交到DB，默认 5W
    FLUSH_BUFF_SIZE = 5 * 10000

    # 每个批次生成多少条记录, 这个值影响占用内存的大小，默认10W
    ITER_CHUNK_SIZE = 10 * 10000


@cs.regist_for("cpl_service")     # 声明这个session属于哪个DB
def load_cpl_service_data():
    # 自动生成的代码，按约定命名直接使用即可，
    # from target.<dbname> import iter_<tbname>
    from target.cpl_service import (
        iter_cpl, iter_complex_lms_device, iter_cpl_location
    )

    for idx, cplx in iter_complex_lms_device(10):   # 指定固定量的模拟数据生成量
        yield cplx       # 生成数据

        # 随机离散数据生成量，注意这里的数量乘以祖先级for的数量才是其生成量
        for idx, cpl in iter_cpl( fast_rand.randint(1, 10) ): 
            yield cpl       # 生成数据

            for idx, cpl_loc in iter_cpl_location(
                fast_rand.randint(1, 10),
                # auto_incr_cols=['自动增长的列名'],  # 指定自动增长的列以续增ID
                
                # 覆盖默认列的生成策略来关联数据关系:
                complex_uuid=cplx.complex_uuid,
                device_uuid=fast_rand.choice([
                    cplx.device_uuid, fast_rand.randuuid()
                ]),
                cpl_uuid=cpl.uuid
            ):
                yield cpl_loc       # 生成数据


app = DataLoader(__name__, Config)    # 实例化应用
app.register_session( cs )            # 注册session


if __name__ == "__main__":
    app.run()
```

运行（`examples/src`）：

```shell
$ python3 app.py
```

## Issues

1. 随机生成的数据不是很快，目前这里是耗时的瓶颈所在，存在可优化的空间；