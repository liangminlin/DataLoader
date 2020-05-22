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

`DATABASE_URLS`：以配置类属性的形式存在，类型为字符串列表，列表值如上所示，为数据库连接schema；

`LOG_LEVEL`：日志打印级别，https://docs.python.org/3/library/logging.html#levels

* 取值：logging.INFO, logging.DEBUG, logging.NOTSET,logging.CRITICAL, logging.ERROR, logging.WARNING

`SAVE_LOG_TO_FILE`：True / False，是否保存运行日志到文件，默认False；

`LOG_FILE_LOCATION`：自定日志保存的绝对路径，不填则默认保存在项目文件夹下；



`FLUSH_BUFF_SIZE`：多少条记录做一次IO提交到DB, 默认值: 5w

`ITER_CHUNK_SIZE`：每个批次生成多少条记录, 这个值影响占用内存的大小，默认值: 10w

* Postgres的数据在入库之前存在内存中，如果是Postgres，则可以调整大一点
* MySQL的数据在入库之前会先刷到磁盘的CSV文件中，因此每次加载相比PG要多2倍I/O



在配置当中指定数据库的连接地址，目前支持的数据库类型为Postgres和MySQL：

**MySQL**:    `mysql://{username}:{password}@{hostname}:{port}/{database}`

**Postgres**: `postgresql://{username}:{password}@{hostname}:{port}/{database}`



## 例子

以cpl-service为例来写个小demo：

```shell
$ mkdir demo
$ virtualenv --py=python3 env
$ env/bin/pip install git+https://<token>@github.com/artsalliancemedia/producer2-stress-testing.git@<version>#egg=dataloader
$ touch app.py
## write codes to app.py ......
```

`app.py`：

```python
import logging
from dataloader.helper import incache, free
from dataloader import DataLoader, LoadSession

pvs = LoadSession(__name__)   # 定义Load Session


class Config(object):
    """ 配置类，目前支持如下三个配置项 """
    DATABASE_URL = "mysql://root:123456@k8s-dev-1.aamcn.com.cn:32205/producer_view_service"
    # 多少条记录做一次IO提交到DB，默认 5W
    FLUSH_BUFF_SIZE = 5 * 10000

    # 每个批次生成多少条记录, 这个值影响占用内存的大小，默认10W
    ITER_CHUNK_SIZE = 10 * 10000
    
    LOG_LEVEL = logging.INFO
    SAVE_LOG_TO_FILE = True
    # LOG_FILE_LOCATION = "/tmp"


@pvs.regist_for("producer_view_service")     # 声明这个session属于哪个DB
def load_cpl_service_data():
    # 自动生成的代码，按约定命名直接使用即可，
    # from target.<dbname> import iter_<tbname>
    from target.producer_view_service import (
        iter_cpl_data, iter_complex_data, iter_cpl_complex_mapping, CplData
    )

    # 使用retain_pkey声明数据生成后主键字段保留待用
    for cpl in iter_cpl_data(100, retain_pkey=True):
        yield cpl
        
    for cplx in iter_complex_data(100):
        yield cplx
        
        # 使用incache来指定数据从指定表的指定字段获取
        for mp in iter_cpl_complex_mapping(
            2, cpl_uuid=incache(CplData, "uuid"), complex_uuid=cplx.uuid
        ):
            yield mp
     
    # 不再使用之后释放掉保留的数据
    free(CplData)

app = DataLoader(__name__, Config)    # 实例化应用
app.register_session(pvs)             # 注册session


if __name__ == "__main__":
    app.run()
```

运行（`examples/src`）：

```shell
$ env/bin/python app.py
```
