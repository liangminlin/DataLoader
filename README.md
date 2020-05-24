## DataLoader

一个自动反射数据表结构、自动生成模拟数据并加载到DB的测试辅助框架；设计原则：

1. 简单可配置，约定优于配置；
2. 自动反射表结构和生成模拟数据；
3. 开发者只需开发LoadSession定义表关系即可。

## 安装

* **虚拟环境方式：**

    请自行替换 `<version>`；最新version版本[请看这里](https://github.com/i36lib/DataLoader/releases)

    ```shell
    $ pip3 install git+https://github.com/i36lib/DataLoader.git@<version>#egg=dataloader
    ```

* **docker 方式：**
    基础镜像：`dataloader:python3.6`

    在你的项目目录下：编写你的代码`*.py`（假定入口文件为app.py），编写`Dockerfile`：

    ```dockerfile
    FROM dataloader:python3.6
    
    WORKDIR /var/app
    
    COPY . .
    
    RUN python app.py
    ```

    构建：

    ```shell
    $ docker build -t xxx:xxx .
    ```

    运行：

    ```shell
    $ docker run xxx:xxx
    ```

## DEMO

代码库自带了几个Demo，分别对应MySQL和Postgres，请在Clone代码之后通过如下命令运行：

```shell
$ make init-demo
$ make run-demo         # base on docker
$ make run-mysql-demo
$ make run-postgres-demo
```

* 其中`$ make run-demo`是将dataloader先打包安装到docker镜像`dataloader:python3.6`，再基于该镜像和所写的`demo.py`构建新的demo docker镜像，然后运行该镜像。

## 说明

* **设计约定：**你总是能够通过`target.<database>`引入驼峰形式的表模型类，和制造数据的`iter_<table_name>`数据生成器；

    * 例如对于一个表`cpl_location`，我们将自动获得`CplLocation`和`iter_cpl_location`，前者用于配合`retaining/incache`做数据缓存，后者用于定义数据的生成策略
    * `iter_<tbname>`的方法签名：
        `iter_<table_name>(count, retaining=False, auto_incr_cols=[])`
        * count：整数；大于1的正数，定义该表的数据生成量；
        * retaining：布尔类型，默认False；指明是否对于已生成的数据保留主键字段的值以备后续使用；
        * auto_incr_cols：列表，默认为空；如果一个列是整型且自动增长的，那么应当将其指定到auto_incr_cols当中，这样反射器将自动从DB的最大值开始递增，以避免冲突；

* **几个重要的方法**

    * `helper.incache`：

        ​		 配合`retaining`参数使用，在声明`retaining=True`之后，可在后续使用`incache`方法来引用已生成的模型的主键字段：`incache(TableModel, column_string)`。

    * `helper.free`：

        ​		 配合`retaining`参数使用，用于及时释放不再需要通过`incache`策略引用的数据：`free(TableModel)`。

    * `helper.fastuuid`：

        该方法通过在首次生成一个随机UUID，然后根据`iter_<tbname>`的生成数量从0依次递增替换UUID尾数，来实现生成快速独立的UUID，以期获得更好的性能；**注意：**为了获得更快的速度，对于所有UUID字段，都应该在`iter_<tbname>`中显式地使用`<uuid_column>=fastuuid()`来覆盖默认的UUID生成策略。

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
$ env/bin/pip install git+https://github.com/i36lib/DataLoader.git@<version>#egg=dataloader
$ touch app.py
## write codes to app.py ......
```

`app.py`：

```python
import logging
from dataloader import DataLoader, LoadSession
from dataloader.helper import incache, free, fastuuid

pvs = LoadSession(__name__)   # 定义Load Session


class Config(object):
    """ 配置类，目前支持如下几个配置项 """
    # 另一个可选的配置是DATABASE_URLS，支持同时操作多个DB，详见examples/demo.py
    # DATABASE_URLS = ["mysql://xxxx", "postgres://xxx"]
    DATABASE_URL = "mysql://root:123456@k8s-dev-1-localhost:32205/producer_view_service"
    
    # 多少条记录做一次IO提交到DB，默认 5W
    FLUSH_BUFF_SIZE = 10 * 10000

    # 每个批次生成多少条记录, 这个值影响占用内存的大小，默认10W
    ITER_CHUNK_SIZE = 20 * 10000
    
    # 配置日志级别, https://docs.python.org/3/library/logging.html#levels
    LOG_LEVEL = logging.INFO
    
    # 是否保存日志到文件，默认False
    SAVE_LOG_TO_FILE = True
    
    # 指定日志文件保存的绝对路径, 在开启保存日志到文件选项的情况下如不指定则
    # 默认保存到项目根目录下，按日期进行日志的命名：dataloader.<YYYY>-<mm>-<dd>.log
    # LOG_FILE_LOCATION = "/tmp"


@pvs.regist_for("producer_view_service")     # 声明这个session属于哪个DB
def load_cpl_service_data():
    # 自动生成的代码，按约定命名直接使用即可，表名按驼峰方式给定
    # from target.<dbname> import iter_<tbname>，<tbname>
    from target.producer_view_service import (
        iter_cpl_data, iter_complex_data, iter_cpl_complex_mapping, CplData
    )
    
    # 当一个字段是UUID类型时，请明确指定使用fastuuid来作为其生成策略
    # 如此可以根据生成量采取更为快速的UUID生成策略，以获取更好的性能

    # 使用retaining=True声明数据生成后主键字段保留待用
    # 注意, 只保留了主键字段及其值，未限制容量上限
    for cpl in iter_cpl_data(100, retaining=True, uuid=fastuuid()):
        yield cpl
        
    for cplx in iter_complex_data(100, uuid=fastuuid()):
        yield cplx
        
        # 使用incache来指定数据从指定表的指定字段获取：
        #     第一个参数是驼峰形式的表名，第二个是字符串形式的主键字段名
        # 现在的策略是按下标顺序获取值：
        # 例如在这个例子当中已生成100条CPL，但是这里只会从取前面的2条(这里数量是2）
        for mp in iter_cpl_complex_mapping(
            2, cpl_uuid=incache(CplData, "uuid"), complex_uuid=cplx.uuid
        ):
            yield mp
     
    # 不再使用之后释放掉保留的数据，参数为驼峰形式的表名
    free(CplData)

app = DataLoader(__name__, Config)    # 实例化应用

# 当有多个session时可选的简便方式：
# app.register_sessions([s1, s2, ...])
app.register_session(pvs)             # 注册session


if __name__ == "__main__":
    app.run()
```

运行（`examples/src`）：

```shell
$ env/bin/python app.py
```

## 问题

1. 对于唯一约束类型的字段，需要开发者自行覆盖数据生成策略来避免约束冲突

