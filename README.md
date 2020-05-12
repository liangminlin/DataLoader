## DataLoader

一个自动反射数据表结构以生成模拟数据并加载到DB的测试辅助框架；设计原则：

1. 简单可配置，约定优于配置；
2. 自动反射表结构和生成模拟数据；
3. 开发者只需开发LoadSession定义表关系即可。



* 运行DEMO（CPL-SERVICE)：`make init-demo && make run-demo`

## 引入

（暂未支持pip方式）目前可手动打包：`$ make dist`

> $ pip3 install git+https://<token\>@github.com/artsalliancemedia/producer2-stress-testing@<version\>#egg=dataloader

##  配置

至少需要在配置当中指定数据库的连接地址，目前支持的数据库类型为Postgres和MySQL：

**MySQL**:    `mysql+mysqlconnector://{username}:{password}@{hostname}:{port}/{database}`

**Postgres**: `postgresql://{username}:{password}@{hostname}:{port}/{database}`



`DATABASE_URL`：以配置类属性的形式存在，类型为字符串，值如上所示，为数据库连接schema；

`DATABASE_URLS`：以配置类属性的形式存在，类型为字符串列表，列表值如上所示，为数据库连接schema。



## 例子

以cpl-service为例来写个小demo：

```python
from dataloader import fast_rand
from dataloader import DataLoader, LoadSession

cs = LoadSession(__name__)   # 定义Load Session


class Config(object):
    """ 配置类，目前支持 DATABASE_URL 和 DATABASE_URLS 配置 """
    DATABASE_URL = "postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/cpl_service"


@cs.regist_for("cpl_service")     # 声明这个session属于哪个DB
def load_cpl_service_data():
    # 自动生成的代码，按约定命名直接使用即可，
    # from target.<dbname> import iter_<tbname>
    from target.cpl_service import (
        iter_cpl, iter_complex_lms_device, iter_cpl_location
    )

    for cplx in iter_complex_lms_device(10):   # 指定固定量的模拟数据生成量
        yield cplx       # 生成数据

        # 随机离散数据生成量，注意这里的数量乘以祖先级for的数量才是其生成量
        for cpl in iter_cpl( fast_rand.randint(1, 10) ): 
            yield cpl       # 生成数据

            for cpl_loc in iter_cpl_location(
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
    app.load()
```

运行（`examples/src`）：

`$ make init-demo && make run-demo`

## Issues

1. MySQL的适配尚未经过测试；
3. 非自增主键/唯一约束的冲突尚未解决，需要使用者自定策略解决；
4. 随机生成的数据不是很快，需优化：目前是提供了fast_rand（`from dataloader import fast_rand`）编程接口来提供`randint`、`randuuid`、`choice`等方法，未来将会替换内部实现以提供更好的性能。