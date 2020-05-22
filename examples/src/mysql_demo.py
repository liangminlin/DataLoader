import logging
from dataloader.helper import incache, free
from dataloader import DataLoader, LoadSession

pvs = LoadSession(__name__)   # 定义Load Session
logger = logging.getLogger(__name__)


class Config(object):
    """ 配置类，目前支持如下三个配置项 """
    DATABASE_URL = "mysql://root:123456@k8s-dev-1.aamcn.com.cn:32205/producer_view_service"

    # 多少条记录做一次IO提交到DB，默认 5W
    FLUSH_BUFF_SIZE = 5 * 10000

    # 每个批次生成多少条记录, 这个值影响占用内存的大小，默认10W
    ITER_CHUNK_SIZE = 10 * 10000

    LOG_LEVEL = logging.INFO
    SAVE_LOG_TO_FILE = True
    LOG_FILE_LOCATION = "/tmp"


@pvs.regist_for("producer_view_service")     # 声明这个session属于哪个DB
def load_cpl_service_data():
    # 自动生成的代码，按约定命名直接使用即可，
    # from target.<dbname> import iter_<tbname>
    from target.producer_view_service import (
        iter_cpl_data, iter_complex_data, iter_cpl_complex_mapping, CplData
    )

    # 使用retaining声明数据生成后主键字段保留待用
    for cpl in iter_cpl_data(20, retaining=True):
        yield cpl

    for cplx in iter_complex_data(100):
        yield cplx

        # 使用incache来指定数据从指定表的指定字段获取
        for ccm in iter_cpl_complex_mapping(
            20, cpl_uuid=incache(CplData, "uuid"), complex_uuid=cplx.uuid
        ):
            yield ccm

    # 不再使用之后释放掉保留的数据
    free(CplData)


app = DataLoader(__name__, Config)    # 实例化应用
app.register_session(pvs)             # 注册session


if __name__ == "__main__":
    app.run()
