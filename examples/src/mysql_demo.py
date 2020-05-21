import uuid
from dataloader import factories
from dataloader import DataLoader, LoadSession

pvs = LoadSession(__name__)


class Config(object):
    DATABASE_URL = "mysql://root:123456@k8s-dev-1.aamcn.com.cn:32205/producer_view_service"
    
    # 多少条记录做一次IO提交到DB, 建议值: 10w
    FLUSH_BUFF_SIZE = 10 * 10000

    # 每个批次生成多少条记录, 这个值影响占用内存的大小，建议值: 20w
    ITER_CHUNK_SIZE = 20 * 10000


@pvs.regist_for("producer_view_service")
def load_pvs_data():
    from target.producer_view_service import iter_cpl_data

    for idx, cpl in iter_cpl_data(
        30*10000, uuid=factories.FuzzyUuid()
    ):
        yield cpl


app = DataLoader(__name__, Config)
app.register_session( pvs )


if __name__ == "__main__":
    app.run()
