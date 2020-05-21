import uuid
from dataloader import factories, logging
from dataloader import DataLoader, LoadSession


logger = logging.getLogger(__name__)


# 1. Define configuration
class Config(object):
    DATABASE_URL = "postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/cpl_service?connect_timeout=2"

    # 多少条记录做一次IO提交到DB, 建议值: 10w
    FLUSH_BUFF_SIZE = 10 * 10000

    # 每个批次生成多少条记录, 这个值影响占用内存的大小，建议值: 20w
    ITER_CHUNK_SIZE = 20 * 10000


# 2. Define LoadSession
cs = LoadSession(__name__)


# 2. Define LoadSession
@cs.regist_for("cpl_service")
def load_cpl_service_data():
    """ 1kw complex_lms_device and 1kw cpl_file"""
    from target.cpl_service import (
        iter_complex_lms_device, iter_cpl_file
    )

    for idx, cplx in iter_complex_lms_device(
        10*10000, complex_uuid=factories.FuzzyUuid()
    ):
        yield cplx

        for idx, cpl in iter_cpl_file(2):
            yield cpl


# 3. Create DataLoader App
app = DataLoader(__name__, Config)

# 4. Register LoadSession to DataLoader
app.register_session(cs)


# 5. Run the DataLoader app with app.run()
if __name__ == "__main__":
    app.run()
