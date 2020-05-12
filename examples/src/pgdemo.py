from dataloader import fast_rand, logging
from dataloader import DataLoader, LoadSession


logger = logging.getLogger(__name__)


# 1. Define configuration
class Config(object):
    DATABASE_URL = "postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/cpl_service?connect_timeout=3"


# 2. Define LoadSession
cs = LoadSession(__name__)


# 2. Define LoadSession
@cs.regist_for("cpl_service")
def load_cpl_service_data():
    """ 1kw complex_lms_device and 1kw cpl_file"""
    from target.cpl_service import (
        iter_complex_lms_device, iter_cpl_file
    )

    for cplx in iter_complex_lms_device(
        1000 * 10000, complex_uuid=fast_rand.randuuid()
    ):
        yield cplx

        for cpl in iter_cpl_file(1):
            yield cpl


# 3. Create DataLoader App
app = DataLoader(__name__, Config)

# 4. Register LoadSession to DataLoader
app.register_session(cs)


# 5. Run the DataLoader app with app.run()
if __name__ == "__main__":
    app.run()
