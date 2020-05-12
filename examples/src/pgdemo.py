from dataloader import fast_rand, logging
from dataloader import DataLoader, LoadSession

cs = LoadSession(__name__)

logger = logging.getLogger(__name__)


class Config(object):
    DATABASE_URL = "postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/cpl_service?connect_timeout=3"


@cs.regist_for("cpl_service")
def load_cpl_service_data():
    from target.cpl_service import (
        iter_complex_lms_device, iter_cpl_file
    )

    for cplx in iter_complex_lms_device(52345, complex_uuid=fast_rand.randuuid()):
        yield cplx

        for cpl in iter_cpl_file(
            2
            # audio_formats="{5.1}",
            # experiences="{}",
            # subtitle_languages="{en}",
            # aspect_ratio_active_area='1.85'
        ):
            yield cpl


app = DataLoader(__name__, Config)
app.register_session( cs )


if __name__ == "__main__":
    app.load()
