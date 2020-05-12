from dataloader import fast_rand
from dataloader import DataLoader, LoadSession

cs = LoadSession(__name__)


class Config(object):
    DATABASE_URL = "postgresql://postgres:postgres@k8s-dev-1.aamcn.com.cn:32100/cpl_service"


@cs.regist_for("cpl_service")
def load_cpl_service_data():
    from target.cpl_service import (
        iter_cpl, iter_complex_lms_device, iter_cpl_location
    )

    for cplx in iter_complex_lms_device(50000, complex_uuid=fast_rand.randuuid()):
        yield cplx

        for cpl in iter_cpl(
            fast_rand.randint(1, 3),
            audio_formats="{5.1}",
            experiences="{}",
            subtitle_languages="{en}",
            aspect_ratio_active_area='1.85'
        ):
            yield cpl

            # for cpl_loc in iter_cpl_location(
            #     fast_rand.randint(1, 10),
            #     complex_uuid=cplx.complex_uuid,
            #     device_uuid=fast_rand.randuuid(),
            #     cpl_uuid=cpl.uuid
            # ):
            #     yield cpl_loc


app = DataLoader(__name__, Config)
app.register_session( cs )


if __name__ == "__main__":
    app.load()
