import logging

from dataloader import DataLoader, LoadSession
from dataloader.helper import free, incache, fastuuid


logger = logging.getLogger(__name__)


class Config(object):
    DATABASE_URLS = [
        "postgresql://postgres:postgres@k8s-dev-1-localhost:32100/cpl_service",
        "mysql://root:123456@k8s-dev-1-localhost:32205/producer_view_service_test"
    ]
    SAVE_LOG_TO_FILE = True


pvs = LoadSession(__name__)
cs = LoadSession(__name__)


@pvs.regist_for("producer_view_service_test")
def load_pvs_data():
    from target.producer_view_service_test import (
        iter_complex_data, iter_cpl_data, CplData,
        iter_complex_device_mapping, iter_cpl_locations_mapping
    )

    for cpl_data in iter_cpl_data(20, retaining=True, uuid=fastuuid()):
        yield cpl_data

    for cplx_data in iter_complex_data(10, uuid=fastuuid()):
        yield cplx_data

        for cdm in iter_complex_device_mapping(
            10,
            complex_uuid=cplx_data.uuid,
            device_uuid=fastuuid()
        ):
            yield cdm

            for clm in iter_cpl_locations_mapping(
                10,
                device_uuid=cdm.device_uuid,
                complex_uuid=cdm.complex_uuid,
                cpl_uuid=incache(CplData, "uuid")
            ):
                yield clm

    free(CplData)


@cs.regist_for("cpl_service")
def load_cpl_data():
    from target.cpl_service import (
        iter_complex_lms_device, iter_cpl_location, iter_cpl, Cpl
    )

    for c in iter_cpl(10, retaining=True, uuid=fastuuid()):
        yield c

    for cld in iter_complex_lms_device(
        10, complex_uuid=fastuuid(), device_uuid=fastuuid()
    ):
        yield cld

        for cl in iter_cpl_location(
            8,
            complex_uuid=cld.complex_uuid,
            device_uuid=cld.device_uuid,
            cpl_uuid=incache(Cpl, "uuid")
        ):
            yield cl

    free(Cpl)


app = DataLoader(__name__, Config)
app.register_sessions([pvs, cs])


if __name__ == "__main__":
    app.run()
