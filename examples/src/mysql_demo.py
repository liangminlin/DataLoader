from dataloader import fast_rand
from dataloader import DataLoader, LoadSession

pvs = LoadSession(__name__)


class Config(object):
    DATABASE_URL = "mysql://root:123456@k8s-dev-1.aamcn.com.cn:32205/producer_view_service?connect_timeout=2"


@pvs.regist_for("producer_view_service")
def load_pvs_data():
    from target.producer_view_service import iter_cpl_data

    for cpl in iter_cpl_data(
        897, uuid=fast_rand.randuuid(),
        title=fast_rand.choice([
            "Content_for_Andy_2s_feature_2k_51", "3d white 2048 1080",
            "611_Xiamen_LXH_Benz_CLA_15s_mpg235", "3105_XinYang_15S_jpg239",
            "ADogsJourney_RTG-F_S_EN-XX_UK-PG_MOS_2K_EONE_20190419_DTU_IOP_OV"
        ])
    ):
        yield cpl


app = DataLoader(__name__, Config)
app.register_session( pvs )


if __name__ == "__main__":
    app.run()
