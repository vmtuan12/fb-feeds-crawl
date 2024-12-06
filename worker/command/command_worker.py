from worker.base_worker import BaseWorker
from connectors.db_connector import DbConnectorBuilder, KafkaProducerBuilder
from utils.constants import PostgresConnectionConstant as Postgres, KafkaConnectionConstant as Kafka
from entities.entities import CommandEntity
from utils.command_utils import CommandType
from custom_logging.logging import TerminalLogging

class CommandWorker(BaseWorker):
    def __init__(self) -> None:
        self.pg_conn = DbConnectorBuilder()\
                        .set_host(Postgres.HOST)\
                        .set_port(Postgres.PORT)\
                        .set_username(Postgres.USER)\
                        .set_password(Postgres.PWD)\
                        .set_db_name(Postgres.DB)\
                        .build_pg()
        self.kafka_producer = KafkaProducerBuilder().set_brokers(Kafka.BROKERS)\
                                                    .build()
        
    def _get_page_list(self) -> set:
        cursor = self.pg_conn.cursor()
        cursor.execute("""
        SELECT po.username from fb.page_options po WHERE po.option = 'included';
        """)
        rows = cursor.fetchall()
        cursor.close()

        default_pages = ["tinnhanhthanhvinh","TinhHoaBinh","ChuyenFangirl","PhuketStudio.so2","batradaaa","90phutTV","@NGHEANBEAT","MUSVNfanpage","toilanguoihanoi","HaNoiCuaToii","capnhattunggiay","daomusicnews","fanlangnhincuocsong","okiahanoi","QuangYen24h","ocu.vn","runyouredmxyz","YeuCuDem","TrangPhucBieuDienLily","rapvietmusic","tinhaynhatngay","thoisuvtv","daiphatthanh.sound","neuconfessions","truyenhinhvov","giaitri.blogtamsu.vn","wechoice.vn","yeah1news","thoibaonganhang.vn","tintucvtv24","storybienhoa","hanoifan","bongda55","toiladanthuduc.vn","QuangNgaiPlus","thegioiKPOP.asia","FanpageNho","hoahoctro.vn","ChuKienThuc","vkrnews","baotuoitre","congdongvnexpress","baocongthuong","matdienvnn","vtcnowofficial","pageGiaVang","toihanoi.30","idollive28","vnews.gov.vn","tinnhanhchungkhoan","tinbinhdinh.info","@vtv24caplayeuthuong","bantinhinhsu247","DeepLoveEntertainment","tinmoi.vn","trollbongda","thinh2sao.vietnamnet","doisongvnn","duonggiapage","beattvv.co","AnhTraiSayHi.VieChannelHTV2","uel360","SSvaKR","kizworldvn","truongnguoita.vn","diemtinshowbizz","ngaovl","sandinh.game","Phuvll","saonhapngu","dongmauxanh.chelsea","StyleTV.Page","phephim","doisongxahoipage","baosuckhoevadoisong","vandieuhay.net","BongdaPhui.net","K14vn","kienthuc.net.vn.fan","baodientuvtv","webtretho.vietnam","www.hanoionline.vn","SirAlexViDai","Vietbilliard","cafesangvoivtv3","capnhatbien","@lckvietnams","TruyenHaiLMHT","28hongbienhoabinh","tin.tuc.xe","TintucSoha","phunuonline.com.vn","viethome","bongvaro","tintuccand","trungtammusictalent","hues.vn","nhandandientutiengviet","Theanh28Sport","thanhphohanhphuc","vov1.vn","tv.blogtamsu.vn","greenpalaceshade","thienthannho.vn","baodongthap","phunugiadinhvietnam","LovelyMimiFP","vtv24ViecTuTe","BBCnewsVietnamese","theanh28express","saigonplus.net","Theanh28.60giay","vtvthoitiet","hhsb.vn","webthethao247.page","BacKan.tp","gocquangtrivn","tintucsonla24h","veNghiXuan","giaitrivtv3","quochoitv.vn","NghiaHungTV","phongvientrebctt","kinglive.vccorp","ChuyencuaHaNoi","MancuniansFamily","mottruyentranh","thongtinninhbinh24h","90sbangcassette","namplusvn","tcsbvn","QuangNinhConnect","Codongpage","vietnamnet.vn","bacgiangfun","Theanh28.Hanoi","langthanghanoiofficial","VinhPhuc247","topcomments.vn","hhsbvideo","Nhacchillphet","chimlonshowbiz","bongdachauaupage","nhatky","baothethaovanhoa","vutruduatin","caothuvn","FanBongda24h.vn","TrollRapViet","sorrymusicc","tgsb.vnn","chuyenthienha.fanpage","cafebiz.vn","tinngoisao","www.kenhthethao.vn","yannews","nhungcautruyenthamthuy2018","VTVcab.Tintuc","trungquockokiemduyet","anninhxahoi.page","hanoitoiyeu30","ViBongDaViet","laodongonline","DIYsangtao","afamilyvccorp","DAMLAY96","kenhnhacvietnam","tintuctungphut","realmadridcf.vietnam","tintuctinhbinhdinh","FCManUnitedVN","hatgiongtamhon","BaoDanTocVaPhatTrien","congthongtindientutpdanang","vtvchuyendong","kenhtruyen","tintucquangbinh24h","dacsanmiensongnuoctv","womannews.vn","TrenDuongPitch","schannel.vn","tuoitrequynhon","phathocdoisong9","K14special","truyenhinhthanhhoa","adm.vanhoaviet.vn","RFIvi","QueMinhQuangNam","hatinhtv.vnn","saobiz.vn","baodantridientu","phapluat.tuoitrethudo.com.vn","DAnghiencuuquocte","ThuongXuanOnLine","khuphobaton","tram.cotdien","phimchieuraps","dienanhcbiz","NguoiChiLinhHD","nhipcaudautu.vn","bachkhoatoannhac","lolnetizenvtrans","gialambeat","thethao247.vn","VGTTV.channel","nhungcaunoibathu","Theanh28","tiin.vn","mangenz.vn","baohatinh","theanh28.page","denthanhpheroletuy","doisongviet.2024","emdep.vn","trumtrada.entertainment","thethaovntintuc","saigontv.official","phuxuyen24g","nghean24h.vn","tinnongday","xehay","thegioivanhoa","100076216868116","tintucuc","beatvn.network","KenhVOVGT","goalvietnam","tintucmoinhattrongngay","rapphim.vn","hanoiso1","thichquangcao","ONEEsportsVN","thongtinchinhphu","TuyenVanHoaOfficial","BaTamShowbiz.vn","undefendable","baodientu","chandongshowbiz","HaNoiCuaToi12","baobinhaquarius","Reviewdanang24h","xuanduongcssr","bongdaphuisaigon","trasuachotamhon","hongexpress.page","EsportsiThethaovn","sieuanhhungMarvel","tintuconlinevnn","danang.mylove","thongtindalat","vutrushowbiz","Tintuchayonline24h","PenaMadridistaVietnam","nhabaovacongluan","vtvgiaitri","cttdongthap","NhatoiThaiBinh","truyenthongrec","thainguyen24h","baodientubinhduong","2sao.vietnamnet.vn","BTLCSCD","tinnhanhbentre","sporttv.vietnam","truyenhinhhungyen","tintuccoin68","Truyenhinh.BaoTuoiTre","svghongsaigon","vietnamnews.asia","MUSVNpage","anninhkontum","dungchetvithieuhieubiet.vn","bancong.com.vn","tintuc.hues.vn","tinmoi1234","quynhonjob","ghienbongdaTV","thanhnien","TrollEsportsTV","when.in.vietnam.vn","Vietnamnews7","bongda6666","tintuc.cbiz","nhung.truyen.ngan.hay","BongDaJK","hanoimetro24h","AnhTraiVuotNganChongGai","ManUnitedVN1878","tintucphapluatvadoisong","500broslienminh","cuongphimreview","laocai24h","ButEDMMusique","diendantaynguyen","hatinhngaymoi24h","LECVNEU","trollgamecom","www.baotintuc.vn","TinTucKhanhHoa","bookademy.vn","PSGVNFC1970","cskhanhphuongfan","onestvn.media","vtcnewsvn","hoisangchanh","khongsocho.official","khatvongsong","hanoipho","fbfan24h","bikipyeu","mew629","thongtinhanquoc","tintucvietnammoinong","nchmf","bepnhatv","congly.vn","tintuc247h","KCrushbetterthanyourcrush","trollbongrovn","UnitedFuture1902","tapchibapcai","TinBinhSon","VOATiengViet","ybtintuc","TinTucHungYen247","beatlangson.official","PageWSS","haiphongnewss","rightmusic.asia","tckt.ktsvn","doisongvanhoa","bachhop.giatrang","@goprothoigiaosu","Genk.vn","antttthue","TintucMSN","muvn.page","nguoilaodong","GAMeSportsVN","100063439916969","www.doanhnhan.vn","HaNoi18h00","khanhhoa247","tintaynguyen.net","laithuonghung.blog","baolongan.vn","showbiznhatbao","capnhattungphut","caphethubay","inthemoodforshowbiz","theanh28trendingg","www.thantuong.tv","ghienbongdaofficial","OurVietnam","tinbongdapro","nlsrlplfanpage","danangyeuthuong","cityvietnam","doisongphapluatonline","pvdinhque.docbao.vn","kinhtesaigon.vn","pagechuithue","lolesportsvn","cothebandabiet","xevathethao.vn","baodientuvov.vn","yeah1tv","VGTVN.news","vietnamindiemusic","TrollCaShowbiz","yantv"]
        return set([r[0] for r in rows] + default_pages)

    def start(self):
        list_pages = self._get_page_list()
        current_partition = 0

        for index, p in enumerate(list_pages):
            scrape_msg = CommandEntity(cmd_type=CommandType.SCRAPE_PAGE, page=p).to_dict()

            try:
                self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg, partition=current_partition)
            except AssertionError as ae:
                current_partition = 0
                self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=scrape_msg, partition=current_partition)

            current_partition += 1

            TerminalLogging.log_info(f"Command {scrape_msg}")

            if index == (len(list_pages) - 1):
                temp_partition = 0
                clear_cache_msg = CommandEntity(cmd_type=CommandType.CLEAR_CACHE).to_dict()
                TerminalLogging.log_info(f"Command {clear_cache_msg}")

                while (True):
                    try:
                        self.kafka_producer.send(Kafka.TOPIC_COMMAND, value=clear_cache_msg, partition=temp_partition)
                        temp_partition += 1
                    except AssertionError as ae:
                        break
                
        self.kafka_producer.flush()

    def clean_up(self):
        self.kafka_producer.flush()
        self.kafka_producer.close(timeout=5)
        self.pg_conn.close()

    def __del__(self):
        self.clean_up()
    
    def __delete__(self):
        self.clean_up()