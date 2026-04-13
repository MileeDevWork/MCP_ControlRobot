"""
MCP tools for Truong Dai hoc Bach khoa Thanh pho Ho Chi Minh only.
"""

from typing import Optional, List
import re
import logging


logger = logging.getLogger("RobotControl.HCMUT")


HCMUT_BASE_KEYWORDS = [
    "đại học bách khoa hồ chí minh",
    "đại học bách khoa tp hcm",
    "đại học bách khoa tphcm",
    "hcmut",
    "bach khoa",
    "bách khoa",
    "truong dai hoc bach khoa",
]

HCMUT_TOPICS = [
    "tong_quan",
    "lich_su_va_dinh_huong",
    "dia_chi",
    "co_so_dao_tao",
    "website",
    "dao_tao",
    "chuong_trinh_dao_tao",
    "nganh_hoc",
    "nganh_hoc_day_du_2025",
    "tuyen_sinh",
    "phuong_thuc_tuyen_sinh",
    "hoc_phi",
    "hoc_bong",
    "co_so_vat_chat",
    "ky_tuc_xa",
    "nghien_cuu_khoa_hoc",
    "doi_song_sinh_vien",
    "co_hoi_nghe_nghiep",
    "hop_tac_quoc_te",
    "lien_he",
    "luu_y",
]

HCMUT_TOPIC_RESPONSES = {
    "tong_quan": (
        "Trường Đại học Bách khoa Thành phố Hồ Chí Minh thuộc hệ thống Đại học Quốc gia Thành phố Hồ Chí Minh "
        "và là trường đại học kỹ thuật trọng điểm."
    ),
    "lich_su_va_dinh_huong": (
        "Nhà trường có định hướng phát triển theo mô hình đại học nghiên cứu, chú trọng đào tạo kỹ sư và cử nhân chất lượng cao, "
        "gắn kết chặt chẽ giữa đào tạo, nghiên cứu khoa học, chuyển giao công nghệ và đổi mới sáng tạo."
    ),
    "dia_chi": (
        "Thông tin thường được dùng: Cơ sở Lý Thường Kiệt tại 268 Lý Thường Kiệt, "
        "Phường 14, Quận 10, Thành phố Hồ Chí Minh."
    ),
    "co_so_dao_tao": (
        "Nhà trường tổ chức đào tạo tại cơ sở Lý Thường Kiệt và cơ sở thuộc khu đô thị đại học tại thành phố Thủ Đức, "
        "tùy chương trình đào tạo và kế hoạch học tập cụ thể."
    ),
    "website": (
        "Kênh chính thức nên theo dõi:\n"
        "- Website trường: https://www.hcmut.edu.vn\n"
        "- Cổng tuyển sinh và các thông báo năm hiện hành: xem mục Tuyển sinh trên website trường."
    ),
    "dao_tao": (
        "Trường đào tạo đa bậc gồm Đại học, Thạc sĩ, Tiến sĩ; định hướng mạnh về kỹ thuật, "
        "công nghệ và ứng dụng thực tế với doanh nghiệp."
    ),
    "chuong_trinh_dao_tao": (
        "Các hệ chương trình đào tạo gồm chương trình tiêu chuẩn, chương trình dạy và học bằng tiếng Anh, "
        "chương trình tài năng, chương trình tiên tiến, chương trình định hướng quốc tế và một số chương trình liên kết."
    ),
    "nganh_hoc": (
        "Nhóm ngành chính tại Trường Đại học Bách khoa Thành phố Hồ Chí Minh gồm:\n"
        "1. Máy tính - Công nghệ thông tin - Dữ liệu.\n"
        "2. Điện - Điện tử - Viễn thông - Tự động hóa - Vi mạch.\n"
        "3. Cơ khí - Cơ điện tử - Robot - Ô tô - Hàng không.\n"
        "4. Dệt may - Hóa - Thực phẩm - Sinh học.\n"
        "5. Xây dựng - Kiến trúc - Địa kỹ thuật - Kinh tế xây dựng.\n"
        "6. Dầu khí - Địa chất - Vật liệu - Vật lý kỹ thuật - Y sinh.\n"
        "7. Quản lý công nghiệp - Logistics - Tài nguyên môi trường - Bảo dưỡng công nghiệp.\n"
        "Bạn có thể dùng hcmut_majors_full để lấy danh sách chi tiết theo mã tuyển sinh."
    ),
    "nganh_hoc_day_du_2025": (
        "Danh sách ngành chi tiết theo nhóm chương trình:\n"
        "1. Khoa học Máy tính.\n"
        "2. Kỹ thuật Máy tính.\n"
        "3. Điện, Điện tử, Viễn thông, Tự động hóa, Thiết kế Vi mạch.\n"
        "4. Kỹ thuật Cơ khí, Kỹ thuật Cơ Điện tử, Kỹ thuật Robot, Cơ Kỹ thuật.\n"
        "5. Kỹ thuật Ô tô, Kỹ thuật Hàng không, nhóm liên quan tàu thủy và hàng không.\n"
        "6. Kỹ thuật Hóa học, Công nghệ Sinh học, Công nghệ Thực phẩm.\n"
        "7. Dệt May.\n"
        "8. Xây dựng, Quản lý Dự án Xây dựng, Kiến trúc, Kinh tế Xây dựng, Địa Kỹ thuật Xây dựng.\n"
        "9. Dầu khí, Địa chất.\n"
        "10. Kỹ thuật Vật liệu, Vật lý Kỹ thuật, Kỹ thuật Y sinh.\n"
        "11. Logistics và Hệ thống Công nghiệp, Quản lý Công nghiệp, Bảo dưỡng Công nghiệp.\n"
        "12. Tài nguyên và Môi trường, Khoa học Dữ liệu.\n"
        "Bạn nên đối chiếu danh mục ngành và mã tuyển sinh tại cổng tuyển sinh chính thức của trường."
    ),
    "tuyen_sinh": (
        "Tuyển sinh đại học thường triển khai nhiều phương thức, kết hợp nhiều tiêu chí đánh giá. "
        "Mỗi phương thức có điều kiện hồ sơ, điều kiện ngoại ngữ, ngưỡng xét tuyển và chỉ tiêu riêng theo từng ngành."
    ),
    "phuong_thuc_tuyen_sinh": (
        "Các nhóm phương thức thường gồm xét tuyển theo kết quả kỳ thi chuẩn hóa, xét kết hợp nhiều tiêu chí, "
        "xét tuyển thẳng, ưu tiên xét tuyển và các phương thức riêng theo quy định của nhà trường và hệ thống đại học."
    ),
    "hoc_phi": (
        "Học phí phụ thuộc chương trình đào tạo, số tín chỉ đăng ký và lộ trình học. "
        "Mức học phí giữa chương trình tiêu chuẩn và chương trình định hướng quốc tế hoặc chương trình dạy bằng tiếng Anh có sự khác biệt."
    ),
    "hoc_bong": (
        "Nhà trường có các nhóm học bổng gồm học bổng khuyến khích học tập, học bổng từ doanh nghiệp, học bổng hỗ trợ hoàn cảnh khó khăn, "
        "học bổng cho thành tích nghiên cứu và học bổng từ các quỹ phát triển sinh viên."
    ),
    "co_so_vat_chat": (
        "Cơ sở vật chất gồm hệ thống phòng thí nghiệm chuyên ngành, xưởng thực hành, thư viện, không gian nghiên cứu, "
        "hạ tầng công nghệ thông tin, cùng các khu thể thao và sinh hoạt phục vụ người học."
    ),
    "ky_tuc_xa": (
        "Sinh viên có thể đăng ký lưu trú tại hệ thống ký túc xá theo kế hoạch phân bổ và điều kiện của từng khu, "
        "bao gồm ký túc xá gần cơ sở nội thành và ký túc xá trong khu đô thị đại học."
    ),
    "nghien_cuu_khoa_hoc": (
        "Nhà trường đẩy mạnh nghiên cứu khoa học ứng dụng và liên ngành, có các nhóm nghiên cứu, đề tài hợp tác doanh nghiệp, "
        "phòng thí nghiệm trọng điểm và hoạt động công bố khoa học."
    ),
    "doi_song_sinh_vien": (
        "Đời sống sinh viên gồm hoạt động câu lạc bộ học thuật, hoạt động Đoàn Hội, tình nguyện, cuộc thi chuyên môn, "
        "khởi nghiệp đổi mới sáng tạo, và các chương trình kỹ năng mềm."
    ),
    "co_hoi_nghe_nghiep": (
        "Sinh viên có lợi thế về nền tảng kỹ thuật, kỹ năng thực hành và kinh nghiệm dự án. "
        "Nhà trường có hoạt động kết nối doanh nghiệp, thực tập, ngày hội việc làm và mạng lưới cựu sinh viên."
    ),
    "hop_tac_quoc_te": (
        "Nhà trường phát triển hợp tác quốc tế trong đào tạo, nghiên cứu và trao đổi học thuật với đối tác nước ngoài, "
        "tạo cơ hội cho sinh viên tham gia chương trình liên kết và môi trường học tập quốc tế."
    ),
    "lien_he": (
        "Kênh liên hệ chính thức:\n"
        "1. Website trường: https://www.hcmut.edu.vn\n"
        "2. Cổng tuyển sinh và các đầu mối tư vấn học vụ trên website chính thức."
    ),
    "luu_y": "Bạn nên đối chiếu thông báo chính thức của trường để có thông tin cập nhật nhất.",
}


def _build_hcmut_full_profile() -> str:
    return (
        "Thông tin tổng hợp về Trường Đại học Bách khoa Thành phố Hồ Chí Minh:\n"
        f"1. {HCMUT_TOPIC_RESPONSES['tong_quan']}\n"
        f"2. {HCMUT_TOPIC_RESPONSES['lich_su_va_dinh_huong']}\n"
        f"3. {HCMUT_TOPIC_RESPONSES['dia_chi']}\n"
        f"4. {HCMUT_TOPIC_RESPONSES['co_so_dao_tao']}\n"
        f"5. {HCMUT_TOPIC_RESPONSES['dao_tao']}\n"
        f"6. {HCMUT_TOPIC_RESPONSES['chuong_trinh_dao_tao']}\n"
        f"7. {HCMUT_TOPIC_RESPONSES['nganh_hoc']}\n"
        f"8. {HCMUT_TOPIC_RESPONSES['nganh_hoc_day_du_2025']}\n"
        f"9. {HCMUT_TOPIC_RESPONSES['tuyen_sinh']}\n"
        f"10. {HCMUT_TOPIC_RESPONSES['phuong_thuc_tuyen_sinh']}\n"
        f"11. {HCMUT_TOPIC_RESPONSES['hoc_phi']}\n"
        f"12. {HCMUT_TOPIC_RESPONSES['hoc_bong']}\n"
        f"13. {HCMUT_TOPIC_RESPONSES['co_so_vat_chat']}\n"
        f"14. {HCMUT_TOPIC_RESPONSES['ky_tuc_xa']}\n"
        f"15. {HCMUT_TOPIC_RESPONSES['nghien_cuu_khoa_hoc']}\n"
        f"16. {HCMUT_TOPIC_RESPONSES['doi_song_sinh_vien']}\n"
        f"17. {HCMUT_TOPIC_RESPONSES['co_hoi_nghe_nghiep']}\n"
        f"18. {HCMUT_TOPIC_RESPONSES['hop_tac_quoc_te']}\n"
        f"19. {HCMUT_TOPIC_RESPONSES['lien_he']}\n"
        f"20. {HCMUT_TOPIC_RESPONSES['luu_y']}"
    )


NATURAL_OPENERS = [
    "Mình gửi bạn thông tin như sau:",
    "Thông tin bạn cần đây:",
    "Mình tóm tắt nhanh để bạn dễ theo dõi:",
]

NATURAL_CLOSERS = [
    "Nếu bạn muốn, mình có thể làm rõ thêm từng phần.",
    "Bạn cần mình mở rộng phần nào không?",
]


def _pick_variant(variants: List[str], seed_text: str) -> str:
    if not variants:
        return ""
    return variants[sum(ord(ch) for ch in seed_text) % len(variants)]


def _number_token_to_text(token: str) -> str:
    mapping = {
        "0": "không", "1": "một", "2": "hai", "3": "ba", "4": "bốn",
        "5": "năm", "6": "sáu", "7": "bảy", "8": "tám", "9": "chín",
    }
    return " ".join(mapping.get(ch, ch) for ch in token)


def _convert_numbers_to_text(text: str) -> str:
    return re.sub(r"\d[\d.,]*", lambda m: _number_token_to_text(m.group(0)), text)


def _naturalize_response(base_text: str, seed_text: str) -> str:
    raw = f"{_pick_variant(NATURAL_OPENERS, seed_text)}\n{base_text}\n\n{_pick_variant(NATURAL_CLOSERS, seed_text[::-1])}"
    return _convert_numbers_to_text(raw)


def _detect_hcmut_topic(normalized: str) -> Optional[str]:
    if any(k in normalized for k in ["đầy đủ", "chi tiết", "toàn bộ", "full"]):
        return "tong_quan"
    if any(k in normalized for k in ["lịch sử", "định hướng", "sứ mệnh", "tầm nhìn"]):
        return "lich_su_va_dinh_huong"
    if any(k in normalized for k in ["địa chỉ", "ở đâu"]):
        return "dia_chi"
    if any(k in normalized for k in ["cơ sở đào tạo", "cơ sở học", "cơ sở"]):
        return "co_so_dao_tao"
    if any(k in normalized for k in ["website", "trang web"]):
        return "website"
    if any(k in normalized for k in ["đào tạo", "chương trình"]):
        if any(k in normalized for k in ["hệ", "loại chương trình", "tiêu chuẩn", "tiên tiến", "tài năng"]):
            return "chuong_trinh_dao_tao"
        return "dao_tao"
    if any(k in normalized for k in ["ngành", "chuyên ngành"]):
        if any(k in normalized for k in ["đầy đủ", "toàn bộ", "danh sách", "chi tiết"]):
            return "nganh_hoc_day_du_2025"
        return "nganh_hoc"
    if any(k in normalized for k in ["tuyển sinh", "xét tuyển", "đầu vào"]):
        if any(k in normalized for k in ["phương thức", "hình thức", "cách xét"]):
            return "phuong_thuc_tuyen_sinh"
        return "tuyen_sinh"
    if any(k in normalized for k in ["học phí"]):
        return "hoc_phi"
    if any(k in normalized for k in ["học bổng"]):
        return "hoc_bong"
    if any(k in normalized for k in ["cơ sở vật chất", "phòng lab", "thư viện", "phòng thí nghiệm"]):
        return "co_so_vat_chat"
    if any(k in normalized for k in ["ký túc xá", "nội trú", "chỗ ở"]):
        return "ky_tuc_xa"
    if any(k in normalized for k in ["nghiên cứu", "đề tài", "công bố", "chuyển giao"]):
        return "nghien_cuu_khoa_hoc"
    if any(k in normalized for k in ["đời sống", "câu lạc bộ"]):
        return "doi_song_sinh_vien"
    if any(k in normalized for k in ["việc làm", "nghề nghiệp", "thực tập"]):
        return "co_hoi_nghe_nghiep"
    if any(k in normalized for k in ["hợp tác quốc tế", "quốc tế", "liên kết quốc tế", "trao đổi"]):
        return "hop_tac_quoc_te"
    if any(k in normalized for k in ["liên hệ"]):
        return "lien_he"
    return None


def detect_hcmut_info(text: str, require_base_keyword: bool = True) -> Optional[str]:
    normalized = text.strip().lower()
    if require_base_keyword and not any(k in normalized for k in HCMUT_BASE_KEYWORDS):
        return None

    if any(k in normalized for k in ["chi tiết", "đầy đủ", "toàn bộ", "tất cả", "full"]):
        return _naturalize_response(_build_hcmut_full_profile(), normalized)

    topic = _detect_hcmut_topic(normalized)
    if topic:
        return _naturalize_response(HCMUT_TOPIC_RESPONSES[topic], normalized)

    fallback = (
        f"{HCMUT_TOPIC_RESPONSES['tong_quan']}\n"
        f"{HCMUT_TOPIC_RESPONSES['website']}\n"
        "Bạn có thể hỏi theo chủ đề: tuyển sinh, ngành học, học phí, học bổng."
    )
    return _naturalize_response(fallback, normalized)


def register_hcmut_tools(mcp) -> None:
    @mcp.tool()
    async def hcmut_info(user_text: str = "") -> dict:
        logger.info(f"[Tool:hcmut_info] request={{'user_text': {user_text!r}}}")
        if not user_text.strip():
            response = {"success": False, "message": "Bạn chưa nhập nội dung câu hỏi về Trường Đại học Bách khoa Thành phố Hồ Chí Minh."}
            logger.info(f"[Tool:hcmut_info] response={response}")
            return response
        # Called directly as an HCMUT-only tool, so do not require school-name keyword.
        message = detect_hcmut_info(user_text, require_base_keyword=False)
        if not message:
            response = {"success": False, "message": "Mình chưa nhận diện được câu hỏi liên quan đến Trường Đại học Bách khoa Thành phố Hồ Chí Minh."}
            logger.info(f"[Tool:hcmut_info] response={response}")
            return response
        response = {"success": True, "message": message}
        logger.info(f"[Tool:hcmut_info] response={response}")
        return response

    @mcp.tool()
    async def hcmut_topics() -> dict:
        response = {
            "success": True,
            "topics": HCMUT_TOPICS,
            "message": "Dùng hcmut_topic_detail(topic) để lấy thông tin chi tiết theo từng chủ đề.",
        }
        logger.info("[Tool:hcmut_topics] request={}")
        logger.info(f"[Tool:hcmut_topics] response={response}")
        return response

    @mcp.tool()
    async def hcmut_topic_detail(topic: str) -> dict:
        logger.info(f"[Tool:hcmut_topic_detail] request={{'topic': {topic!r}}}")
        normalized = topic.strip().lower()
        value = HCMUT_TOPIC_RESPONSES.get(normalized)
        if not value:
            response = {"success": False, "message": "Chủ đề không hợp lệ. Gọi hcmut_topics để xem danh sách chủ đề hỗ trợ."}
            logger.info(f"[Tool:hcmut_topic_detail] response={response}")
            return response
        response = {"success": True, "topic": normalized, "message": _naturalize_response(value, normalized)}
        logger.info(f"[Tool:hcmut_topic_detail] response={response}")
        return response

    @mcp.tool()
    async def hcmut_majors_full() -> dict:
        logger.info("[Tool:hcmut_majors_full] request={}")
        response = {
            "success": True,
            "topic": "nganh_hoc_day_du_2025",
            "message": _naturalize_response(HCMUT_TOPIC_RESPONSES["nganh_hoc_day_du_2025"], "hcmut_majors_full"),
        }
        logger.info(f"[Tool:hcmut_majors_full] response={response}")
        return response
