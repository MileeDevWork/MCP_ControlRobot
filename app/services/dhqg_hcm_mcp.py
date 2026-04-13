"""
MCP tools for Dai hoc Quoc gia Thanh pho Ho Chi Minh only.
"""

from typing import Optional, List
import re
import logging


logger = logging.getLogger("RobotControl.DHQGHCM")

DHQG_HCM_BASE_KEYWORDS = [
    "đại học quốc gia thành phố hồ chí minh",
    "đại học quốc gia tphcm",
    "đại học quốc gia tp hcm",
    "đại học quốc gia",
    "đhqg",
    "vnu hcm",
    "vnu-hcm",
]

DHQG_HCM_TOPICS = [
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tong_quan",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_cau",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_thanh_vien",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_don_vi_truc_thuoc",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_dao_tao",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tuyen_sinh",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hoc_bong",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_nghien_cuu",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_so",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_he_sinh_thai_hoc_tap",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_ho_tro_sinh_vien",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hop_tac",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_doi_moi_sang_tao_va_chuyen_doi_so",
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_day_du",
]

DHQG_HCM_TOPIC_RESPONSES = {
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tong_quan": (
        "Đại học Quốc gia Thành phố Hồ Chí Minh là hệ thống giáo dục đại học đa ngành, đa lĩnh vực, "
        "đào tạo từ bậc đại học đến sau đại học, đồng thời là trung tâm nghiên cứu và đổi mới sáng tạo có vai trò quan trọng."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_cau": (
        "Hệ thống gồm các trường đại học thành viên, các viện nghiên cứu, các khoa trực thuộc và đơn vị chức năng. "
        "Các đơn vị phối hợp theo định hướng liên ngành, chia sẻ hạ tầng học thuật và thúc đẩy nghiên cứu chung."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_thanh_vien": (
        "Các trường và viện thành viên trong hệ thống hiện gồm: "
        "Trường Đại học Bách khoa, Trường Đại học Khoa học Tự nhiên, Trường Đại học Khoa học Xã hội và Nhân văn, "
        "Trường Đại học Quốc tế, Trường Đại học Công nghệ Thông tin, Trường Đại học Kinh tế Luật, "
        "Trường Đại học An Giang, Trường Đại học Khoa học Sức khỏe, cùng Viện Môi trường và Tài nguyên."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_don_vi_truc_thuoc": (
        "Ngoài khối trường thành viên, hệ thống còn có các đơn vị trực thuộc phục vụ đào tạo, khảo thí, nghiên cứu, "
        "thư viện, ký túc xá, đào tạo quốc tế, giáo dục quốc phòng và các trung tâm hỗ trợ học thuật khác."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_dao_tao": (
        "Hoạt động đào tạo bao phủ nhiều nhóm lĩnh vực như kỹ thuật, công nghệ, khoa học tự nhiên, khoa học xã hội, "
        "kinh tế, quản lý, y sinh và các lĩnh vực liên ngành."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tuyen_sinh": (
        "Hệ thống tổ chức tuyển sinh theo nhiều phương thức tùy từng đơn vị thành viên, "
        "bao gồm xét tuyển theo kết quả học tập, kết quả kỳ thi chuẩn hóa và các phương thức kết hợp theo quy định hiện hành."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hoc_bong": (
        "Người học có thể tiếp cận nhiều nhóm học bổng như học bổng khuyến khích học tập, học bổng doanh nghiệp, "
        "học bổng hỗ trợ hoàn cảnh khó khăn, học bổng trao đổi học thuật và các quỹ hỗ trợ phát triển tài năng."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_nghien_cuu": (
        "Hệ thống tập trung mạnh vào nghiên cứu khoa học, chuyển giao công nghệ, đổi mới sáng tạo và hợp tác với doanh nghiệp, "
        "góp phần giải quyết các bài toán thực tiễn của xã hội."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_so": (
        "Khu đô thị đại học tại phường Linh Trung, thành phố Thủ Đức là khu vực tập trung nhiều trường thành viên, "
        "ký túc xá, thư viện và hạ tầng nghiên cứu, tạo môi trường học tập liên thông cho người học."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_he_sinh_thai_hoc_tap": (
        "Hệ sinh thái học tập gồm thư viện trung tâm, không gian tự học, phòng thí nghiệm, trung tâm thực hành, "
        "hạ tầng công nghệ thông tin và các nền tảng học tập số dùng chung giữa nhiều đơn vị."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_ho_tro_sinh_vien": (
        "Người học có thể tiếp cận hệ sinh thái hỗ trợ gồm tư vấn học tập, hướng nghiệp, học bổng, hoạt động sinh viên, "
        "không gian nghiên cứu và mạng lưới kết nối doanh nghiệp."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hop_tac": (
        "Hệ thống đẩy mạnh hợp tác trong nước và quốc tế, phát triển chương trình liên kết, trao đổi học thuật và "
        "các hoạt động nghiên cứu chung để nâng cao chất lượng đào tạo."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_doi_moi_sang_tao_va_chuyen_doi_so": (
        "Hệ thống ưu tiên đổi mới sáng tạo và chuyển đổi số trong quản trị, đào tạo và nghiên cứu, "
        "thúc đẩy kết nối giữa trường đại học, viện nghiên cứu, doanh nghiệp và cơ quan quản lý."
    ),
    "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_day_du": (
        "Thông tin chi tiết về Đại học Quốc gia Thành phố Hồ Chí Minh:\n"
        "1. Vai trò: Là hệ thống đại học đa ngành, giữ vai trò trung tâm đào tạo và nghiên cứu.\n"
        "2. Cấu trúc: Gồm các trường thành viên, viện thành viên, đơn vị trực thuộc và các trung tâm hỗ trợ học thuật.\n"
        "3. Thành viên: Có các trường thành viên đa lĩnh vực cùng viện nghiên cứu chuyên sâu.\n"
        "4. Đào tạo: Tổ chức đào tạo đa bậc, đa ngành, chú trọng liên ngành và chất lượng đầu ra.\n"
        "5. Tuyển sinh: Nhiều phương thức tuyển sinh theo quy định của từng đơn vị thành viên.\n"
        "6. Học bổng: Có các nhóm học bổng học tập, hỗ trợ tài chính và phát triển tài năng.\n"
        "7. Nghiên cứu: Tăng cường nghiên cứu, chuyển giao công nghệ và hợp tác với doanh nghiệp.\n"
        "8. Hạ tầng: Có khu đô thị đại học tại phường Linh Trung, thành phố Thủ Đức với thư viện, ký túc xá, hạ tầng nghiên cứu.\n"
        "9. Hệ sinh thái học tập: Có hạ tầng học thuật và nền tảng số dùng chung cho người học.\n"
        "10. Hỗ trợ sinh viên: Có tư vấn học tập, hướng nghiệp, hoạt động sinh viên và kết nối việc làm.\n"
        "11. Hợp tác: Mở rộng hợp tác trong nước và quốc tế ở cả đào tạo và nghiên cứu.\n"
        "12. Đổi mới sáng tạo và chuyển đổi số: Thúc đẩy chuyển đổi số toàn hệ thống gắn với đổi mới sáng tạo."
    ),
}

DHQG_HCM_TOPIC_ALIASES = {
    "tong_quan": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tong_quan",
    "co_cau": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_cau",
    "thanh_vien": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_thanh_vien",
    "don_vi_truc_thuoc": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_don_vi_truc_thuoc",
    "dao_tao": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_dao_tao",
    "dao_tao_y_sinh": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_dao_tao",
    "dao_tao_lien_nganh": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_dao_tao",
    "tuyen_sinh": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tuyen_sinh",
    "hoc_bong": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hoc_bong",
    "nghien_cuu": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_nghien_cuu",
    "co_so": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_so",
    "ky_tuc_xa": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_so",
    "he_sinh_thai_hoc_tap": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_he_sinh_thai_hoc_tap",
    "ho_tro_sinh_vien": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_ho_tro_sinh_vien",
    "hop_tac": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hop_tac",
    "doi_moi_sang_tao": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_doi_moi_sang_tao_va_chuyen_doi_so",
    "chuyen_doi_so": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_doi_moi_sang_tao_va_chuyen_doi_so",
    "day_du": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_day_du",
}


NATURAL_OPENERS = [
    "Mình gửi bạn thông tin như sau:",
    "Thông tin bạn cần đây:",
]

NATURAL_CLOSERS = [
    "Nếu bạn muốn, mình có thể làm rõ thêm theo từng nhóm chủ đề.",
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


def detect_dhqg_hcm_topic(normalized_text: str) -> str:
    if any(k in normalized_text for k in ["chi tiết", "đầy đủ", "toàn bộ", "tổng quan", "full"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_day_du"
    if any(k in normalized_text for k in ["thành viên", "trường thành viên", "viện thành viên"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_thanh_vien"
    if any(k in normalized_text for k in ["đơn vị trực thuộc", "trực thuộc", "trung tâm", "đơn vị"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_don_vi_truc_thuoc"
    if any(k in normalized_text for k in ["cơ cấu", "tổ chức", "đơn vị", "thành viên"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_cau"
    if any(k in normalized_text for k in ["đào tạo", "ngành", "chương trình"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_dao_tao"
    if any(k in normalized_text for k in ["tuyển sinh", "xét tuyển", "đầu vào"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tuyen_sinh"
    if any(k in normalized_text for k in ["học bổng", "hỗ trợ tài chính", "quỹ học bổng"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hoc_bong"
    if any(k in normalized_text for k in ["nghiên cứu", "đổi mới", "chuyển giao"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_nghien_cuu"
    if any(k in normalized_text for k in ["cơ sở", "khu đô thị đại học", "linh trung", "thủ đức", "ký túc xá", "thư viện"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_co_so"
    if any(k in normalized_text for k in ["hệ sinh thái", "thư viện trung tâm", "hạ tầng học tập", "nền tảng số"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_he_sinh_thai_hoc_tap"
    if any(k in normalized_text for k in ["học bổng", "hỗ trợ", "hướng nghiệp", "sinh viên"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_ho_tro_sinh_vien"
    if any(k in normalized_text for k in ["hợp tác", "quốc tế", "liên kết"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_hop_tac"
    if any(k in normalized_text for k in ["chuyển đổi số", "đổi mới sáng tạo", "hệ thống số"]):
        return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_doi_moi_sang_tao_va_chuyen_doi_so"
    return "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_tong_quan"


def detect_dhqg_hcm_info(text: str) -> Optional[str]:
    normalized = text.strip().lower()
    if not any(k in normalized for k in DHQG_HCM_BASE_KEYWORDS):
        return None
    topic = detect_dhqg_hcm_topic(normalized)
    return _naturalize_response(DHQG_HCM_TOPIC_RESPONSES[topic], normalized)


def _resolve_dhqg_topic(topic_or_query: str) -> Optional[str]:
    normalized = topic_or_query.strip().lower()

    # Exact canonical topic id
    if normalized in DHQG_HCM_TOPIC_RESPONSES:
        return normalized

    # Exact alias key
    if normalized in DHQG_HCM_TOPIC_ALIASES:
        return DHQG_HCM_TOPIC_ALIASES[normalized]

    # Alias by containment, useful for keys like "..._dao_tao_y_sinh"
    for alias_key, canonical in DHQG_HCM_TOPIC_ALIASES.items():
        if alias_key in normalized:
            return canonical

    # Fallback using semantic keyword detection
    detected = detect_dhqg_hcm_topic(normalized)
    if detected in DHQG_HCM_TOPIC_RESPONSES:
        return detected
    return None


def register_dhqg_hcm_tools(mcp) -> None:
    @mcp.tool()
    async def dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_info(user_text: str = "") -> dict:
        logger.info(f"[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_info] request={{'user_text': {user_text!r}}}")
        question = user_text.strip() or "thông tin chi tiết về đại học quốc gia thành phố hồ chí minh"
        message = detect_dhqg_hcm_info(question)
        if not message:
            message = _naturalize_response(
                DHQG_HCM_TOPIC_RESPONSES["he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_day_du"],
                "dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_info",
            )
        response = {
            "success": True,
            "topic": "he_thong_dai_hoc_quoc_gia_thanh_pho_ho_chi_minh",
            "message": message,
        }
        logger.info(f"[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_info] response={response}")
        return response

    @mcp.tool()
    async def dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topics() -> dict:
        logger.info("[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topics] request={}")
        response = {
            "success": True,
            "topics": DHQG_HCM_TOPICS,
            "message": (
                "Dùng dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topic_detail(topic) "
                "hoặc dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_info(user_text) để lấy nội dung chi tiết."
            ),
        }
        logger.info(f"[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topics] response={response}")
        return response

    @mcp.tool()
    async def dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topic_detail(topic: str) -> dict:
        logger.info(f"[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topic_detail] request={{'topic': {topic!r}}}")
        resolved_topic = _resolve_dhqg_topic(topic)
        if not resolved_topic:
            response = {
                "success": False,
                "message": "Chủ đề không hợp lệ. Gọi dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topics để xem danh sách hỗ trợ.",
            }
            logger.info(f"[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topic_detail] response={response}")
            return response
        value = DHQG_HCM_TOPIC_RESPONSES[resolved_topic]
        response = {
            "success": True,
            "topic": resolved_topic,
            "message": _naturalize_response(value, resolved_topic),
        }
        logger.info(f"[Tool:dai_hoc_quoc_gia_thanh_pho_ho_chi_minh_topic_detail] response={response}")
        return response
