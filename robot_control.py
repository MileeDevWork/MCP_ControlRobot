# robot_control.py
# MCP Server điều khiển robot với NLP + phản hồi tự nhiên
# Hỗ trợ:
# - Gọi lệnh robot trực tiếp qua các tool MCP
# - Hiểu câu lệnh ngôn ngữ tự nhiên qua smart_control
# - Trả về phản hồi thân thiện, rõ ràng và đồng bộ giữa các hành động

import sys
import logging
import os
import httpx
import random
from typing import Optional
from fastmcp import FastMCP

# ==================== SYSTEM SETUP ====================

# Fix UTF-8 và buffering cho Windows / pipe
if sys.platform == "win32":
    sys.stderr.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RobotControl")

# ==================== ENV CONFIG ====================

ROBOT_IP = os.getenv("ROBOT_IP")
TIMEOUT = 12.0

if not ROBOT_IP:
    logger.warning("ROBOT_IP chưa được thiết lập trong biến môi trường.")

ROBOT_URL = f"http://{ROBOT_IP}:9000/control" if ROBOT_IP else None

logger.info(f"RobotControl started | ROBOT_IP={ROBOT_IP} | ROBOT_URL={ROBOT_URL}")

# ==================== MCP SERVER ====================

mcp = FastMCP("RobotControl")

# ==================== ROBOT API CLIENT ====================

async def call_robot_api(payload: dict) -> dict:
    """
    Gửi lệnh điều khiển tới robot thông qua HTTP API.

    Args:
        payload: Dữ liệu lệnh gửi tới robot.
                 Ví dụ:
                 {"command": "posture", "name": "Stand_Up"}
                 {"command": "behavior", "name": "Wave_Hand"}

    Returns:
        dict: Kết quả thực thi từ robot hoặc lỗi nếu gọi API thất bại.
    """
    if not ROBOT_URL:
        logger.error("ROBOT_URL không hợp lệ vì thiếu ROBOT_IP.")
        return {
            "success": False,
            "error": "Chưa cấu hình ROBOT_IP. Vui lòng kiểm tra biến môi trường."
        }

    try:
        logger.info(f"[Robot] Sending command: {payload}")

        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.post(ROBOT_URL, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            result = response.json() if response.content else {"status": "ok"}

        logger.info(f"[Robot] Success -> {result}")

        return {
            "success": True,
            "robot_response": result
        }

    except Exception as e:
        logger.error(f"[Robot] Failed -> payload={payload} | error={e}")
        return {
            "success": False,
            "error": f"Không thể kết nối tới robot. Vui lòng kiểm tra lại hệ thống. ({str(e)})"
        }

# ==================== TEXT HELPERS ====================

def normalize_text(text: str) -> str:
    """
    Chuẩn hóa văn bản đầu vào để dễ matching hơn.
    """
    return text.strip().lower()

# ==================== SMALL TALK ====================

SMALL_TALK = {
    "bạn là ai": "Mình là robot hỗ trợ điều khiển và giao tiếp.",
    "bạn khỏe không": "Mình luôn sẵn sàng hoạt động.",
    "cảm ơn": "Rất vui được giúp bạn.",
    "hello": "Xin chào bạn.",
    "hi": "Chào bạn, mình đang lắng nghe đây.",
    "xin chào": "Xin chào, rất vui được gặp bạn."
}

def detect_small_talk(text: str) -> Optional[str]:
    """
    Nhận diện các câu giao tiếp cơ bản như chào hỏi, cảm ơn, hỏi thăm.
    """
    normalized = normalize_text(text)

    for key, value in SMALL_TALK.items():
        if key in normalized:
            return value

    return None

# ==================== INTENT DEFINITIONS ====================

# Mapping giữa câu nói tự nhiên -> hành động robot + phản hồi tự nhiên
INTENT_MAP = {
    "stand_up": {
        "keywords": [
            "đứng lên", "đứng dậy", "dậy đi", "dậy nào", "đứng lên đi",
            "bạn đứng lên", "robot đứng lên", "làm ơn đứng dậy",
            "stand up", "get up", "rise up"
        ],
        "responses": [
            "Mình đứng dậy đây.",
            "Đang thực hiện thao tác đứng lên.",
            "Ok, mình đứng lên ngay.",
            "Đã nhận lệnh, mình đang đứng dậy.",
            "Robot bắt đầu chuyển sang tư thế đứng."
        ]
    },

    "sit_down": {
        "keywords": [
            "ngồi xuống", "ngồi đi", "ngồi lại", "ngồi xuống đi",
            "bạn ngồi xuống", "robot ngồi xuống",
            "sit down", "take a seat"
        ],
        "responses": [
            "Mình ngồi xuống nhé.",
            "Đang thực hiện thao tác ngồi xuống.",
            "Ok, mình ngồi lại đây.",
            "Đã chuyển sang tư thế ngồi.",
            "Robot đang hạ về tư thế ngồi."
        ]
    },

    "hand_shake": {
        "keywords": [
            "bắt tay", "bắt tay nào", "bắt tay với tôi",
            "handshake", "shake hand", "shake hands"
        ],
        "responses": [
            "Mình bắt tay với bạn đây.",
            "Đang thực hiện thao tác bắt tay.",
            "Rất vui được bắt tay với bạn.",
            "Ok, mình đang chào hỏi bằng bắt tay."
        ]
    },

    "wave_hand": {
        "keywords": [
            "vẫy tay", "chào bằng tay", "giơ tay chào",
            "wave hand", "wave hello", "say hi with hand"
        ],
        "responses": [
            "Mình đang vẫy tay chào bạn.",
            "Ok, mình vẫy tay đây.",
            "Đang thực hiện động tác vẫy tay.",
            "Robot đang chào bạn bằng cử chỉ tay."
        ]
    },

    "wave_body": {
        "keywords": [
            "lắc người", "vẫy người", "đu đưa", "nghiêng người",
            "wave body", "move body", "body wave"
        ],
        "responses": [
            "Mình đang thực hiện chuyển động cơ thể.",
            "Ok, robot bắt đầu lắc người.",
            "Đang thực hiện động tác wave body.",
            "Robot đang chuyển động thân người theo yêu cầu."
        ]
    },

    "stretch": {
        "keywords": [
            "vươn vai", "giãn cơ", "duỗi người",
            "stretch", "stretch body"
        ],
        "responses": [
            "Mình đang thực hiện động tác giãn cơ.",
            "Ok, robot bắt đầu vươn vai.",
            "Đang thực hiện thao tác stretch.",
            "Robot đang duỗi cơ thể."
        ]
    },

    "axis": {
        "keywords": [
            "3 trục", "ba trục", "xoay 3 trục", "chuyển động 3 trục",
            "axis", "3 axis", "three axis"
        ],
        "responses": [
            "Mình đang thực hiện chuyển động 3 trục.",
            "Ok, robot bắt đầu chạy chế độ axis.",
            "Đang thực hiện hành vi 3 axis.",
            "Robot đang chuyển động theo chế độ ba trục."
        ]
    },

    "reset": {
        "keywords": [
            "reset", "khởi động lại", "làm mới", "đặt lại",
            "restart", "reboot"
        ],
        "responses": [
            "Đang reset hệ thống.",
            "Mình sẽ khởi động lại robot.",
            "Đang làm mới trạng thái hoạt động.",
            "Robot đang được đưa về trạng thái ban đầu."
        ]
    }
}

def detect_intent(text: str):
    """
    Phân tích câu nói của người dùng để xác định intent chính.

    Returns:
        tuple:
            - intent (str | None): hành động được nhận diện
            - response (str): phản hồi tự nhiên tương ứng
    """
    normalized = normalize_text(text)

    for intent, data in INTENT_MAP.items():
        for keyword in data["keywords"]:
            if keyword in normalized:
                response = random.choice(data["responses"])
                return intent, response

    return None, "Mình chưa hiểu rõ yêu cầu của bạn. Bạn có thể nói cụ thể hơn không?"

# ==================== BASIC ROBOT TOOLS ====================

@mcp.tool()
async def reset_robot() -> dict:
    """
    Đưa robot về trạng thái reset.
    """
    return await call_robot_api({"command": "reset"})


@mcp.tool()
async def stand_up() -> dict:
    """
    Điều khiển robot đứng lên.
    """
    return await call_robot_api({"command": "posture", "name": "Stand_Up"})


@mcp.tool()
async def sit_down() -> dict:
    """
    Điều khiển robot ngồi xuống.
    """
    return await call_robot_api({"command": "posture", "name": "Sit_Down"})


@mcp.tool()
async def hand_shake() -> dict:
    """
    Điều khiển robot thực hiện hành động bắt tay.
    """
    return await call_robot_api({"command": "behavior", "name": "Handshake"})


@mcp.tool()
async def wave_hand() -> dict:
    """
    Điều khiển robot thực hiện động tác vẫy tay.
    """
    return await call_robot_api({"command": "behavior", "name": "Wave_Hand"})


@mcp.tool()
async def wave_body() -> dict:
    """
    Điều khiển robot thực hiện chuyển động thân người.
    """
    return await call_robot_api({"command": "behavior", "name": "Wave_Body"})


@mcp.tool()
async def stretch() -> dict:
    """
    Điều khiển robot thực hiện động tác giãn cơ / vươn vai.
    """
    return await call_robot_api({"command": "behavior", "name": "Stretch"})


@mcp.tool()
async def axis() -> dict:
    """
    Điều khiển robot thực hiện chuyển động 3 trục.
    """
    return await call_robot_api({"command": "behavior", "name": "3_Axis"})


@mcp.tool()
async def robot_control(command: str, name: Optional[str] = None) -> dict:
    """
    Tool điều khiển robot tổng quát.

    Args:
        command: Loại lệnh, ví dụ "posture", "behavior", "reset"
        name: Tên hành động cụ thể nếu có
    """
    payload = {"command": command}
    if name:
        payload["name"] = name
    return await call_robot_api(payload)

# ==================== ACTION EXECUTOR ====================

async def execute_intent(intent: str) -> dict:
    """
    Thực thi action tương ứng với intent đã nhận diện.
    """
    action_map = {
        "stand_up": stand_up,
        "sit_down": sit_down,
        "hand_shake": hand_shake,
        "wave_hand": wave_hand,
        "wave_body": wave_body,
        "stretch": stretch,
        "axis": axis,
        "reset": reset_robot,
    }

    action = action_map.get(intent)
    if not action:
        return {
            "success": False,
            "message": "Hiện tại mình chưa hỗ trợ hành động này."
        }

    return await action()

# ==================== SMART NATURAL CONTROL ====================

@mcp.tool()
async def smart_control(user_text: str) -> dict:
    """
    Xử lý câu lệnh tự nhiên từ người dùng.

    Luồng xử lý:
    1. Kiểm tra small talk (chào hỏi, cảm ơn, hỏi thăm...)
    2. Nhận diện intent chính từ câu nói
    3. Gọi hành động robot tương ứng
    4. Trả về phản hồi tự nhiên + kết quả thực thi

    Ví dụ:
        - "Robot đứng lên đi"
        - "Ngồi xuống nhé"
        - "Vẫy tay chào mình đi"
        - "Lắc người đi"
        - "Cho robot xoay 3 trục"
        - "Reset hệ thống"
    """
    logger.info(f"[SmartControl] User text: {user_text}")

    # 1. Xử lý hội thoại đơn giản
    small_talk = detect_small_talk(user_text)
    if small_talk:
        logger.info(f"[SmartControl] Small talk detected -> {small_talk}")
        return {
            "success": True,
            "message": small_talk
        }

    # 2. Nhận diện ý định
    intent, response_text = detect_intent(user_text)
    logger.info(f"[SmartControl] Intent detected -> {intent}")

    if not intent:
        return {
            "success": False,
            "message": response_text
        }

    # 3. Thực thi hành động
    result = await execute_intent(intent)

    # 4. Trả về phản hồi đồng bộ
    return {
        "success": result.get("success", False),
        "message": response_text if result.get("success") else result.get("message", "Thực thi thất bại."),
        "robot_response": result
    }

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    logger.info("MCP Robot Control server is running...")
    sys.stdout.flush()
    mcp.run(transport="stdio")