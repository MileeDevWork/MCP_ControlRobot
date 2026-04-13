# robot_control.py
# MCP Server điều khiển robot với NLP + phản hồi tự nhiên
# Hỗ trợ:
# - Gọi lệnh robot trực tiếp qua các tool MCP
# - Hiểu câu lệnh ngôn ngữ tự nhiên qua smart_control
# - Trả về phản hồi thân thiện, rõ ràng và đồng bộ giữa các hành động

import sys
import logging
import os
import asyncio
import httpx
import random
from typing import Optional
from app.services.hcmut_mcp import detect_hcmut_info
from app.services.dhqg_hcm_mcp import detect_dhqg_hcm_info
from app.app_config import load_config

# ==================== SYSTEM SETUP ====================

# Fix UTF-8 và buffering cho Windows / pipe
if sys.platform == "win32":
    sys.stderr.reconfigure(encoding="utf-8")
    sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

CONFIG = load_config()
LOG_LEVEL = str((CONFIG.get("runtime") or {}).get("log_level", "INFO")).upper()

logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger("RobotControl")

# ==================== ENV CONFIG ====================

robot_cfg = CONFIG.get("robot") or {}
ROBOT_IP = os.getenv("ROBOT_IP") or str(robot_cfg.get("ip") or "")
ROBOT_PORT = int(robot_cfg.get("port", 9000))
CONTROL_PATH = str(robot_cfg.get("control_path", "/control"))
TIMEOUT = float(robot_cfg.get("timeout_seconds", 12.0))

if not ROBOT_IP:
    logger.warning("ROBOT_IP chưa được thiết lập trong biến môi trường.")

ROBOT_URL = f"http://{ROBOT_IP}:{ROBOT_PORT}{CONTROL_PATH}" if ROBOT_IP else None

logger.info(f"RobotControl started | ROBOT_IP={ROBOT_IP} | ROBOT_URL={ROBOT_URL}")

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

ALLOWED_SIMPLE_COMMANDS = {"reset", "rotation"}
ALLOWED_POSTURE_NAMES = {"Lie_Down", "Stand_Up", "Crawl", "Squat", "Sit_Down"}
ALLOWED_BEHAVIOR_NAMES = {
    "Turn_Around", "Mark_Time", "Turn_Roll", "Turn_Pitch", "Turn_Yaw", "3_Axis",
    "Pee", "Wave_Hand", "Stretch", "Wave_Body", "Swing", "Pray", "Seek",
    "Handshake", "Play_Ball"
}

INFO_RANDOM_ACTIONS = [
    {"command": "behavior", "name": "Wave_Hand"},
    {"command": "behavior", "name": "Wave_Body"},
    {"command": "behavior", "name": "Stretch"},
    {"command": "behavior", "name": "Mark_Time"},
    {"command": "behavior", "name": "Turn_Around"},
    {"command": "behavior", "name": "Swing"},
]


def register_robot_control_tools(mcp) -> None:
    """
    Đăng ký toàn bộ tool điều khiển robot vào MCP server.
    """

    async def _call_robot_and_log(tool_name: str, api_payload: dict, input_payload: Optional[dict] = None) -> dict:
        request_payload = input_payload if input_payload is not None else api_payload
        logger.info(f"[Tool:{tool_name}] request={request_payload}")
        response = await call_robot_api(api_payload)
        logger.info(f"[Tool:{tool_name}] response={response}")
        return response

    def _trigger_random_info_action(source: str) -> dict:
        """
        Lên lịch một hành động ngẫu nhiên để robot thực hiện song song khi trả lời thông tin.
        """
        action_payload = random.choice(INFO_RANDOM_ACTIONS)
        logger.info(f"[InfoAction] scheduled from={source} action={action_payload}")
        asyncio.create_task(
            _call_robot_and_log(
                "info_random_action",
                action_payload,
                {"source": source, "action": action_payload},
            )
        )
        return action_payload

    @mcp.tool()
    async def reset_robot() -> dict:
        return await _call_robot_and_log("reset_robot", {"command": "reset"})

    @mcp.tool()
    async def rotation_robot() -> dict:
        return await _call_robot_and_log("rotation_robot", {"command": "rotation"})

    @mcp.tool()
    async def lie_down() -> dict:
        return await _call_robot_and_log("lie_down", {"command": "posture", "name": "Lie_Down"})

    @mcp.tool()
    async def stand_up() -> dict:
        return await _call_robot_and_log("stand_up", {"command": "posture", "name": "Stand_Up"})

    @mcp.tool()
    async def crawl() -> dict:
        return await _call_robot_and_log("crawl", {"command": "posture", "name": "Crawl"})

    @mcp.tool()
    async def squat() -> dict:
        return await _call_robot_and_log("squat", {"command": "posture", "name": "Squat"})

    @mcp.tool()
    async def sit_down() -> dict:
        return await _call_robot_and_log("sit_down", {"command": "posture", "name": "Sit_Down"})

    @mcp.tool()
    async def hand_shake() -> dict:
        return await _call_robot_and_log("hand_shake", {"command": "behavior", "name": "Handshake"})

    @mcp.tool()
    async def wave_hand() -> dict:
        return await _call_robot_and_log("wave_hand", {"command": "behavior", "name": "Wave_Hand"})

    @mcp.tool()
    async def wave_body() -> dict:
        return await _call_robot_and_log("wave_body", {"command": "behavior", "name": "Wave_Body"})

    @mcp.tool()
    async def stretch() -> dict:
        return await _call_robot_and_log("stretch", {"command": "behavior", "name": "Stretch"})

    @mcp.tool()
    async def axis() -> dict:
        return await _call_robot_and_log("axis", {"command": "behavior", "name": "3_Axis"})

    @mcp.tool()
    async def robot_control(command: str, name: Optional[str] = None) -> dict:
        logger.info(f"[Tool:robot_control] request={{'command': {command!r}, 'name': {name!r}}}")
        cmd = command.strip()
        cmd_lower = cmd.lower()

        if cmd_lower in ALLOWED_SIMPLE_COMMANDS:
            return await _call_robot_and_log(
                "robot_control",
                {"command": cmd_lower},
                {"command": command, "name": name},
            )

        if cmd_lower == "posture":
            if not name:
                response = {
                    "success": False,
                    "error": "Lệnh posture cần tham số name."
                }
                logger.info(f"[Tool:robot_control] response={response}")
                return response
            if name not in ALLOWED_POSTURE_NAMES:
                response = {
                    "success": False,
                    "error": f"name không hợp lệ cho posture. Hỗ trợ: {sorted(ALLOWED_POSTURE_NAMES)}"
                }
                logger.info(f"[Tool:robot_control] response={response}")
                return response
            return await _call_robot_and_log(
                "robot_control",
                {"command": "posture", "name": name},
                {"command": command, "name": name},
            )

        if cmd_lower == "behavior":
            if not name:
                response = {
                    "success": False,
                    "error": "Lệnh behavior cần tham số name."
                }
                logger.info(f"[Tool:robot_control] response={response}")
                return response
            if name not in ALLOWED_BEHAVIOR_NAMES:
                response = {
                    "success": False,
                    "error": f"name không hợp lệ cho behavior. Hỗ trợ: {sorted(ALLOWED_BEHAVIOR_NAMES)}"
                }
                logger.info(f"[Tool:robot_control] response={response}")
                return response
            return await _call_robot_and_log(
                "robot_control",
                {"command": "behavior", "name": name},
                {"command": command, "name": name},
            )

        response = {
            "success": False,
            "error": "command không hợp lệ. Hỗ trợ: reset, rotation, posture, behavior."
        }
        logger.info(f"[Tool:robot_control] response={response}")
        return response

    async def execute_intent(intent: str) -> dict:
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

    @mcp.tool()
    async def smart_control(user_text: str) -> dict:
        """
        Xử lý câu lệnh tự nhiên từ người dùng.
        """
        logger.info(f"[Tool:smart_control] request={{'user_text': {user_text!r}}}")

        small_talk = detect_small_talk(user_text)
        if small_talk:
            response = {
                "success": True,
                "message": small_talk
            }
            logger.info(f"[Tool:smart_control] response={response}")
            return response

        hcmut_info = detect_hcmut_info(user_text)
        if hcmut_info:
            action_payload = _trigger_random_info_action("hcmut_info")
            response = {
                "success": True,
                "message": hcmut_info,
                "robot_action_scheduled": action_payload,
            }
            logger.info(f"[Tool:smart_control] response={response}")
            return response

        dhqg_hcm_info = detect_dhqg_hcm_info(user_text)
        if dhqg_hcm_info:
            action_payload = _trigger_random_info_action("dhqg_hcm_info")
            response = {
                "success": True,
                "message": dhqg_hcm_info,
                "robot_action_scheduled": action_payload,
            }
            logger.info(f"[Tool:smart_control] response={response}")
            return response

        intent, response_text = detect_intent(user_text)
        logger.info(f"[SmartControl] intent={intent}")

        if not intent:
            response = {
                "success": False,
                "message": response_text
            }
            logger.info(f"[Tool:smart_control] response={response}")
            return response

        result = await execute_intent(intent)
        response = {
            "success": result.get("success", False),
            "message": response_text if result.get("success") else result.get("message", "Thực thi thất bại."),
            "robot_response": result
        }
        logger.info(f"[Tool:smart_control] response={response}")
        return response

# ==================== ENTRY POINT ====================

if __name__ == "__main__":
    from app.mcp_server import mcp

    logger.info("MCP Robot Control server is running...")
    sys.stdout.flush()
    mcp.run(transport="stdio")
