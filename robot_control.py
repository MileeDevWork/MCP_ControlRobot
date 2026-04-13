# robot_control.py - Phiên bản nâng cấp: có NLP + phản hồi tự nhiên

import sys
import logging
import os
import httpx
import asyncio
import random
from fastmcp import FastMCP
from typing import Optional

# Fix UTF-8 và buffering cho Windows + pipe
if sys.platform == 'win32':
    sys.stderr.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

# Logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RobotControl')

# ENV config
ROBOT_IP = os.getenv("ROBOT_IP")
ROBOT_URL = f"http://{ROBOT_IP}:9000/control"
TIMEOUT = 12.0

logger.info(f"RobotControl started with IP: {ROBOT_IP} | URL: {ROBOT_URL}")

mcp = FastMCP("RobotControl")

# ==================== API CALL ====================

async def call_robot_api(payload: dict) -> dict:
    try:
        logger.info(f"Sending to robot: {payload}")
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.post(ROBOT_URL, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            result = response.json() if response.content else {"status": "ok"}

        logger.info(f"Robot success: {payload} -> {result}")
        return {
            "success": True,
            "robot_response": result
        }

    except Exception as e:
        logger.error(f"Robot API failed: {payload} | Error: {e}")
        return {
            "success": False,
            "error": f"Không kết nối được robot: {str(e)}"
        }

# ==================== INTENT DATA ====================

INTENT_MAP = {
    "stand_up": {
        "keywords": [
            "đứng lên", "đứng dậy", "dậy đi", "dậy nào", "đứng lên đi",
            "bạn đứng lên", "robot đứng lên", "làm ơn đứng dậy",
            "stand up", "get up", "rise up"
        ],
        "responses": [
            "Mình đứng dậy đây.",
            "Đang thực hiện đứng lên.",
            "Ok, mình đứng lên ngay.",
            "Đứng dậy theo yêu cầu.",
            "Robot bắt đầu đứng dậy."
        ]
    },

    "sit_down": {
        "keywords": [
            "ngồi xuống", "ngồi đi", "ngồi lại", "ngồi xuống đi",
            "bạn ngồi xuống", "robot ngồi xuống",
            "sit down", "take a seat"
        ],
        "responses": [
            "Mình ngồi xuống rồi.",
            "Đang ngồi xuống.",
            "Ok, mình ngồi lại.",
            "Đã chuyển sang tư thế ngồi."
        ]
    },

    "hand_shake": {
        "keywords": [
            "bắt tay", "bắt tay nào", "hello", "xin chào", "chào",
            "hi", "hey", "chào bạn", "chào robot",
            "greeting", "say hello"
        ],
        "responses": [
            "Xin chào, rất vui được gặp bạn.",
            "Hello, mình ở đây.",
            "Chào bạn, sẵn sàng hỗ trợ.",
            "Rất vui được tương tác với bạn."
        ]
    },
    "reset": {
        "keywords": ["reset", "khởi động lại", "làm mới"],
        "responses": [
            "Đang reset hệ thống.",
            "Khởi động lại robot.",
            "Đang làm mới trạng thái."
        ]
    }
}

SMALL_TALK = {
    "bạn là ai": "Mình là robot hỗ trợ điều khiển và giao tiếp.",
    "bạn khỏe không": "Mình luôn sẵn sàng hoạt động.",
    "cảm ơn": "Rất vui được giúp bạn.",
    "hello": "Xin chào bạn."
}

# ==================== NLP ====================

def detect_small_talk(text: str):
    text = text.lower()
    for key, value in SMALL_TALK.items():
        if key in text:
            return value
    return None

def detect_intent(text: str):
    text = text.lower()

    for intent, data in INTENT_MAP.items():
        for keyword in data["keywords"]:
            if keyword in text:
                response = random.choice(data["responses"])
                return intent, response

    return None, "Mình chưa hiểu yêu cầu của bạn."

# ==================== BASIC TOOLS ====================

@mcp.tool()
async def reset_robot() -> dict:
    return await call_robot_api({"command": "reset"})

@mcp.tool()
async def stand_up() -> dict:
    return await call_robot_api({"command": "posture", "name": "Stand_Up"})

@mcp.tool()
async def sit_down() -> dict:
    return await call_robot_api({"command": "posture", "name": "Sit_Down"})

@mcp.tool()
async def hand_shake() -> dict:
    return await call_robot_api({"command": "behavior", "name": "Handshake"})

@mcp.tool()
async def wave_hand() -> dict:
    return await call_robot_api({"command": "behavior", "name": "Wave_Hand"})

@mcp.tool()
async def wave_body() -> dict:
    return await call_robot_api({"command": "behavior", "name": "Wave_Body"})

@mcp.tool()
async def stretch() -> dict:
    return await call_robot_api({"command": "behavior", "name": "Stretch"})

@mcp.tool()
async def axis() -> dict:
    return await call_robot_api({"command": "behavior", "name": "3_Axis"})

@mcp.tool()
async def robot_control(command: str, name: Optional[str] = None) -> dict:
    payload = {"command": command}
    if name:
        payload["name"] = name
    return await call_robot_api(payload)


# ==================== SMART CONTROL ====================

@mcp.tool()
async def smart_control(user_text: str) -> dict:
    """Xử lý câu nói tự nhiên từ user"""

    # 1. Small talk
    small_talk = detect_small_talk(user_text)
    if small_talk:
        return {
            "success": True,
            "message": small_talk
        }

    # 2. Intent detection
    intent, response_text = detect_intent(user_text)

    if not intent:
        return {
            "success": False,
            "message": response_text
        }

    # 3. Execute action
    if intent == "stand_up":
        result = await stand_up()
    elif intent == "sit_down":
        result = await sit_down()
    elif intent == "hand_shake":
        result = await hand_shake()
    elif intent == "reset":
        result = await reset_robot()
    else:
        return {
            "success": False,
            "message": "Lệnh chưa được hỗ trợ."
        }

    # 4. Return natural response
    return {
        "success": result.get("success", False),
        "message": response_text,
        "robot_response": result
    }

# ==================== MAIN ====================

if __name__ == "__main__":
    sys.stdout.flush()
    mcp.run(transport="stdio")
