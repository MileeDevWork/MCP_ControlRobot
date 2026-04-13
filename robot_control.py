# robot_control.py - Phiên bản tối ưu cho mcp_pipe + Xiaozhi
import sys
import logging
import os
import httpx
import asyncio
from fastmcp import FastMCP
from typing import Optional

# Fix UTF-8 và buffering cho Windows + pipe
if sys.platform == 'win32':
    sys.stderr.reconfigure(encoding='utf-8')
    sys.stdout.reconfigure(encoding='utf-8', line_buffering=True)

# Logging rõ ràng
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger('RobotControl')

# Đọc IP từ .env
ROBOT_IP = os.getenv("ROBOT_IP")
ROBOT_URL = f"http://{ROBOT_IP}:9000/control"
TIMEOUT = 12.0

logger.info(f"RobotControl started with IP: {ROBOT_IP} | URL: {ROBOT_URL}")

mcp = FastMCP("RobotControl")

async def call_robot_api(payload: dict) -> dict:
    """Gọi API robot với logging chi tiết"""
    try:
        logger.info(f"Sending to robot: {payload}")
        async with httpx.AsyncClient(timeout=TIMEOUT) as client:
            response = await client.post(ROBOT_URL, json=payload, timeout=TIMEOUT)
            response.raise_for_status()
            result = response.json() if response.content else {"status": "ok"}

        logger.info(f"Robot success: {payload} -> {result}")
        return {
            "success": True,
            "message": f"Đã thực hiện lệnh {payload.get('command')} thành công",
            "robot_response": result
        }
    except Exception as e:
        logger.error(f"Robot API failed: {payload} | Error: {e}")
        return {
            "success": False,
            "error": f"Không kết nối được robot: {str(e)}"
        }

# ==================== TOOLS ====================

@mcp.tool()
async def reset_robot() -> dict:
    """Reset robot về trạng thái ban đầu."""
    return await call_robot_api({"command": "reset"})

@mcp.tool()
async def stand_up() -> dict:
    """Robot đứng dậy (Stand Up)."""
    return await call_robot_api({"command": "posture", "name": "Stand_Up"})

@mcp.tool()
async def sit_down() -> dict:
    """Robot ngồi xuống (Sit Down)."""
    return await call_robot_api({"command": "posture", "name": "Sit_Down"})

@mcp.tool()
async def hand_shake() -> dict:
    """Robot vẫy tay (Hand shake)."""
    return await call_robot_api({"command": "behavior", "name": "Handshake"})

@mcp.tool()
async def robot_control(command: str, name: Optional[str] = None) -> dict:
    """Điều khiển robot linh hoạt."""
    payload = {"command": command}
    if name:
        payload["name"] = name
    return await call_robot_api(payload)

if __name__ == "__main__":
    # Force flush output để mcp_pipe nhận được response
    sys.stdout.flush()
    mcp.run(transport="stdio")
