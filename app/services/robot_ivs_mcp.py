"""
MCP tools for RobotIVS control.
"""

import os
import logging
import httpx

from app.app_config import load_config


logger = logging.getLogger("RobotControl.RobotIVS")

CONFIG = load_config()
ROBOT_IVS_CFG = CONFIG.get("robot_ivs") or {}
ROBOT_IVS_IP = os.getenv("ROBOT_IVS_IP") or str(ROBOT_IVS_CFG.get("ip") or "")
ROBOT_IVS_PORT = int(ROBOT_IVS_CFG.get("port", 8000))
ROBOT_IVS_BASE_PATH = str(ROBOT_IVS_CFG.get("base_path", "/robot")).rstrip("/")
ROBOT_IVS_TIMEOUT = float(ROBOT_IVS_CFG.get("timeout_seconds", 12.0))
ROBOT_IVS_BASE_URL = (
    f"http://{ROBOT_IVS_IP}:{ROBOT_IVS_PORT}{ROBOT_IVS_BASE_PATH}" if ROBOT_IVS_IP else None
)

if not ROBOT_IVS_IP:
    logger.warning("ROBOT_IVS_IP chưa được thiết lập trong biến môi trường hoặc config.")

logger.info(
    f"RobotIVS started | ROBOT_IVS_IP={ROBOT_IVS_IP} | ROBOT_IVS_BASE_URL={ROBOT_IVS_BASE_URL}"
)

ROBOT_IVS_ALLOWED_ACTIONS = {"stand", "sit", "stop", "wave"}


async def call_robot_ivs_api(action: str) -> dict:
    """
    POST http://<robot-ip>:8000/robot/[sit,stand,stop,wave]
    """
    normalized = action.strip().lower()
    if normalized not in ROBOT_IVS_ALLOWED_ACTIONS:
        return {
            "success": False,
            "error": f"Hành động không hợp lệ. Hỗ trợ: {sorted(ROBOT_IVS_ALLOWED_ACTIONS)}"
        }
    if not ROBOT_IVS_BASE_URL:
        return {
            "success": False,
            "error": "Chưa cấu hình robot_ivs.ip hoặc ROBOT_IVS_IP."
        }

    endpoint = f"{ROBOT_IVS_BASE_URL}/{normalized}"
    try:
        logger.info(f"[RobotIVS] POST {endpoint}")
        async with httpx.AsyncClient(timeout=ROBOT_IVS_TIMEOUT) as client:
            response = await client.post(endpoint, timeout=ROBOT_IVS_TIMEOUT)
            response.raise_for_status()
            if response.content:
                data = response.json()
                if isinstance(data, dict):
                    return data
                return {"success": True, "data": data}
            return {"success": True, "action": normalized}
    except Exception as e:
        logger.error(f"[RobotIVS] Failed -> action={normalized} | error={e}")
        return {
            "success": False,
            "action": normalized,
            "error": f"Không thể gọi robotIVS. ({str(e)})"
        }


def register_robot_ivs_tools(mcp) -> None:
    async def _call_robot_ivs_and_log(tool_name: str, action: str) -> dict:
        request_payload = {"action": action}
        logger.info(f"[Tool:{tool_name}] request={request_payload}")
        response = await call_robot_ivs_api(action)
        logger.info(f"[Tool:{tool_name}] response={response}")
        return response

    @mcp.tool()
    async def robotivs_stand() -> dict:
        return await _call_robot_ivs_and_log("robotivs_stand", "stand")

    @mcp.tool()
    async def robotivs_sit() -> dict:
        return await _call_robot_ivs_and_log("robotivs_sit", "sit")

    @mcp.tool()
    async def robotivs_stop() -> dict:
        return await _call_robot_ivs_and_log("robotivs_stop", "stop")

    @mcp.tool()
    async def robotivs_wave() -> dict:
        return await _call_robot_ivs_and_log("robotivs_wave", "wave")

    @mcp.tool()
    async def robotivs_control(action: str) -> dict:
        return await _call_robot_ivs_and_log("robotivs_control", action)
