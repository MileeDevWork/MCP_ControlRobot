"""
Centralized MCP server initialization for RobotControl.
"""

import logging
import sys

from fastmcp import FastMCP
from app.app_config import load_config
from app.services.hcmut_mcp import register_hcmut_tools
from app.services.dhqg_hcm_mcp import register_dhqg_hcm_tools
from app.services.robot_ivs_mcp import register_robot_ivs_tools
from app.services.robot_control import register_robot_control_tools


def _is_service_enabled(cfg: dict, service_key: str) -> bool:
    key_map = {
        "hcmut": "hcmut",
        "dhqg_hcm": "dhqg_hcm",
        "robot_control": "robot",
        "robot_ivs": "robot_ivs",
    }
    target_key = key_map.get(service_key)
    if not target_key:
        return True
    return bool((cfg.get(target_key) or {}).get("enabled", True))


def create_robot_mcp_server() -> FastMCP:
    """
    Create and configure the MCP server instance used by RobotControl.
    """
    cfg = load_config()
    mcp = FastMCP("RobotControl")

    if _is_service_enabled(cfg, "hcmut"):
        register_hcmut_tools(mcp)
    else:
        logging.getLogger("RobotControl").info("Service disabled: hcmut")

    if _is_service_enabled(cfg, "dhqg_hcm"):
        register_dhqg_hcm_tools(mcp)
    else:
        logging.getLogger("RobotControl").info("Service disabled: dhqg_hcm")

    if _is_service_enabled(cfg, "robot_control"):
        register_robot_control_tools(mcp)
    else:
        logging.getLogger("RobotControl").info("Service disabled: robot_control")

    if _is_service_enabled(cfg, "robot_ivs"):
        register_robot_ivs_tools(mcp)
    else:
        logging.getLogger("RobotControl").info("Service disabled: robot_ivs")

    return mcp


mcp = create_robot_mcp_server()


if __name__ == "__main__":
    if sys.platform == "win32":
        sys.stderr.reconfigure(encoding="utf-8")
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

    logging.getLogger("RobotControl").info("MCP Robot Control server is running...")
    sys.stdout.flush()
    mcp.run(transport="stdio")
