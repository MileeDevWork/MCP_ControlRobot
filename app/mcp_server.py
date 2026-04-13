"""
Centralized MCP server initialization for RobotControl.
"""

import logging
import sys

from fastmcp import FastMCP
from app.services.hcmut_mcp import register_hcmut_tools
from app.services.dhqg_hcm_mcp import register_dhqg_hcm_tools
from app.services.robot_control import register_robot_control_tools


def create_robot_mcp_server() -> FastMCP:
    """
    Create and configure the MCP server instance used by RobotControl.
    """
    mcp = FastMCP("RobotControl")
    register_hcmut_tools(mcp)
    register_dhqg_hcm_tools(mcp)
    register_robot_control_tools(mcp)
    return mcp


mcp = create_robot_mcp_server()


if __name__ == "__main__":
    if sys.platform == "win32":
        sys.stderr.reconfigure(encoding="utf-8")
        sys.stdout.reconfigure(encoding="utf-8", line_buffering=True)

    logging.getLogger("RobotControl").info("MCP Robot Control server is running...")
    sys.stdout.flush()
    mcp.run(transport="stdio")
