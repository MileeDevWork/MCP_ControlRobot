# MCP Control Robot

MCP server để:
- Điều khiển robot qua HTTP API `/control`
- Trả lời thông tin về Trường Đại học Bách khoa Thành phố Hồ Chí Minh
- Kết nối endpoint từ xa qua WebSocket bridge

## 1. Kiến trúc

Luồng chạy:
1. `app.mcp_pipe` đọc `config/config.yaml` để lấy `mcp.endpoint`.
2. `app.mcp_pipe` đọc `config/mcp_config.json` để biết server local cần chạy.
3. Server local là `app.mcp_server` (transport `stdio`).
4. `app.mcp_server` khởi tạo `FastMCP("RobotControl")`, đăng ký:
- Tool HCMUT từ `app/services/hcmut_mcp.py`
- Tool robot từ `app/services/robot_control.py`
5. Tool robot gọi API: `http://<robot_ip>:<port>/control`

## 2. Cấu trúc thư mục

- `app/mcp_pipe.py`: WebSocket <-> stdio bridge
- `app/mcp_server.py`: MCP server entrypoint
- `app/app_config.py`: load cấu hình YAML
- `app/services/hcmut_mcp.py`: tool thông tin trường
- `app/services/robot_control.py`: tool điều khiển robot
- `config/config.yaml`: cấu hình runtime, endpoint, robot
- `config/mcp_config.json`: cấu hình danh sách MCP servers
- `requirements.txt`: dependencies

## 3. Cài đặt

```bash
pip install -r requirements.txt
```

## 4. Cấu hình

### 4.1 `config/config.yaml`

```yaml
runtime:
  log_level: INFO

mcp:
  endpoint: "wss://<your-endpoint>/mcp/?token=<your-token>"

robot:
  enabled: true
  ip: "192.168.1.10"
  port: 9000
  control_path: /control
  timeout_seconds: 12.0

robot_ivs:
  enabled: true
  ip: "10.254.131.62"
  port: 8000
  base_path: /robot
  timeout_seconds: 12.0

hcmut:
  enabled: true

dhqg_hcm:
  enabled: true
```

Ghi chú:
- `ROBOT_IP` (env) nếu có sẽ override `robot.ip` trong YAML.
- `ROBOT_IVS_IP` (env) nếu có sẽ override `robot_ivs.ip` trong YAML.
- Mỗi service chỉ dùng một cờ `enabled` ở top-level, tắt là tắt toàn bộ service.
- Nếu không có `mcp.endpoint`, hệ thống sẽ không chạy.

### 4.2 `config/mcp_config.json`

```json
{
  "mcpServers": {
    "robot-control": {
      "type": "stdio",
      "command": "python",
      "args": ["-m", "app.mcp_server"]
    }
  }
}
```

## 5. Chạy hệ thống

```bash
python -m app.mcp_pipe
```

Windows PowerShell:
```powershell
py -3.12 -m app.mcp_pipe
```

Chạy một server theo tên trong `config/mcp_config.json`:
```bash
python -m app.mcp_pipe robot-control
```

## 6. Robot API hỗ trợ

Endpoint:
- `POST http://<robot_ip>:<port>/control`

Payload hợp lệ:

Lệnh đơn:
```json
{"command":"reset"}
{"command":"rotation"}
```

Lệnh posture:
```json
{"command":"posture","name":"Lie_Down"}
{"command":"posture","name":"Stand_Up"}
{"command":"posture","name":"Crawl"}
{"command":"posture","name":"Squat"}
{"command":"posture","name":"Sit_Down"}
```

Lệnh behavior:
```json
{"command":"behavior","name":"Turn_Around"}
{"command":"behavior","name":"Mark_Time"}
{"command":"behavior","name":"Turn_Roll"}
{"command":"behavior","name":"Turn_Pitch"}
{"command":"behavior","name":"Turn_Yaw"}
{"command":"behavior","name":"3_Axis"}
{"command":"behavior","name":"Pee"}
{"command":"behavior","name":"Wave_Hand"}
{"command":"behavior","name":"Stretch"}
{"command":"behavior","name":"Wave_Body"}
{"command":"behavior","name":"Swing"}
{"command":"behavior","name":"Pray"}
{"command":"behavior","name":"Seek"}
{"command":"behavior","name":"Handshake"}
{"command":"behavior","name":"Play_Ball"}
```

## 7. RobotIVS API hỗ trợ

Endpoint:
- `POST http://<robot-ip>:8000/robot/stand`
- `POST http://<robot-ip>:8000/robot/sit`
- `POST http://<robot-ip>:8000/robot/stop`
- `POST http://<robot-ip>:8000/robot/wave`

Ví dụ phản hồi mong muốn:
```json
{
  "success": true,
  "action": 1,
  "ros_response": "{\"op\":\"service_response\",\"service\":\"\\/switch_op_mode\",\"values\":{\"mode_in\":1,\"success\":true},\"result\":true}"
}
```

## 8. MCP Tools

Robot:
- `reset_robot`
- `rotation_robot`
- `lie_down`
- `stand_up`
- `crawl`
- `squat`
- `sit_down`
- `hand_shake`
- `wave_hand`
- `wave_body`
- `stretch`
- `axis`
- `robot_control(command, name?)`
- `robotivs_stand()`
- `robotivs_sit()`
- `robotivs_stop()`
- `robotivs_wave()`
- `robotivs_control(action)`
- `smart_control(user_text)`

Thông tin trường:
- `hcmut_info(user_text)`
- `hcmut_topics()`
- `hcmut_topic_detail(topic)`
- `hcmut_majors_full()`

## 9. Logging

Hệ thống có log:
- Request/response theo từng tool (robot và HCMUT)
- Trạng thái bridge kết nối WebSocket
- Retry khi mất kết nối (exponential backoff)

## 10. Lưu ý vận hành

- Nếu robot không phản hồi, kiểm tra `robot.ip`, port, mạng nội bộ.
- Nếu robotIVS không phản hồi, kiểm tra `robot_ivs.ip`, port `8000`, endpoint `/robot/*`.
- Nếu endpoint không kết nối được, kiểm tra token trong `mcp.endpoint`.
- Sau khi đổi code/config, restart process `app.mcp_pipe`.
