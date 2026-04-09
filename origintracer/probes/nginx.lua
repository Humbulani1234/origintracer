-- origintracer/nginx/nginx.lua
-- Fires at rewrite phase (start) and log phase (complete).
-- Sends JSON events via UDP to the Python engine's receiver.
-- Correlation key: $remote_addr + $remote_port = same as kprobe accept4 sockaddr.

local STACKTRACER_HOST = os.getenv("STACKTRACER_LUA_HOST") or "127.0.0.1"
local STACKTRACER_PORT = tonumber(os.getenv("STACKTRACER_LUA_PORT") or "9119")

local _udp = nil
-- Replace get_udp() with this:
local function get_udp()
    -- Reuse socket stored in context from access phase
    if ngx.ctx._st_udp then
        return ngx.ctx._st_udp
    end
    -- Fallback: create a simple socket using luasocket (available in log phase)
    local socket = require("socket")
    local sock = socket.udp()
    sock:setpeername("127.0.0.1", STACKTRACER_PORT)
    ngx.ctx._st_udp = sock
    return sock
end

local ok_cjson, cjson = pcall(require, "cjson.safe")
local function encode(t)
    if ok_cjson then return cjson.encode(t) end
    local parts, first = {}, true
    for k, v in pairs(t) do
        if not first then parts[#parts+1] = "," end
        first = false
        parts[#parts+1] = '"'..k..'":'
        if type(v) == "number" then parts[#parts+1] = tostring(v)
        elseif v == nil then parts[#parts+1] = "null"
        else parts[#parts+1] = '"'..tostring(v):gsub('"','\\"')..'"' end
    end
    return "{"..table.concat(parts).."}"
end

local function emit(event)
    local sock = get_udp()
    if sock then sock:send(encode(event)) end
end

-- **** rewrite phase: record start time ************************
local function on_rewrite()
    ngx.ctx.st_start_ms    = ngx.now() * 1000
    ngx.ctx.st_trace_id    = ngx.var.request_id or (ngx.var.pid.."-"..ngx.now())
    ngx.ctx.st_remote_addr = ngx.var.remote_addr or ""
    ngx.ctx.st_remote_port = tonumber(ngx.var.remote_port) or 0
end

-- ── log phase: emit complete event ───────────────────────────────────
local function on_log()
    local ctx         = ngx.ctx
    local start_ms    = ctx.st_start_ms or (ngx.now() * 1000)
    local dur_ms      = ngx.now() * 1000 - start_ms
    local trace_id    = ctx.st_trace_id or ngx.var.request_id or ""
    local remote_addr = ctx.st_remote_addr or ngx.var.remote_addr or ""
    local remote_port = ctx.st_remote_port or tonumber(ngx.var.remote_port) or 0

    local upstream_ms = -1
    local urt = ngx.var.upstream_response_time
    if urt and urt ~= "-" and urt ~= "" then
        local n = tonumber((urt:match("[^,]+$") or urt):match("[%d%.]+"))
        if n then upstream_ms = n * 1000 end
    end

    local nginx_own_ms = -1
    if upstream_ms > 0 then
        nginx_own_ms = dur_ms - upstream_ms
        if nginx_own_ms < 0 then nginx_own_ms = 0 end
    end

    emit({
        probe          = "nginx.request.complete",
        service        = "nginx",
        source         = "lua",
        trace_id       = trace_id,
        -- These two fields are the kprobe correlation key:
        -- kprobe captures them from accept4() sockaddr.
        -- Python engine merges by matching (remote_addr, remote_port).
        remote_addr    = remote_addr,
        remote_port    = remote_port,
        -- HTTP semantics — what kprobe cannot see:
        uri            = ngx.var.uri or "",
        method         = ngx.var.request_method or "",
        http_version   = ngx.var.server_protocol or "",
        host           = ngx.var.host or "",
        status         = tonumber(ngx.var.status) or 0,
        bytes_sent     = tonumber(ngx.var.bytes_sent) or 0,
        -- Timing decomposition:
        duration_ms    = dur_ms,
        upstream_ms    = upstream_ms,   -- -1 means no upstream (cache/error)
        nginx_own_ms   = nginx_own_ms,  -- nginx processing time excluding upstream
        -- Upstream identity:
        upstream_addr  = ngx.var.upstream_addr or "",
        upstream_status= ngx.var.upstream_status or "",
        worker_pid     = tonumber(ngx.var.pid) or 0,
        timestamp_ms   = ngx.now() * 1000,
    })
end

local phase = ngx.get_phase()
if     phase == "rewrite" then on_rewrite()
elseif phase == "log"     then on_log() end