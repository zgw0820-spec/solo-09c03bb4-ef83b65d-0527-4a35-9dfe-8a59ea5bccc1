/* Operation page logic: poll status/events, drive recorder & upgrade APIs. */
const $ = (id) => document.getElementById(id);
let cursor = 0;

function toast(msg, isErr = false) {
  const t = $("toast");
  t.textContent = msg;
  t.className = "toast show" + (isErr ? " err" : "");
  setTimeout(() => t.className = "toast", 3500);
}

async function api(path, opts = {}) {
  const res = await fetch(path, {
    headers: { "Content-Type": "application/json" },
    ...opts,
  });
  const data = await res.json().catch(() => ({}));
  if (!res.ok) throw new Error(data.error || data.detail || `HTTP ${res.status}`);
  return data;
}

const versionBadge = (v) =>
  `<span class="badge ${v === "v1" ? "v1" : "v2"}">${v ?? "?"}</span>`;

function renderStatus(s) {
  const busy = s.handover_in_progress
    ? '<span class="badge busy">交接中</span>'
    : '<span class="badge on">稳态运行</span>';
  const gens = s.generations.map((g) => `
    <div class="gen ${g.current ? "current" : ""}">
      <div class="row">
        <span>${versionBadge(g.version)} ${g.current ? "（当前）" : ""}</span>
        <span class="badge ${g.alive ? "on" : "off"}">${g.alive ? "存活" : "已退出"}</span>
      </div>
      <div class="meta">pid=${g.pid} · ready=${g.ready}${g.failed ? ` · 启动失败: ${g.fail_reason ?? ""}` : ""} · rc=${g.exit_code ?? "-"}</div>
    </div>`).join("");
  const connInfo = Object.entries(s.ack_counts)
    .map(([peer, n]) => `<div><span class="k">${peer}</span> 已确认 <b>${n}</b> 笔</div>`)
    .join("") || '<div class="small">尚无连接</div>';
  $("statusBox").innerHTML = `
    <div class="kv">
      <span class="k">当前采集进程</span><span>${versionBadge(s.current_version)}</span>
      <span class="k">换版状态</span><span>${busy}</span>
      <span class="k">记录仪进程</span>
      <span class="badge ${s.recorder_alive ? "on" : "off"}">${s.recorder_alive ? "运行中（单连接）" : "未运行/已结束"}</span>
      <span class="k">TCP 接受连接总数</span><span>${s.tcp_connections_accepted_total}</span>
      <span class="k">失败标记</span>
      <span class="badge ${s.fail_marker_present ? "busy" : "off"}">${s.fail_marker_present ? "存在" : "无"}</span>
      <span class="k">重复确认</span>
      <span class="badge ${s.duplicate_acks.length ? "busy" : "on"}">${s.duplicate_acks.length} 条</span>
    </div>
    ${gens}
    <div style="margin-top:8px">${connInfo}</div>`;
}

function appendEvents(events) {
  const log = $("logBox");
  const atBottom = log.scrollTop + log.clientHeight >= log.scrollHeight - 30;
  const ackRows = [];
  for (const e of events) {
    const div = document.createElement("div");
    div.className = "e";
    const t = new Date((e.ts || 0) * 1000).toLocaleTimeString("zh-CN", { hour12: false });
    const v = e.version ? `<span class="pill ${e.version}">${e.version}</span>` : "";
    let cls = "";
    let body = "";
    switch (e.type) {
      case "ack":
        cls = "ack";
        body = `确认读数 #${e.id} 温度=${e.reading?.temp_c ?? "?"}°C 传感器=${e.reading?.sensor ?? "?"}`;
        ackRows.push(e);
        break;
      case "ready": cls = "ok"; body = "采集进程就绪"; break;
      case "start_failed": cls = "err"; body = `启动失败：${e.reason}`; break;
      case "upgrade_started": cls = "warn"; body = `换版开始：${e.old_version} → ${e.new_version}`; break;
      case "upgrade_completed": cls = "ok";
        body = `换版完成：${e.old_version} → ${e.new_version}，原连接转发 ${e.connections_forwarded} 个（同一 TCP fd）`; break;
      case "upgrade_failed": cls = "err"; body = `换版回退：${e.new_version} 未能启动（${e.reason}），旧进程继续收数`; break;
      case "handed_over": cls = "ok"; body = `旧进程已交接并退出：${e.connection_count} 个连接交给 ${e.new_version}`; break;
      case "handover_nack": cls = "err"; body = `交接失败：${e.reason}`; break;
      case "conn_accepted": cls = ""; body = `接受记录仪连接 ${e.peer}`; break;
      case "conn_closed": cls = ""; body = `记录仪关闭连接 ${e.peer ?? ""}`; break;
      case "conn_error": cls = "err"; body = `连接异常 ${e.peer ?? ""}`; break;
      case "duplicate_ack": cls = "err"; body = `重复确认 #${e.id}（${e.first_version} 已确认，${e.dup_version} 又确认）`; break;
      case "supervisor_log": cls = ""; body = e.msg; break;
      case "recorder_log": cls = ""; body = `[记录仪] ${e.msg}`; break;
      case "recorder_exited": cls = ""; body = `[记录仪] 退出 rc=${e.rc}`; break;
      case "log": cls = ""; body = `[采集进程 ${e.version}] ${e.msg}`; break;
      case "exiting": cls = ""; body = `采集进程 ${e.version} 退出中`; break;
      case "bad_line": cls = "err"; body = `无法解析的行：${e.error}`; break;
      default: body = JSON.stringify(e);
    }
    div.className = "e " + cls;
    div.innerHTML = `<span style="color:#54687c">${t}</span>${v}${body}`;
    log.appendChild(div);
  }
  while (log.children.length > 400) log.removeChild(log.firstChild);
  if (atBottom) log.scrollTop = log.scrollHeight;

  if (ackRows.length) {
    const tb = $("ackBody");
    for (const e of ackRows) {
      const tr = document.createElement("tr");
      tr.innerHTML = `<td>#${e.id}</td><td>${versionBadge(e.version)}</td>
        <td>${e.reading?.temp_c ?? "?"}</td><td>${e.peer ?? ""}</td><td>${e.seq}</td>`;
      tb.appendChild(tr);
    }
    while (tb.children.length > 2000) tb.removeChild(tb.firstChild);
  }
}

function renderVerify(v) {
  if (!v.connections.length) {
    $("verifyBox").innerHTML = '<span class="small">尚无确认记录</span>';
    $("seqBanner").innerHTML = "";
    return;
  }
  const parts = v.connections.map((c) => {
    const status = (c.contiguous_from_1 && c.unique)
      ? '<span class="good">✓ 编号 1..N 连续、无重复</span>'
      : '<span class="bad">✗ 存在缺号或重复</span>';
    const vers = Object.entries(c.versions)
      .map(([ver, info]) => `${versionBadge(ver)} 确认 #${info.first_id}–#${info.last_id}（${info.count} 笔）`)
      .join("　");
    return `<div style="margin-bottom:8px">
      <div><b>${c.peer || "连接"}</b>：共 ${c.count} 笔，${status}</div>
      <div style="margin-top:3px">${vers}</div>
      <div class="small" style="margin-top:2px">编号序列：${c.ids.length > 40 ? c.ids.slice(0, 40).join(", ") + " …" : c.ids.join(", ")}</div>
    </div>`;
  }).join("");
  const dup = v.duplicate_acks.length
    ? `<div class="bad">✗ 发现 ${v.duplicate_acks.length} 条重复确认</div>`
    : '<div class="good">✓ 没有任何读数被确认两次</div>';
  $("verifyBox").innerHTML = (v.ok
    ? '<div class="good" style="margin-bottom:8px"><b>总体校验通过</b></div>'
    : '<div class="bad" style="margin-bottom:8px"><b>总体校验未通过</b></div>')
    + dup + parts;
  $("seqBanner").innerHTML = v.connections.map((c) =>
    `<span>${c.peer || "conn"}: <b>${c.count}</b> 笔，版本分界 ${
      Object.keys(c.versions).join(" → ")
    }</span>`).join("");
}

async function poll() {
  try {
    const [s, ev, v] = await Promise.all([
      fetch("/api/status").then((r) => r.json()),
      fetch(`/api/events?cursor=${cursor}`).then((r) => r.json()),
      fetch("/api/verify").then((r) => r.json()),
    ]);
    renderStatus(s);
    if (ev.events.length) appendEvents(ev.events);
    cursor = ev.cursor;
    renderVerify(v);
  } catch (err) {
    console.error(err);
  }
}

$("recForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  try {
    await api("/api/recorder/start", {
      method: "POST",
      body: JSON.stringify({
        count: +$("rCount").value,
        interval: +$("rInterval").value,
        gap_at: +$("rGapAt").value,
        gap_seconds: +$("rGapSec").value,
      }),
    });
    toast("记录仪已启动：只建立一次 TCP 连接");
  } catch (err) { toast(err.message, true); }
});

$("btnStopRec").addEventListener("click", async () => {
  try { await api("/api/recorder/stop", { method: "POST" }); toast("已停止记录仪"); }
  catch (err) { toast(err.message, true); }
});

$("upForm").addEventListener("submit", async (e) => {
  e.preventDefault();
  const newVersion = $("newVersion").value.trim();
  if (!newVersion) return toast("请填写新版本号", true);
  try {
    const r = await api("/api/upgrade", {
      method: "POST",
      body: JSON.stringify({ new_version: newVersion, simulate_fail: $("simFail").checked }),
    });
    toast($("simFail").checked
      ? "已发起换版（新版将启动失败，预期自动回退）"
      : `换版已发起：${r.from} → ${r.to}`);
  } catch (err) { toast(err.message, true); }
});

$("btnMarkerOn").addEventListener("click", async () => {
  await api("/api/fail-marker", { method: "POST", body: JSON.stringify({ present: true }) });
  toast("失败标记已放置：下一个启动的采集进程将失败");
});
$("btnMarkerOff").addEventListener("click", async () => {
  await api("/api/fail-marker", { method: "POST", body: JSON.stringify({ present: false }) });
  toast("失败标记已清除");
});
$("btnReset").addEventListener("click", async () => {
  await api("/api/events/reset", { method: "POST" });
  cursor = 0;
  $("ackBody").innerHTML = "";
  $("logBox").innerHTML = "";
  toast("事件与确认记录已清空");
});

poll();
setInterval(poll, 700);
