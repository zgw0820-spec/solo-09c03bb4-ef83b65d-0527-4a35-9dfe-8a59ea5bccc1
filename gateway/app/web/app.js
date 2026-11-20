/* Operation page logic: poll status/events, drive recorder, entries & upgrade APIs. */
const $ = (id) => document.getElementById(id);
let cursor = 0;
let entryDefaultsLoaded = false;

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
const entryBadge = (e) => {
  const name = e === "backup" ? "备用" : e === "primary" ? "主" : (e ?? "?");
  return `<span class="badge ${e === "backup" ? "v2" : "v1"}">${name}</span>`;
};
const statusChip = (s) => `<span class="status-chip ${s ?? "none"}">${s ?? "未尝试"}</span>`;
const ms = (v) => (v === undefined || v === null ? "—" : `${Math.round(v)} ms`);

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
  const entries = Object.entries(s.entries || {}).map(([name, e]) => `
    <div class="small">${entryBadge(name)} 端口 <b>${e.port}</b> · 就绪延迟 ${e.delay}s
      ${e.simulated_fail ? '· <span class="badge busy">故障</span>' : ""}</div>`).join("");
  $("statusBox").innerHTML = `
    <div class="kv">
      <span class="k">当前采集进程</span><span>${versionBadge(s.current_version)}</span>
      <span class="k">换版状态</span><span>${busy}</span>
      <span class="k">记录仪进程</span>
      <span class="badge ${s.recorder_alive ? "on" : "off"}">${s.recorder_alive ? "运行中" : "未运行/已结束"}</span>
      <span class="k">TCP 接受连接总数</span><span>${s.tcp_connections_accepted_total}</span>
      <span class="k">失败标记</span>
      <span class="badge ${s.fail_marker_present ? "busy" : "off"}">${s.fail_marker_present ? "存在" : "无"}</span>
      <span class="k">重复确认</span>
      <span class="badge ${s.duplicate_acks.length ? "busy" : "on"}">${s.duplicate_acks.length} 条</span>
    </div>
    <div style="margin:8px 0">${entries}</div>
    ${gens}
    <div style="margin-top:8px">${connInfo}</div>`;
  renderEntryInputs(s.entries || {});
  renderRace(s.recorder || {});
}

function renderEntryInputs(entries) {
  if (!entries.primary || !entries.backup) return;
  $("pPort").textContent = `:${entries.primary.port}`;
  $("bPort").textContent = `:${entries.backup.port}`;
  if (!entryDefaultsLoaded) {
    $("pDelay").value = entries.primary.delay;
    $("bDelay").value = entries.backup.delay;
    $("pFail").checked = entries.primary.simulated_fail;
    $("bFail").checked = entries.backup.simulated_fail;
    entryDefaultsLoaded = true;
  }
}

function renderRace(rec) {
  const box = $("raceBox");
  if (!rec.mode || (!rec.choice && !rec.no_entry_ready && !(rec.attempts || []).length)) {
    box.innerHTML = '<span class="small">尚未启动记录仪（双入口模式可查看各路尝试结果）</span>';
    return;
  }
  const attempts = Object.fromEntries((rec.attempts || []).map((a) => [a.entry, a]));
  const chosen = rec.choice?.entry ?? null;
  const cards = ["primary", "backup"].map((name) => {
    const a = attempts[name];
    const cls = chosen === name ? "win" : (a ? "lose" : "");
    const metrics = a ? `
      <div class="metric"><span>状态</span><span>${statusChip(a.status)}</span></div>
      <div class="metric"><span>阶段</span><b>${a.phase ?? "—"}</b></div>
      <div class="metric"><span>建连耗时</span><b>${ms(a.connect_ms)}</b></div>
      <div class="metric"><span>就绪答复耗时</span><b>${ms(a.ready_ms)}</b></div>
      <div class="metric"><span>等待</span><b>${ms(a.waited_ms)}</b></div>
      ${a.error ? `<div class="metric"><span>错误</span><b>${a.error}</b></div>` : ""}
      ${a.reply ? `<div class="metric"><span>答复采集进程</span><b>${a.reply.collector ?? "?"}</b></div>` : ""}
    ` : '<div class="metric"><span>状态</span><span>未尝试</span></div>';
    return `<div class="entry-card ${cls}">
      <h3>${entryBadge(name)} ${chosen === name ? '<span class="badge on">已选中</span>' : ""}</h3>
      ${metrics}
    </div>`;
  }).join("");

  let banner = "";
  if (rec.no_entry_ready) {
    banner = `<div class="banner bad">✗ 总等待时限 ${Math.round(rec.no_entry_ready.waited_ms)} ms 内没有一路就绪：
      本批读数未发送（尝试过 ${(rec.no_entry_ready.entries_tried || []).join(" / ")}）</div>`;
  } else if (rec.choice) {
    const other = rec.choice.entry === "primary" ? "备" : "主";
    banner = `<div class="banner ok">✓ 选定 ${rec.choice.entry === "primary" ? "主" : "备用"}入口
      （${Math.round(rec.choice.decision_ms)} ms），整批只走这条 TCP 连接；
      另一路已${rec.choice.other_entry_closed ? "中止并关闭" : "未开启"}。</div>`;
  } else {
    banner = `<div class="banner">竞速进行中…</div>`;
  }
  const batch = rec.batch
    ? `<div class="small">批次：${rec.batch.status} · 已发送 ${rec.batch.sent ?? "?"}/${rec.batch.expected ?? "?"}</div>`
    : "";
  box.innerHTML = banner + `<div class="entry-grid">${cards}</div>
    ${rec.choice ? `<div class="small" style="margin-top:8px">胜出入口：${entryBadge(rec.choice.entry)} · 尝试过的入口：${(rec.choice.entries_tried || []).join(" / ") || "—"}</div>` : ""}
    <div style="margin-top:6px">${batch}</div>`;
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
        body = `${e.entry === "backup" ? "[备用]" : "[主]"} 确认读数 #${e.id} 温度=${e.reading?.temp_c ?? "?"}°C 传感器=${e.reading?.sensor ?? "?"}`;
        ackRows.push(e);
        break;
      case "ready": cls = "ok"; body = "采集进程就绪"; break;
      case "entry_ready":
        cls = "ok"; body = `入口 ${e.entry} 就绪答复已发出（连接 ${e.peer}）`; break;
      case "entry_connect":
        cls = ""; body = `入口 ${e.entry} 收到连接 ${e.peer}（就绪延迟 ${e.delay ?? 0}s${e.simulated_fail ? "，模拟故障" : ""}）`; break;
      case "entry_failed":
        cls = "warn"; body = `入口 ${e.entry} 明确失败：${e.reason}（连接 ${e.peer} 被关闭）`; break;
      case "recorder_report": {
        if (e.kind === "attempt") {
          cls = e.status === "ready" ? "ok" : (e.status === "ready_late" ? "warn" : "");
          const extra = [e.phase, e.error, e.connect_ms !== undefined ? `建连 ${Math.round(e.connect_ms)}ms` : "",
            e.ready_ms !== undefined ? `就绪 ${Math.round(e.ready_ms)}ms` : "",
            e.waited_ms !== undefined ? `等待 ${Math.round(e.waited_ms)}ms` : ""]
            .filter(Boolean).join(" · ");
          body = `尝试 ${e.entry}：${e.status}${extra ? "（" + extra + "）" : ""}`;
        } else if (e.kind === "race") {
          cls = "warn";
          body = `竞速：${e.status}${e.at_ms !== undefined ? ` @${Math.round(e.at_ms)}ms` : ""}`
            + (e.primary ? ` · 主 ${e.primary} · 备 ${e.backup}` : "");
        } else if (e.kind === "choice") {
          cls = "ok";
          body = `选定 ${e.entry} 入口（${Math.round(e.decision_ms)}ms）；另一路${e.other_entry_closed ? "已中止并关闭" : "未开启"}`;
        } else if (e.kind === "no_entry_ready") {
          cls = "err"; body = `到期无入口就绪（等待 ${Math.round(e.waited_ms)}ms），本批不发`;
        } else if (e.kind === "batch") {
          cls = e.status === "sent" ? "ok" : "err";
          body = `批次 ${e.status}：${e.sent}/${e.expected} 笔通过 ${e.entry}`;
        } else {
          body = `记录仪：${JSON.stringify(e)}`;
        }
        break;
      }
      case "start_failed": cls = "err"; body = `启动失败：${e.reason}`; break;
      case "upgrade_started": cls = "warn"; body = `换版开始：${e.old_version} → ${e.new_version}`; break;
      case "upgrade_completed": cls = "ok";
        body = `换版完成：${e.old_version} → ${e.new_version}，原连接转发 ${e.connections_forwarded} 个（同一 TCP fd）`; break;
      case "upgrade_failed": cls = "err"; body = `换版回退：${e.new_version} 未能启动（${e.reason}），旧进程继续收数`; break;
      case "handed_over": cls = "ok"; body = `旧进程已交接并退出：${e.connection_count} 个连接交给 ${e.new_version}`; break;
      case "handover_nack": cls = "err"; body = `交接失败：${e.reason}`; break;
      case "conn_accepted": cls = ""; body = `接受记录仪连接 ${e.peer}（入口 ${e.entry ?? "primary"}）`; break;
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
      tr.innerHTML = `<td>#${e.id}</td><td>${entryBadge(e.entry)}</td><td>${versionBadge(e.version)}</td>
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
      <div><b>${c.peer || "连接"}</b> ${entryBadge(c.entry)}：共 ${c.count} 笔，${status}</div>
      <div style="margin-top:3px">${vers}</div>
      <div class="small" style="margin-top:2px">编号序列：${c.ids.length > 40 ? c.ids.slice(0, 40).join(", ") + " …" : c.ids.join(", ")}</div>
    </div>`;
  }).join("");

  const entryRows = Object.entries(v.entries || {}).map(([name, info]) => {
    const vers = Object.entries(info.versions)
      .map(([ver, d]) => `${versionBadge(ver)} ×${d.count}`).join("　");
    const ids = info.ids.length > 24
      ? info.ids.slice(0, 12).join(", ") + " … " + info.ids.slice(-6).join(", ")
      : info.ids.join(", ");
    return `<tr>
      <td>${entryBadge(name)}</td>
      <td><b>${info.count}</b> 笔</td>
      <td>#${info.ids[0] ?? "—"}–#${info.ids[info.ids.length - 1] ?? "—"}</td>
      <td>${vers}</td>
      <td class="small">${ids || "—"}</td>
    </tr>`;
  }).join("");
  const entryTable = entryRows
    ? `<div style="margin:10px 0 4px"><b>按入口核对（采集端确认的读数编号）</b></div>
       <table><thead><tr><th>入口</th><th>已确认</th><th>范围</th><th>处理版本</th><th>编号</th></tr></thead>
       <tbody>${entryRows}</tbody></table>` : "";

  const dup = v.duplicate_acks.length
    ? `<div class="bad">✗ 发现 ${v.duplicate_acks.length} 条重复确认</div>`
    : '<div class="good">✓ 没有任何读数被确认两次</div>';
  $("verifyBox").innerHTML = (v.ok
    ? '<div class="good" style="margin-bottom:8px"><b>总体校验通过</b></div>'
    : '<div class="bad" style="margin-bottom:8px"><b>总体校验未通过</b></div>')
    + dup + parts + entryTable;
  $("seqBanner").innerHTML = v.connections.map((c) =>
    `<span>${entryBadge(c.entry)} ${c.peer || "conn"}: <b>${c.count}</b> 笔，版本分界 ${
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
  const mode = $("rMode").value;
  try {
    await api("/api/recorder/start", {
      method: "POST",
      body: JSON.stringify({
        mode,
        count: +$("rCount").value,
        interval: +$("rInterval").value,
        gap_at: +$("rGapAt").value,
        gap_seconds: +$("rGapSec").value,
        failover_after: +$("rFailoverAfter").value,
        total_timeout: +$("rTotalTimeout").value,
      }),
    });
    toast(mode === "dual" ? "双入口记录仪已启动：等待最先就绪的一路" : "单入口记录仪已启动");
  } catch (err) { toast(err.message, true); }
});

$("btnStopRec").addEventListener("click", async () => {
  try { await api("/api/recorder/stop", { method: "POST" }); toast("已停止记录仪"); }
  catch (err) { toast(err.message, true); }
});

async function applyEntry(entry, delayId, failId) {
  try {
    await api(`/api/entries/${entry}`, {
      method: "POST",
      body: JSON.stringify({ delay: +$(delayId).value, fail: $(failId).checked }),
    });
    toast(`${entry === "primary" ? "主" : "备用"}入口策略已应用`);
  } catch (err) { toast(err.message, true); }
}
$("btnApplyPrimary").addEventListener("click", () =>
  applyEntry("primary", "pDelay", "pFail"));
$("btnApplyBackup").addEventListener("click", () =>
  applyEntry("backup", "bDelay", "bFail"));

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
