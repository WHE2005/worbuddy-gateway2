(function () {
  "use strict";

  var KEY_STORAGE = "wb_gateway_admin_key";
  var POLL_MS = 3000;
  var LIST_FILTERS = [
    {kind: "accounts", id: "onlyUsableAccounts", cookie: "gateway_accounts_only_usable",
      route: "/admin/api/status", statusID: "accountFilterStatus"},
    {kind: "models", id: "onlyUsable", cookie: "gateway_models_only_usable",
      route: "/admin/api/models", statusID: "modelFilterStatus"}
  ];

  var state = {
    apiKey: sessionStorage.getItem(KEY_STORAGE) || "",
    activeTab: "overview",
    lastStatus: null,
    lastModels: null,
    timer: null,
    refreshPending: false,
    settings: null,
    togglePending: false,
    policyVersion: 0,
    logSettings: null,
    logSettingsVersion: 0,
    settingsLoadVersion: 0,
    tasksLastPoll: 0,
    configMode: "preview",
    configPending: false,
    configVersion: 0,
    configPreviewVersion: 0,
    configRevision: "",
    configOriginal: "",
    rateDetails: null,
    rateDetailsAt: 0,
    rateDetailsPending: null,
    openRateKeys: {},
    openRawLogs: {},
    statsPending: false,
    statsVersion: 0,
    statsLastPoll: 0,
    lastStats: null,
    accountAlertsPending: false,
    accountAlertsLastPoll: 0,
    accountAlertsGeneration: 0,
    lastAccountAlerts: null,
    addAccountId: "",
    addAccountPoll: 0,
    addAccountDeadline: 0,
    statsAccount: "",
    statsStartTouched: false,
    statsEndTouched: false,
    statsFollowToday: true
  };

  function $(id) { return document.getElementById(id); }

  function esc(v) {
    return String(v === undefined || v === null ? "" : v)
      .replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;").replace(/'/g, "&#39;");
  }

  function num(v) {
    return (v === undefined || v === null || v === "") ? "-" : v;
  }

  function count(v) { return v === undefined || v === null ? 0 : v; }

  function fmtTime(unix) {
    if (!unix) return "-";
    var d = new Date(unix * 1000);
    if (isNaN(d.getTime())) return "-";
    return d.toLocaleString("zh-CN", { hour12: false });
  }

  // 统计区间按服务端 UTC+8 口径计算“今天”，避免浏览器本地时区跨日不一致。
  function statsToday() {
    try {
      return new Intl.DateTimeFormat("sv-SE", { timeZone: "Asia/Shanghai" }).format(new Date());
    } catch (_) {
      return new Date(Date.now() + 8 * 60 * 60 * 1000).toISOString().slice(0, 10);
    }
  }

  function fmtMs(v, has) {
    if (!has || v === undefined || v === null) return "-";
    return v < 1000 ? v + "ms" : (v / 1000).toFixed(1) + "s";
  }

  function fmtTokensM(v) {
    if (!v) return "0.00M";
    return (v / 1000000).toFixed(2) + "M";
  }

  function showAlert(msg, ok) {
    var el = $("alert");
    el.textContent = msg;
    el.className = "alert" + (ok ? " ok" : "");
  }

  function hideAlert() { $("alert").className = "alert hidden"; }

  // setConn 更新右上角常驻连接状态，让每次请求的结果都可见（不会一闪而过）。
  function setConn(text, cls) {
    var el = $("conn");
    if (!el) return;
    el.textContent = text;
    el.className = "conn" + (cls ? " " + cls : "");
  }

  function nowText() {
    return new Date().toLocaleTimeString("zh-CN", { hour12: false });
  }

  function traceID() {
    return "web-" + Date.now().toString(36) + "-" + Math.random().toString(36).slice(2);
  }

  function reportUIEvent(event, path, status, trace, enabled) {
    if (!state.apiKey || path === "/admin/api/setup") return;
    var record = {event: event, route: path.split("?")[0], status: status};
    if (typeof enabled === "boolean") record.enabled = enabled;
    fetch("/admin/api/client-events", {
      method: "POST",
      headers: {"Authorization": "Bearer " + state.apiKey, "Content-Type": "application/json",
        "X-Workbuddy-Admin": "1", "X-Trace-ID": trace},
      body: JSON.stringify(record)
    }).catch(function () { /* 原请求失败仍由服务端审计记录，不递归发送失败事件。 */ });
  }

  function lockDashboard() {
    hideChartTooltip();
    resetConfigEditor();
    state.tasksLastPoll = 0;
    $("tabs").classList.add("hidden");
    $("dashboard").classList.add("hidden");
    state.lastStatus = null;
    state.lastModels = null;
    state.rateDetails = null;
    state.rateDetailsAt = 0;
    state.rateDetailsPending = null;
    state.openRateKeys = {};
    state.openRawLogs = {};
    state.statsVersion++;
    state.statsLastPoll = 0;
    state.lastStats = null;
    ["usageSummary", "usageChart", "latencyChart", "tokenChart", "statsInfo", "statsRange"].forEach(function (id) {
      $(id).textContent = "";
    });
    if ($("latencyLegend")) $("latencyLegend").textContent = "";
    if ($("tokenLegend")) $("tokenLegend").textContent = "";
    $("statsAccount").innerHTML = '<option value="">全局 · 全部账号</option>';
    $("statsTable").querySelector("tbody").innerHTML = "";
    $("statsStorage").textContent = "等待统计";
    state.accountAlertsGeneration++;
    state.accountAlertsLastPoll = 0;
    state.lastAccountAlerts = null;
    state.addAccountId = "";
    state.addAccountPoll = 0;
    state.addAccountDeadline = 0;
    $("poolHealth").textContent = "—";
    $("poolHealth").className = "health-value";
    $("poolHealthBar").style.width = "0%";
    $("poolHealthStatus").textContent = "等待账号状态";
    if ($("poolStateDist")) $("poolStateDist").innerHTML = "";
    $("accountAlertsInfo").textContent = "登录后读取账号异常日志。";
    $("accountAlerts").innerHTML = '<p class="stats-empty">等待日志</p>';
    ["meta", "gatewayInfo", "logBox", "configBox", "tasksInfo"].forEach(function (id) {
      $(id).textContent = "";
    });
    ["accountsTable", "modelsTable", "tasksTable"].forEach(function (id) {
      $(id).querySelector("tbody").innerHTML = "";
    });
  }

  function api(path, options) {
    options = options || {};
    var trace = options.trace || traceID();
    var keyAtStart = state.apiKey;
    var headers = {"X-Trace-ID": trace};
    if (state.apiKey) headers["Authorization"] = "Bearer " + state.apiKey;
    if (options.method === "POST") {
      headers["Content-Type"] = "application/json";
      headers["X-Workbuddy-Admin"] = "1";
    }
    var status = 0;
    return fetch(path, {headers: headers, method: options.method || "GET", cache: "no-store",
      body: options.body === undefined ? undefined : JSON.stringify(options.body)}).then(function (resp) {
      status = resp.status;
      return resp.json().then(function (data) { return {resp: resp, data: data}; });
    }).then(function (result) {
      if (keyAtStart !== state.apiKey) throw new Error("登录状态已变化，忽略旧请求结果");
      var resp = result.resp;
      if (resp.status === 401) {
        state.apiKey = "";
        sessionStorage.removeItem(KEY_STORAGE);
        lockDashboard();
        setConn("鉴权失败 (401)", "danger");
        showAlert("管理鉴权失败：请输入 config.json 中的 gateway.adminKey，模型 API Key 不能用于管理登录。");
        throw new Error("管理鉴权失败");
      }
      if (!resp.ok) {
        if (!options.quiet) {
          setConn("请求失败 (" + resp.status + ")", "danger");
          showAlert("HTTP " + resp.status + "：" + (result.data.error || "请求失败") +
            (path.split("?")[0] === "/admin/api/config-edit" ? "；操作未完成，已有编辑输入保留。" : "；已有数据保留，未更新为零。"));
        }
        var failure = new Error("HTTP " + resp.status);
        failure.status = resp.status;
        failure.detail = result.data.error || "请求失败";
        throw failure;
      }
      hideAlert();
      setConn("已连接 · " + nowText(), "ok");
      reportUIEvent("request_completed", path, status, trace);
      return result.data;
    }).catch(function (err) {
      if (keyAtStart === state.apiKey) {
        reportUIEvent("request_failed", path, status, trace);
        if (!status) {
          setConn("连接失败", "danger");
          showAlert("请求未完成；保留上次数据，请检查网络后重试。");
        }
      }
      throw err;
    });
  }

  // ---- 渲染：总览 --------------------------------------------------------
  function renderPoolHealth(data) {
    var h = data.health || {}, p = h.percent;
    var known = !data.stale && typeof p === "number" && isFinite(p) && p >= 0 && p <= 100;
    var stateClass = known ? (p === 100 ? "healthy" : (p === 0 ? "unavailable" : "degraded")) : "unknown";
    $("poolHealth").textContent = known ? p.toLocaleString("zh-CN", {maximumFractionDigits: 1}) + "%" : "—";
    $("poolHealth").className = "health-value " + stateClass;
    $("poolHealthBar").style.width = known ? p + "%" : "0%";
    $("poolHealthBar").className = stateClass;
    $("poolHealthStatus").textContent = data.stale ? "快照过旧 · 健康度待确认" :
      h.state === "empty" ? "未加载账号 · 暂无健康度" : !known ? "健康度待确认" :
      p === 100 ? "账号池全部可用" : p === 0 ? "账号池当前不可用，请查看告警" : "账号池部分受限，请查看告警";
    // 状态分布：健康度只给一个比例，这里补充构成（可用/冷却/耗尽/过期/失效各多少），
    // quota_exhausted 是 paid_exhausted 的历史别名，合并后展示。
    var dist = $("poolStateDist");
    if (dist) {
      var counts = {};
      (data.accounts || []).forEach(function (acc) {
        var key = acc.state === "quota_exhausted" ? "paid_exhausted" : acc.state;
        if (!STATE_TEXT[key]) key = "active";
        counts[key] = (counts[key] || 0) + 1;
      });
      dist.innerHTML = ["active", "cooldown", "paid_exhausted", "expired", "disabled"].filter(function (k) {
        return counts[k];
      }).map(function (k) {
        return '<span class="state-chip ' + STATE_CLASS[k] + '">' + esc(STATE_TEXT[k]) + " " + counts[k] + "</span>";
      }).join("") || '<span class="state-chip">暂无账号</span>';
    }
  }

  function renderAccountAlerts(data) {
    state.lastAccountAlerts = data;
    var labels = {urgent: "紧急", important: "重要", warning: "注意", notice: "提示"};
    var alerts = data.alerts || [];
    var snapshot = state.lastStatus;
    // 日志扫描短缓存期间仍用最新账号快照剔除已恢复项，不把历史故障继续当作当前告警。
    if (snapshot && !snapshot.stale && Array.isArray(snapshot.accounts)) {
      alerts = alerts.filter(function (a) {
        return snapshot.accounts.some(function (acc) {
          return acc.path === a.account && (a.state === "paused" ? acc.schedulingEnabled === false :
            a.state === (acc.state === "quota_exhausted" ? "paid_exhausted" : acc.state));
        });
      });
    }
    var notes = ["来源：普通日志 · 只列当前异常，最新告警在上；等级仅区分颜色和标签"];
    if (data.stale || (snapshot && snapshot.stale)) notes.push("快照过旧，以下为上次确认的异常");
    if (data.missingLogCount) notes.push("部分异常暂未找到对应日志，可能等待下一次扫描");
    if (data.scanTruncated || data.listTruncated) notes.push("已达到读取上限，仅显示扫描范围内记录");
    $("accountAlertsInfo").textContent = notes.join("；") + "。";
    $("accountAlerts").innerHTML = alerts.length ? alerts.map(function (a) {
      var level = labels[a.severity] ? a.severity : "notice";
      return '<article class="account-alert alert-' + level + '"><div class="account-alert-head">' +
        '<span class="alert-priority">' + esc(labels[level]) + '</span><strong>' + esc(a.label) +
        '</strong><span class="account-alert-file">' + esc(a.account) + '</span><time>' + esc(fmtTime(a.at)) +
        '</time></div><p>' + esc(a.message) + '</p></article>';
    }).join("") : '<p class="stats-empty">' + (data.missingLogCount ? "当前存在异常，但扫描范围内暂未找到对应日志" :
      data.stale ? "暂无日志记录，账号状态待确认" : "未发现当前账号异常日志") + '</p>';
  }

  function loadAccountAlerts() {
    if (!state.apiKey || state.accountAlertsPending || Date.now() - state.accountAlertsLastPoll < 10000) return Promise.resolve();
    var generation = state.accountAlertsGeneration, key = state.apiKey;
    state.accountAlertsPending = true;
    state.accountAlertsLastPoll = Date.now();
    return api("/admin/api/account-alerts").then(function (data) {
      if (generation === state.accountAlertsGeneration && key === state.apiKey) renderAccountAlerts(data);
    }).catch(function () {
      if (generation === state.accountAlertsGeneration && key === state.apiKey) {
        $("accountAlertsInfo").textContent = "告警日志读取失败，保留上次结果；这不代表账号池没有异常。";
      }
    }).then(function () { state.accountAlertsPending = false; });
  }

  // 统计事实来自可选 stats.json；所有查询使用管理鉴权和既有前端落盘审计。
  function statsNumber(v) {
    return Number(v || 0).toLocaleString("zh-CN", {maximumFractionDigits: 2});
  }

  function statsCredit(v) {
    return Number(v || 0).toLocaleString("zh-CN", {maximumFractionDigits: 4});
  }

  // 图表轴用紧凑单位，避免几十万、几百万的 Token 标签挤在一起；悬停仍显示精确值。
  function statsTokenAxis(v) {
    v = Number(v || 0);
    if (v >= 1000000000) return (v / 1000000000).toFixed(2) + "B";
    if (v >= 1000000) return (v / 1000000).toFixed(2) + "M";
    if (v >= 1000) return (v / 1000).toFixed(1) + "K";
    return Math.round(v).toLocaleString("zh-CN");
  }

  function statsDuration(v) {
    return v === null || v === undefined ? "—" : (Number(v) / 1000).toFixed(2) + " s";
  }

  function statsCard(label, value, detail, color) {
    return '<article class="usage-metric ' + color + '"><div class="usage-label">' + esc(label) +
      '</div><div class="usage-value">' + esc(value) + '</div><div class="usage-detail">' + esc(detail) + '</div></article>';
  }

  // 原生 SVG，不访问 CDN。空时间桶及未知耗时断开折线，不伪造连续的零值。
  // box 决定 viewBox 尺寸。让它贴近容器实际像素（比例≈1:1），
  // 字号才不会随缩放变小；窄屏用更矮的目标高度。
  // 原生 SVG，不加载 CDN。折线在时间桶缺样本处断开，不补零；柱形画在下层，折线覆盖在上层。
  // opts = {lines, bars, title, leftFormat, rightFormat, tooltipFormat, logScale, box}；
  // 给了 rightFormat 时柱形改用右轴；logScale 只作用于左轴折线。
  function statsComboChart(buckets, opts) {
    opts = opts || {};
    var lines = opts.lines || [], bars = opts.bars || [];
    var title = opts.title || "";
    var leftFormat = opts.leftFormat || function (v) { return String(v); };
    var tooltipFormat = opts.tooltipFormat || leftFormat;
    var rightFormat = opts.rightFormat || null;
    var logScale = !!opts.logScale;
    var box = opts.box || {w: 900, h: 260};
    var pick = function (s, b, i) {
      var v = s.value(b, i);
      return (v === null || v === undefined || !isFinite(v)) ? null : Number(v);
    };
    var gather = function (list) {
      var out = [];
      list.forEach(function (s) { buckets.forEach(function (b, i) {
        var v = pick(s, b, i);
        if (v !== null) out.push(v);
      }); });
      return out;
    };
    var lineValues = gather(lines), barValues = gather(bars);
    if (!lineValues.length && !barValues.length) return '<p class="stats-empty">该时间范围暂无可用样本</p>';
    var dual = !!rightFormat && barValues.length > 0;
    var scaleMax = function (values) {
      var m = values.length ? Math.max.apply(null, values) : 0;
      if (m <= 0) m = 1;
      return m * 1.12;
    };
    var leftMax = scaleMax(dual ? lineValues : lineValues.concat(barValues));
    var rightMax = scaleMax(dual ? barValues : []);
    var leftLogMax = Math.log10(leftMax + 1);
    var width = Math.max(260, Math.round(box.w)), height = Math.max(190, Math.round(box.h));
    // 悬停命中数据由同一次绘制生成，避免前端再猜一遍坐标；测试和重绘都复用这份几何。
    var meta = {width: width, height: height, lines: [], bars: [],
      lineKind: opts.lineKind || "折线", barKind: opts.barKind || "柱形"};
    var top = 18, bottom = 40;
    var leftTicks = [0, 1, 2, 3, 4].map(function (n) {
      return leftFormat(logScale ? Math.pow(10, leftLogMax * (1 - n / 4)) - 1 : leftMax * (1 - n / 4));
    });
    var rightTicks = dual ? [0, 1, 2, 3, 4].map(function (n) { return rightFormat(rightMax * (1 - n / 4)); }) : [];
    var left = Math.min(130, Math.max(52, 14 + Math.max.apply(null, leftTicks.map(function (t) { return t.length; })) * 7));
    var right = dual ? Math.min(130, Math.max(44, 14 + Math.max.apply(null, rightTicks.map(function (t) { return t.length; })) * 7)) : 16;
    var plotW = width - left - right, plotH = height - top - bottom;
    var x = function (i) { return buckets.length < 2 ? left + plotW / 2 : left + i * plotW / (buckets.length - 1); };
    var yLeft = logScale ?
      function (v) { return top + plotH * (1 - Math.log10(v + 1) / leftLogMax); } :
      function (v) { return top + plotH * (1 - v / leftMax); };
    var yRight = function (v) { return top + plotH * (1 - v / rightMax); };
    var out = '<svg viewBox="0 0 ' + width + ' ' + height + '" role="img" aria-label="' + esc(title) +
      '"><title>' + esc(title) + '</title>';
    for (var n = 0; n <= 4; n++) {
      var yy = top + plotH * n / 4;
      out += '<line class="chart-grid" x1="' + left + '" y1="' + yy + '" x2="' + (width - right) + '" y2="' + yy + '"/>' +
        '<text class="chart-label" text-anchor="end" x="' + (left - 8) + '" y="' + (yy + 4) + '">' + esc(leftTicks[n]) + '</text>';
      if (dual) {
        out += '<text class="chart-label" text-anchor="start" x="' + (width - right + 8) + '" y="' + (yy + 4) + '">' + esc(rightTicks[n]) + '</text>';
      }
    }
    // 窄屏把日期缩成 MM-DD；刻度数量按可用宽度反推，避免标签互相重叠。
    var narrow = width < 520;
    var tickLabel = function (text) {
      return narrow && /^\d{4}-\d{2}-\d{2}$/.test(text) ? text.slice(5) : text;
    };
    var widest = 0;
    buckets.forEach(function (b) { widest = Math.max(widest, tickLabel(b.label).length); });
    var perTick = Math.max(44, widest * 7 + 14);
    var maxTicks = Math.max(2, Math.floor(plotW / perTick));
    var every = Math.max(1, Math.ceil(buckets.length / maxTicks));
    // 首尾标签贴边（居中会溢出 viewBox 被裁），再按像素间距贪心取舍：
    // 末尾刻度优先级最高，与前一个冲突时挤掉前一个，保证最后一个日期始终可见。
    var labelW = widest * 6.6;
    var lastIdx = buckets.length - 1;
    var candidates = [];
    for (var ci = 0; ci < buckets.length; ci += every) {
      candidates.push(ci);
    }
    if (candidates[candidates.length - 1] !== lastIdx) {
      candidates.push(lastIdx);
    }
    var geometry = function (i, isLast) {
      var anchor = i === 0 ? "start" : (isLast ? "end" : "middle");
      var tx = i === 0 ? left : (isLast ? width - right : x(i));
      var l = anchor === "start" ? tx : (anchor === "end" ? tx - labelW : tx - labelW / 2);
      return {i: i, anchor: anchor, tx: tx, left: l, right: l + labelW};
    };
    var kept = [];
    candidates.forEach(function (i) {
      var isLast = i === lastIdx, geo = geometry(i, isLast);
      if (isLast) {
        while (kept.length && geo.left - kept[kept.length - 1].right < 8) {
          kept.pop();
        }
      } else if (kept.length && geo.left - kept[kept.length - 1].right < 8) {
        return;
      }
      kept.push(geo);
    });
    kept.forEach(function (t) {
      out += '<text class="chart-label" text-anchor="' + t.anchor + '" x="' + t.tx + '" y="' + (height - 12) + '">' +
        esc(tickLabel(buckets[t.i].label)) + '</text>';
    });
    if (bars.length) {
      // 柱组必须居中在该桶折线点的时间位置上，否则柱会比线偏半个桶位；
      // 只做 SVG 边界保护，正常尺寸下不会触发。
      var slot = plotW / Math.max(1, buckets.length);
      var groupW = Math.min(slot * 0.7, 24);
      var barW = Math.max(1, groupW / bars.length);
      var baseY = top + plotH;
      buckets.forEach(function (b, i) {
        var groupX = Math.min(Math.max(x(i) - groupW / 2, 0), width - groupW);
        bars.forEach(function (s, j) {
          var value = pick(s, b, i);
          if (value === null) return;
          var yy = (dual ? yRight : yLeft)(value);
          var h = Math.max(1, baseY - yy);
          var barX = groupX + j * barW, barWidth = Math.max(1, barW - 1);
          var formatted = (dual ? rightFormat : leftFormat)(value);
          var seriesMeta = meta.bars.filter(function (m) { return m.key === s.label; })[0];
          if (!seriesMeta) {
            seriesMeta = {key: s.label, label: s.label, color: s.color, kind: "bar", rects: []};
            meta.bars.push(seriesMeta);
          }
          seriesMeta.rects.push({x: barX, y: yy, w: barWidth, h: h,
            bucket: b.label, value: value, formatted: formatted});
          out += '<rect class="chart-bar series-' + s.color + '" x="' + barX.toFixed(2) +
            '" y="' + yy.toFixed(2) + '" width="' + barWidth.toFixed(2) + '" height="' + h.toFixed(2) +
            '" data-series-key="' + esc(s.label) + '" data-series-color="' + esc(s.color) + '" aria-label="' +
            esc(b.label + " · " + s.label + " " + formatted) + '"></rect>';
        });
      });
    }
    lines.forEach(function (s) {
      var points = [], paths = [];
      var seriesMeta = {key: s.label, label: s.label, color: s.color, kind: "line", points: []};
      buckets.forEach(function (b, i) {
        var value = pick(s, b, i);
        if (value === null) {
          if (points.length) paths.push(points.join(" "));
          points = [];
          seriesMeta.points.push(null);
          return;
        }
        var xx = x(i), yy = yLeft(value);
        var formatted = tooltipFormat(value);
        points.push((points.length ? "L" : "M") + xx.toFixed(2) + "," + yy.toFixed(2));
        seriesMeta.points.push({x: xx, y: yy, bucket: b.label, value: value, formatted: formatted});
        out += '<circle class="series-' + s.color + '" cx="' + xx + '" cy="' + yy +
          '" r="' + (buckets.length > 80 ? 1.8 : 3.2) + '" data-series-key="' + esc(s.label) +
          '" data-series-color="' + esc(s.color) + '" aria-label="' +
          esc(b.label + " · " + s.label + " " + formatted) + '"></circle>';
      });
      if (points.length) paths.push(points.join(" "));
      if (seriesMeta.points.some(function (p) { return p !== null; })) meta.lines.push(seriesMeta);
      paths.forEach(function (p) {
        out += '<path class="chart-line series-' + s.color + '" data-series-key="' + esc(s.label) +
          '" data-series-color="' + esc(s.color) + '" d="' + p + '"/>';
      });
    });
    if (typeof opts.onGeometry === "function") opts.onGeometry(meta);
    return out + "</svg>";
  }

  function statsLineChart(buckets, series, title, format, box) {
    return statsComboChart(buckets, {lines: series, bars: [], title: title, leftFormat: format, box: box});
  }

  // 悬停命中：按鼠标到折线段/柱形的距离取最近系列，悬浮显示模型名和当前时间桶数值。
  var chartTooltipEl = null;

  function ensureChartTooltip() {
    if (chartTooltipEl) return chartTooltipEl;
    if (!document.createElement || !document.body) return null;
    chartTooltipEl = document.createElement("div");
    chartTooltipEl.className = "chart-tooltip";
    chartTooltipEl.hidden = true;
    document.body.appendChild(chartTooltipEl);
    return chartTooltipEl;
  }

  function hideChartTooltip() {
    if (chartTooltipEl) chartTooltipEl.hidden = true;
  }

  function chartDistanceToSegment(px, py, x1, y1, x2, y2) {
    var dx = x2 - x1, dy = y2 - y1;
    if (!dx && !dy) return Math.sqrt(Math.pow(px - x1, 2) + Math.pow(py - y1, 2));
    var t = ((px - x1) * dx + (py - y1) * dy) / (dx * dx + dy * dy);
    t = Math.max(0, Math.min(1, t));
    var x = x1 + t * dx, y = y1 + t * dy;
    return Math.sqrt(Math.pow(px - x, 2) + Math.pow(py - y, 2));
  }

  function chartDistanceToRect(px, py, rect) {
    var dx = Math.max(rect.x - px, 0, px - (rect.x + rect.w));
    var dy = Math.max(rect.y - py, 0, py - (rect.y + rect.h));
    return Math.sqrt(dx * dx + dy * dy);
  }

  function nearestChartHit(meta, px, py) {
    var lineBest = null, barBest = null;
    (meta.lines || []).forEach(function (series) {
      var points = series.points || [], previous = null;
      points.forEach(function (point) {
        if (!point) {
          previous = null;
          return;
        }
        var distance = previous ?
          chartDistanceToSegment(px, py, previous.x, previous.y, point.x, point.y) :
          Math.sqrt(Math.pow(px - point.x, 2) + Math.pow(py - point.y, 2));
        if (!lineBest || distance < lineBest.distance) {
          lineBest = {kind: "line", key: series.key, label: series.label, color: series.color,
            bucket: point.bucket, value: point.value, formatted: point.formatted, distance: distance};
        }
        previous = point;
      });
    });
    (meta.bars || []).forEach(function (series) {
      (series.rects || []).forEach(function (rect) {
        var distance = chartDistanceToRect(px, py, rect);
        if (!barBest || distance < barBest.distance) {
          barBest = {kind: "bar", key: series.key, label: series.label, color: series.color,
            bucket: rect.bucket, value: rect.value, formatted: rect.formatted, distance: distance};
        }
      });
    });
    // 折线是主要交互目标：靠近线时优先线；明确落在柱形内部时才让柱形接管。
    if (lineBest && lineBest.distance <= 6) return lineBest;
    if (barBest && barBest.distance <= 0.5) return barBest;
    if (lineBest && (!barBest || lineBest.distance < barBest.distance)) return lineBest;
    return barBest;
  }

  function clearChartHighlight(svg) {
    if (!svg) return;
    if (svg.classList) svg.classList.remove("chart-hovering");
    var nodes = svg.querySelectorAll ? svg.querySelectorAll(".is-hovered") : [];
    Array.prototype.forEach.call(nodes, function (node) {
      if (node.classList) node.classList.remove("is-hovered");
    });
  }

  function setChartHighlight(container, key) {
    var svg = container.querySelector ? container.querySelector("svg") : null;
    if (!svg || !svg.querySelectorAll) return;
    if (svg.classList) svg.classList.add("chart-hovering");
    Array.prototype.forEach.call(svg.querySelectorAll("[data-series-key]"), function (node) {
      if (!node.classList) return;
      node.classList.toggle("is-hovered", node.getAttribute("data-series-key") === key);
    });
  }

  function attachChartHover(container, meta) {
    if (!container) return;
    var svg = container.querySelector ? container.querySelector("svg") : null;
    if (container.__wbChartSvg) clearChartHighlight(container.__wbChartSvg);
    container.__wbChartSvg = svg;
    hideChartTooltip();
    if (!svg || !meta || !svg.getBoundingClientRect) {
      container.onmousemove = null;
      container.onmouseleave = null;
      return;
    }
    container.onmousemove = function (event) {
      var rect = svg.getBoundingClientRect();
      if (!rect || !rect.width || !rect.height) return;
      var px, py;
      // 用 SVG 的屏幕矩阵换算，避免浏览器把 height 渲染成 280.07 这类小数时，
      // 用宽高分别缩放造成亚像素偏差；几十条线挤在一起时这点偏差会命中错模型。
      if (svg.createSVGPoint && svg.getScreenCTM) {
        var matrix = svg.getScreenCTM();
        if (matrix) {
          var point = svg.createSVGPoint();
          point.x = event.clientX;
          point.y = event.clientY;
          var local = point.matrixTransform(matrix.inverse());
          px = local.x;
          py = local.y;
        }
      }
      if (px === undefined) {
        var scale = meta.width / rect.width;
        px = (event.clientX - rect.left) / scale;
        py = (event.clientY - rect.top) / scale;
      }
      var hit = nearestChartHit(meta, px, py);
      if (!hit || hit.distance > 14) {
        hideChartTooltip();
        clearChartHighlight(svg);
        return;
      }
      setChartHighlight(container, hit.key);
      var tip = ensureChartTooltip();
      if (!tip) return;
      var kind = hit.kind === "bar" ? meta.barKind : meta.lineKind;
      var detail = hit.bucket || "";
      if (kind && kind !== hit.label) detail += (detail ? " · " : "") + kind;
      tip.innerHTML = '<div class="chart-tooltip-head"><i class="series-dot ' + esc(hit.color) +
        '"></i><span>' + esc(hit.label) + '</span></div><div class="chart-tooltip-body">' +
        esc(detail) + (detail ? " " : "") + '<strong>' + esc(hit.formatted) + '</strong></div>';
      tip.hidden = false;
      var left = event.clientX + 14, top = event.clientY + 14;
      var vw = window.innerWidth || 0, vh = window.innerHeight || 0;
      var tw = tip.offsetWidth || 0, th = tip.offsetHeight || 0;
      if (vw && left + tw > vw - 8) left = Math.max(8, event.clientX - tw - 14);
      if (vh && top + th > vh - 8) top = Math.max(8, event.clientY - th - 14);
      tip.style.left = left + "px";
      tip.style.top = top + "px";
    };
    container.onmouseleave = function () {
      hideChartTooltip();
      clearChartHighlight(svg);
    };
  }

  function renderStatsChart(id, buckets, opts) {
    var el = $(id), meta = null, chartOpts = {};
    opts = opts || {};
    Object.keys(opts).forEach(function (key) { chartOpts[key] = opts[key]; });
    chartOpts.onGeometry = function (value) { meta = value; };
    el.innerHTML = statsComboChart(buckets, chartOpts);
    attachChartHover(el, meta);
  }

  function renderStats(data) {
    state.lastStats = data;
    var s = data.summary || {}, rows = data.buckets || [], store = data.storage || {};
    var currentTTFT = data.ttftMetric === "first_token_v2";
    var tokenTime = function (row) { return currentTTFT ? row.avgTtftMs : null; };
    $("statsStorage").textContent = store.mode === "file" ? "stats.json · 已落盘" :
      (store.mode === "memory" ? "仅内存 · 未落盘" : "等待首次落盘");
    $("statsStorage").className = "badge " + (store.mode === "memory" ? "warn" : "");
    var started = data.startedAt ? new Date(data.startedAt).toLocaleString("zh-CN", {hour12: false}) : "尚未采集";
    $("statsInfo").textContent = "自 " + started + " 起采集；UTC+8，周从周一开始。按实际上游账号调用统计，失败重试分开记。 " +
      (store.message || "缺失的 usage 不当作零消耗；缓存读取包含在输入 Token 中，不重复相加。") +
      " 首Token按有效内容增量计时，仅完整步骤参与平均；旧首响应体样本不混入（排除 " + (s.legacyTtftSamples || 0) + " 条）。不等同于 dsh 当前会话范围。";
    $("statsStart").value = data.start;
    $("statsEnd").value = data.end;
    updateStatsEndState();
    var choice = state.statsAccount || data.account || "";
    var siteScope = choice === "site:cn" || choice === "site:intl";
    var scopeOption = function (value, label) {
      return '<option value="' + esc(value) + '"' + (value === choice ? " selected" : "") + '>' + esc(label) + "</option>";
    };
    $("statsAccount").innerHTML = scopeOption("", "全局 · 全部账号") +
      scopeOption("site:intl", "国际站 · 全部国际账号") + scopeOption("site:cn", "国内站 · 全部国内账号") +
      (data.accounts || []).map(function (a) {
        return scopeOption(a.id, a.file + " · " + (a.edition === "intl" ? "国际" : "国内") + " · " + a.id.slice(0, 6));
      }).join("");
    if (!siteScope && !(data.accounts || []).some(function (a) { return a.id === choice; })) choice = "";
    $("statsAccount").value = choice;
    state.statsAccount = choice;
    var coverage = (s.tokenSamples || 0) + "/" + (s.requests || 0) + " 请求有 Token";
    $("usageSummary").innerHTML =
      statsCard("总消耗 Token", s.tokenSamples ? statsNumber(s.tokens) : "—", "输入 " + statsNumber(s.inputTokens) +
        " · 输出 " + statsNumber(s.outputTokens) + " · 缓存读取 " + statsNumber(s.cachedTokens), "token") +
      statsCard("总消耗积分", s.creditSamples ? statsCredit(s.credits) : "—", (s.creditSamples || 0) + "/" + (s.requests || 0) + " 请求有积分 · 仅计 usage.credit", "credit") +
      statsCard("请求成功率", s.requests ? ((s.success / s.requests) * 100).toFixed(1) + "%" : "—", "调用 " + statsNumber(s.requests) + " 次 · 完整 " + statsNumber(s.success) + " · 失败 / 不完整 " + statsNumber(s.failed), "neutral") +
      statsCard("平均总耗时", statsDuration(s.avgLatencyMs), coverage, "token") +
      statsCard("平均首Token (TTFT)", statsDuration(tokenTime(s)), (currentTTFT ? (s.ttftSamples || 0) : 0) + "/" + (s.requests || 0) + " 请求有新口径有效样本", "first");
    var chartBox = function (id) {
      var el = $(id), w = el && el.clientWidth ? el.clientWidth : 900;
      return {w: w, h: w < 520 ? 230 : 280};
    };
    renderStatsChart("usageChart", rows, {
      lines: [{label: "总 Token", color: "token", value: function (b) { return b.tokenSamples ? b.tokens : null; }}],
      bars: [{label: "积分", color: "credit", value: function (b) { return b.creditSamples ? b.credits : null; }}],
      title: "总 Token（折线，左轴）与积分（柱形，右轴）消耗趋势",
      lineKind: "总 Token", barKind: "积分",
      leftFormat: function (v) { return v >= 1000000 ? (v / 1000000).toFixed(2) + "M" : statsNumber(v); },
      rightFormat: statsCredit,
      box: chartBox("usageChart")
    });
    var latencyRows = data.modelLatency || [];
    var latencyLines = [], latencyBars = [], tokenLines = [];
    latencyRows.forEach(function (row, index) {
      var color = "m" + (index % 8), label = row.model || "未命名模型";
      latencyLines.push({label: label, color: color, value: function (_bucket, i) {
        var points = row.avgLatencyMs || [];
        return i < points.length ? points[i] : null;
      }});
      latencyBars.push({label: label, color: color, value: function (_bucket, i) {
        var points = row.avgTtftMs || [];
        return i < points.length ? points[i] : null;
      }});
      tokenLines.push({label: label, color: color, value: function (_bucket, i) {
        var points = row.tokens || [];
        return i < points.length ? points[i] : null;
      }});
    });
    $("latencyLegend").innerHTML = latencyLines.map(function (s) {
      return '<i class="series-dot ' + s.color + '"></i><span>' + esc(s.label) + "</span>";
    }).join("");
    renderStatsChart("latencyChart", rows, {
      lines: latencyLines, bars: latencyBars,
      title: "各模型请求耗时：柱形=首Token，折线=总耗时。按当前时间和账号分别计算",
      lineKind: "总耗时", barKind: "首Token",
      leftFormat: statsDuration,
      box: chartBox("latencyChart")
    });
    // Token 用量与耗时共用模型顺序和颜色，方便在同一批模型之间对照；缺失样本仍断线。
    $("tokenLegend").innerHTML = tokenLines.map(function (s) {
      return '<i class="series-dot ' + s.color + '"></i><span>' + esc(s.label) + "</span>";
    }).join("");
    renderStatsChart("tokenChart", rows, {
      lines: tokenLines, bars: [],
      title: "各模型 Token 用量趋势：按当前统计粒度（默认日）分别汇总，对数轴，不补零",
      lineKind: "Token",
      leftFormat: statsTokenAxis,
      tooltipFormat: statsNumber,
      logScale: true,
      box: chartBox("tokenChart")
    });
    // 明细改为按模型排名，跟随同一时间范围与账号筛选；服务端已按用量排序并剔除无调用模型。
    var ranked = data.models || [];
    $("statsRange").textContent = data.start + " → " + data.end + " · " + ranked.length + " 个模型有调用";
    $("statsTable").querySelector("tbody").innerHTML = ranked.length ? ranked.map(function (m, i) {
      return "<tr><td>" + (i + 1) + "</td><td class='mono'>" + esc(m.model) + "</td><td>" + esc(statsNumber(m.requests)) + "</td><td>" +
        esc(m.success + " / " + m.failed) + "</td><td>" + esc(m.tokenSamples ? statsNumber(m.tokens) : "—") + "</td><td>" +
        esc(m.tokenSamples ? statsNumber(m.inputTokens) + " / " + statsNumber(m.outputTokens) : "—") + "</td><td>" +
        esc(m.tokenSamples ? statsNumber(m.cachedTokens) : "—") + "</td><td>" +
        esc(m.creditSamples ? statsCredit(m.credits) : "—") + "</td><td>" + esc(statsDuration(m.avgLatencyMs)) + "</td><td>" +
        esc(statsDuration(tokenTime(m))) + "</td><td class='hint'>" + esc("Token " + m.tokenSamples + "/" + m.requests +
          " · 积分 " + m.creditSamples + "/" + m.requests + " · TTFT " + (currentTTFT ? m.ttftSamples : 0) + "/" + m.requests) + "</td></tr>";
    }).join("") : '<tr><td colspan="11" class="empty">所选范围没有模型调用记录</td></tr>';
  }

  function loadStats(force) {
    if (!state.apiKey || state.statsPending || (!force && Date.now() - state.statsLastPoll < 10000)) return Promise.resolve();
    state.statsPending = true;
    var version = state.statsVersion, key = state.apiKey;
    var params = ["period=" + encodeURIComponent($("statsPeriod").value || "day")];
    var scope = state.statsAccount || "";
    if (scope.indexOf("site:") === 0) params.push("site=" + encodeURIComponent(scope.slice(5)));
    else if (scope) params.push("account=" + encodeURIComponent(scope));
    // 未自定义开始日期时不传 start，交给服务端按滚动区间计算；
    // 勾选“跟随今日”时不传 end，服务端每次查询都会把结束日期算成今天。
    if (state.statsStartTouched) {
      var start = $("statsStart").value;
      if (start) params.push("start=" + encodeURIComponent(start));
    }
    if (!state.statsFollowToday && state.statsEndTouched) {
      var end = $("statsEnd").value;
      if (end) params.push("end=" + encodeURIComponent(end));
    }
    $("statsInfo").textContent = "正在读取统计；图表与表格保持上次数据…";
    return api("/admin/api/stats?" + params.join("&")).then(function (data) {
      if (version !== state.statsVersion || key !== state.apiKey) return;
      renderStats(data);
      state.statsLastPoll = Date.now();
    }).catch(function (err) {
      if (err && err.status === 400 && state.statsAccount) {
        // 上次记住的账号已被删除时，退回全局查询而不是一直报错。
        state.statsAccount = "";
        rememberStatsFilter();
        state.statsVersion++;
        return;
      }
      if (version === state.statsVersion && key === state.apiKey) $("statsInfo").textContent = "统计读取失败，图表保留上次结果；请检查筛选日期或稍后重试。";
    }).then(function () {
      state.statsPending = false;
      if (version !== state.statsVersion && state.apiKey) return loadStats(true);
    });
  }

  function updateStatsEndState() {
    var follow = $("statsFollowToday"), end = $("statsEnd");
    if (follow) follow.checked = !!state.statsFollowToday;
    if (end) {
      end.disabled = !!state.statsFollowToday;
      end.title = state.statsFollowToday
        ? "结束日期自动跟随今天；取消勾选「跟随今日」后可手动选择固定日期"
        : "固定结束日期，不会自动变化";
    }
  }

  function changeStatsFilter(resetDates) {
    state.statsVersion++;
    state.statsLastPoll = 0;
    if (resetDates) {
      $("statsStart").value = "";
      $("statsEnd").value = statsToday();
      state.statsStartTouched = false;
      state.statsEndTouched = false;
      state.statsFollowToday = true;
      updateStatsEndState();
    }
    state.statsAccount = $("statsAccount").value || "";
    rememberStatsFilter();
    return loadStats(true);
  }

  var SITE_LABEL = { cn: "国内站", intl: "国际站" };

  function renderStatus(data) {
    state.lastStatus = data;
    var g = data.gateway || {};
    $("meta").innerHTML =
      '<a class="chip chip-link" href="https://github.com/CangShui/workbuddy-gateway"' +
      ' target="_blank" rel="noreferrer" title="在新标签页打开项目地址">v' + esc(g.version) + '</a>' +
      '<span class="chip">' + esc(g.listen) + '</span>' +
      '<span class="chip">模型源 ' + esc(g.modelSource || "unavailable") + '</span>' +
      '<span class="chip">更新 ' + esc(fmtTime(data.updatedAt)) + '</span>';

    renderPoolHealth(data);
    if (state.lastAccountAlerts) renderAccountAlerts(state.lastAccountAlerts);

    $("gatewayInfo").innerHTML = kvRow("版本", g.version) +
      kvRow("监听地址", g.listen) +
      kvRow("API 鉴权", g.apiKeyEnabled ? "已启用" : "未启用") +
      kvRow("管理台", g.webuiEnabled ? "已启用" : "未启用") +
      kvRow("模型来源", g.modelSource || "unavailable") +
      kvRow("快照更新", fmtTime(data.updatedAt));

    renderAccounts(data.accounts || []);
    if (data.stale) showAlert("状态快照超过 15 秒未更新，当前展示的是上次数据，请检查网关状态。");
  }

  function kvRow(k, v) {
    return '<div class="kv-row"><span class="kv-k">' + esc(k) + '</span><span class="kv-v">' + esc(v) + '</span></div>';
  }

  var STATE_TEXT = {
    active: "可用", cooldown: "冷却", paid_exhausted: "付费耗尽",
    quota_exhausted: "付费耗尽", expired: "已过期", disabled: "失效"
  };
  var STATE_CLASS = {
    active: "ok", cooldown: "warn", paid_exhausted: "warn",
    quota_exhausted: "warn", expired: "warn", disabled: "danger"
  };

  // Cookie 仅保存两个显示偏好，不包含管理 Key、凭据、配置或账号信息。
  // 统计筛选（粒度/范围/起止日期）记在 Cookie 里，退出登录或重新打开页面都能恢复。
  // 只保存用户真正改过的日期：没动过就留空，继续用默认的滚动区间，避免把默认区间冻结住。
  var STATS_FILTER_COOKIE = "gateway_stats_filter";

  function readCookieRaw(name) {
    try {
      var cookies = String(document.cookie || "").split(";");
      for (var i = 0; i < cookies.length; i++) {
        var pair = cookies[i].trim();
        if (pair.indexOf(name + "=") === 0) return decodeURIComponent(pair.slice(name.length + 1));
      }
    } catch (_) { /* Cookie 被禁用时按默认值渲染，不影响登录与查询 */ }
    return "";
  }

  function writeCookieRaw(name, value) {
    try {
      document.cookie = name + "=" + encodeURIComponent(value) +
        "; Path=/ui; Max-Age=31536000; SameSite=Lax" +
        (window.location.protocol === "https:" ? "; Secure" : "");
      return readCookieRaw(name) === value;
    } catch (_) { return false; }
  }

  function readStatsFilterCookie() {
    var raw = readCookieRaw(STATS_FILTER_COOKIE);
    if (!raw) return null;
    try {
      var saved = JSON.parse(raw);
      if (!saved || typeof saved !== "object") return null;
      var periods = ["hour", "day", "week", "month"];
      return {
        period: periods.indexOf(saved.period) > -1 ? saved.period : "",
        account: typeof saved.account === "string" ? saved.account : "",
        start: typeof saved.start === "string" ? saved.start : "",
        end: typeof saved.end === "string" ? saved.end : "",
        // 旧 Cookie 没有这个字段：按“跟随今日”处理，避免把旧版冻结的结束日期继续带下来。
        followToday: saved.followToday !== false
      };
    } catch (_) { return null; }
  }

  function writeStatsFilterCookie() {
    return writeCookieRaw(STATS_FILTER_COOKIE, JSON.stringify({
      period: $("statsPeriod").value || "day",
      account: state.statsAccount || "",
      followToday: !!state.statsFollowToday,
      start: state.statsStartTouched ? ($("statsStart").value || "") : "",
      end: (!state.statsFollowToday && state.statsEndTouched) ? ($("statsEnd").value || "") : ""
    }));
  }

  function initStatsFilter() {
    var saved = readStatsFilterCookie();
    if (!saved) {
      $("statsEnd").value = statsToday();
      updateStatsEndState();
      return;
    }
    if (saved.period) $("statsPeriod").value = saved.period;
    state.statsAccount = saved.account;
    state.statsFollowToday = saved.followToday !== false;
    state.statsStartTouched = !!saved.start;
    // 旧 Cookie 的 end 可能是旧版自动写进去的冻结值；followToday 缺失时不再恢复它。
    state.statsEndTouched = !state.statsFollowToday && !!saved.end;
    if (saved.start) $("statsStart").value = saved.start;
    $("statsEnd").value = state.statsEndTouched ? saved.end : statsToday();
    updateStatsEndState();
    var hint = $("statsFilterStatus");
    if (hint) {
      hint.textContent = state.statsFollowToday
        ? "已恢复上次的统计筛选；结束日期默认跟随今日，取消勾选后可固定。"
        : "已恢复上次的统计筛选；结束日期已固定，改完点「查询」即可生效。";
      hint.classList.remove("hidden");
    }
  }

  function rememberStatsFilter() {
    var remembered = writeStatsFilterCookie();
    var hint = $("statsFilterStatus");
    if (hint) {
      hint.textContent = remembered ? "" : "统计筛选已应用，但浏览器没能保存 Cookie，刷新后可能记不住这次选择。";
      hint.classList.toggle("hidden", remembered);
    }
    return remembered;
  }

  function readListFilterCookie(name) {
    try {
      var cookies = String(document.cookie || "").split(";");
      for (var i = 0; i < cookies.length; i++) {
        var pair = cookies[i].trim();
        if (pair.indexOf(name + "=") !== 0) continue;
        var value = pair.slice(name.length + 1);
        return value === "1" ? true : value === "0" ? false : null;
      }
    } catch (_) { /* Cookie 被禁用时回落到显示全部，不影响管理登录。 */ }
    return null;
  }

  function writeListFilterCookie(name, enabled) {
    try {
      document.cookie = name + "=" + (enabled ? "1" : "0") +
        "; Path=/ui; Max-Age=31536000; SameSite=Lax" +
        (window.location.protocol === "https:" ? "; Secure" : "");
      return readListFilterCookie(name) === enabled;
    } catch (_) { return false; }
  }

  function initListFilters() {
    LIST_FILTERS.forEach(function (filter) {
      $(filter.id).checked = readListFilterCookie(filter.cookie) === true;
      $(filter.id).addEventListener("change", function () { changeListFilter(filter.kind); });
    });
  }

  // 「显示已屏蔽模型」是纯前端筛选，不参与服务端列表过滤事件；默认开启，让屏蔽项可见。
  var SHOW_BLOCKED_COOKIE = "gateway_models_show_blocked";
  function initShowBlockedFilter() {
    var box = $("showBlocked");
    box.checked = readListFilterCookie(SHOW_BLOCKED_COOKIE) !== false;
    box.addEventListener("change", function () {
      writeListFilterCookie(SHOW_BLOCKED_COOKIE, box.checked);
      // 只显示可用模型优先级更高，勾选它会隐藏屏蔽项，这里给出明确提示。
      if (box.checked && $("onlyUsable").checked) {
        showAlert("「只显示可用模型」仍处于勾选状态，已屏蔽的模型不会显示。取消该勾选即可看到。");
      }
      if (state.lastModels) renderModels(state.lastModels);
    });
  }

  function reportRestoredListFilters() {
    LIST_FILTERS.forEach(function (filter) {
      reportUIEvent("list_filter_restored", filter.route, 0, traceID(), $(filter.id).checked);
    });
  }

  function changeListFilter(kind) {
    var filter = LIST_FILTERS.find(function (item) { return item.kind === kind; });
    if (!filter) return;
    var trace = traceID();
    var enabled = $(filter.id).checked;
    var remembered = writeListFilterCookie(filter.cookie, enabled);
    var hint = $(filter.statusID);
    hint.textContent = remembered ? "" : "本次筛选已应用，但浏览器未允许保存 Cookie，刷新后可能无法记住状态。";
    hint.classList.toggle("hidden", remembered);
    reportUIEvent("list_filter_changed", filter.route, 0, trace, enabled);
    if (!remembered) reportUIEvent("list_filter_cookie_failed", filter.route, 0, trace, enabled);
    // 筛选立即作用于已加载的数据；刷新中的响应也按当前勾选状态渲染。
    if (kind === "accounts") {
      if (state.lastStatus) renderAccounts(state.lastStatus.accounts || []);
    } else if (state.lastModels) {
      renderModels(state.lastModels);
    }
  }

  // ---- 账号：网页新增（复用 CLI 登录流程）与删除 ---------------------------
  function clearAddAccountArtifacts() {
    $("addAccountQr").removeAttribute("src");
    $("addAccountLink").href = "#";
    $("addAccountLink").textContent = "";
    var copy = $("addAccountCopy");
    if (copy) {
      copy.disabled = true;
      copy.textContent = "复制链接";
    }
  }

  function fallbackCopyText(text, done) {
    var area = document.createElement("textarea");
    area.value = text;
    area.setAttribute("readonly", "");
    area.style.position = "fixed";
    area.style.top = "-1000px";
    area.style.opacity = "0";
    document.body.appendChild(area);
    area.select();
    var ok = false;
    try {
      ok = document.execCommand("copy");
    } catch (_) {
      ok = false;
    }
    document.body.removeChild(area);
    done(ok);
  }

  function copyAddAccountLink() {
    var link = $("addAccountLink");
    var text = (link.textContent || "").replace(/^\s+|\s+$/g, "");
    if (!text) return;
    var button = $("addAccountCopy");
    var done = function (ok) {
      button.textContent = ok ? "已复制" : "复制失败";
      if (!ok) {
        // 非安全上下文或浏览器拒绝剪贴板权限时，至少把链接选中，方便手动复制。
        try {
          var range = document.createRange();
          range.selectNodeContents(link);
          var selection = window.getSelection();
          selection.removeAllRanges();
          selection.addRange(range);
        } catch (_) { /* 选中失败不影响提示 */ }
      }
      window.setTimeout(function () { button.textContent = "复制链接"; }, 1500);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      try {
        navigator.clipboard.writeText(text).then(function () {
          done(true);
        }, function () {
          fallbackCopyText(text, done);
        });
        return;
      } catch (_) { /* 继续走 execCommand 回退 */ }
    }
    fallbackCopyText(text, done);
  }

  function addAccountRemainingSeconds() {
    if (!state.addAccountDeadline) return 0;
    return Math.max(0, Math.ceil((state.addAccountDeadline - Date.now()) / 1000));
  }

  function fmtAddAccountRemaining(seconds) {
    var m = Math.floor(seconds / 60);
    var s = seconds % 60;
    return m + ":" + (s < 10 ? "0" : "") + s;
  }

  function showAddAccountPending() {
    var left = addAccountRemainingSeconds();
    $("addAccountStatus").textContent = "请用手机扫码完成授权；成功后凭据会自动保存并加载。" +
      (left > 0 ? "剩余 " + fmtAddAccountRemaining(left) + "，超时后需重新获取。"
        : "登录链接即将失效，请重新获取。");
  }

  function showAddAccountExpired() {
    state.addAccountId = "";
    state.addAccountPoll = 0;
    state.addAccountDeadline = 0;
    clearAddAccountArtifacts();
    $("addAccountStatus").textContent = "登录链接已失效，请重新点击「获取登录接口」。";
  }

  function openAddAccount() {
    $("addAccountPanel").classList.remove("hidden");
    $("addAccountResult").classList.add("hidden");
    clearAddAccountArtifacts();
    $("addAccountStatus").textContent = "正在读取默认凭据名…";
    state.addAccountId = "";
    state.addAccountPoll = 0;
    state.addAccountDeadline = 0;
    return api("/admin/api/account-add").then(function (data) {
      $("addFile").value = data.defaultFile || "";
      $("addAccountStatus").textContent = "";
    }).catch(function () {
      $("addAccountStatus").textContent = "读取默认凭据名失败，可手动填写。";
    });
  }

  function closeAddAccount() {
    var id = state.addAccountId;
    state.addAccountId = "";
    state.addAccountPoll = 0;
    state.addAccountDeadline = 0;
    $("addAccountPanel").classList.add("hidden");
    $("addAccountResult").classList.add("hidden");
    clearAddAccountArtifacts();
    if (id) return api("/admin/api/account-cancel", {method: "POST", body: {id: id}}).catch(function () {});
    return Promise.resolve();
  }

  function startAddAccount() {
    var site = $("addSite").value === "intl" ? "intl" : "cn";
    var file = ($("addFile").value || "").replace(/^\s+|\s+$/g, "");
    if (!file) { $("addAccountStatus").textContent = "请先填写凭据名。"; return; }
    if (state.addAccountId) closeAddAccount();
    $("addStart").disabled = true;
    $("addAccountStatus").textContent = "正在获取登录接口…";
    return api("/admin/api/account-add", {method: "POST", body: {site: site, file: file}}).then(function (data) {
      state.addAccountId = data.id || "";
      state.addAccountPoll = 0;
      state.addAccountDeadline = data.remainingSeconds
        ? Date.now() + Number(data.remainingSeconds) * 1000
        : (data.expiresAt ? Number(data.expiresAt) * 1000 : 0);
      $("addAccountResult").classList.remove("hidden");
      $("addAccountQr").src = data.qrPng || "";
      $("addAccountLink").href = data.authUrl || "#";
      $("addAccountLink").textContent = data.authUrl || "";
      $("addAccountCopy").disabled = !data.authUrl;
      showAddAccountPending();
      pollAddAccount();
    }).catch(function (err) {
      $("addAccountStatus").textContent = "获取登录接口失败：" + ((err && err.detail) || "请稍后重试");
    }).then(function () { $("addStart").disabled = false; });
  }

  function pollAddAccount(force) {
    if (!state.addAccountId || !state.apiKey) return Promise.resolve();
    if (!force && Date.now() - state.addAccountPoll < 2000) return Promise.resolve();
    state.addAccountPoll = Date.now();
    var id = state.addAccountId;
    return api("/admin/api/account-add?id=" + encodeURIComponent(id), {quiet: true}).then(function (data) {
      if (state.addAccountId !== id) return;
      var status = data.status || "pending";
      if (status === "pending") {
        if (typeof data.remainingSeconds === "number") {
          state.addAccountDeadline = Date.now() + data.remainingSeconds * 1000;
        }
        showAddAccountPending();
        return;
      }
      state.addAccountId = "";
      state.addAccountDeadline = 0;
      if (status === "done") {
        $("addAccountStatus").textContent = "登录成功，凭据已保存为 " + (data.savedAs || "") + "，账号池已自动加载。";
        showAlert("账号已添加：" + (data.savedAs || ""), true);
        refreshActive();
      } else if (status === "expired") {
        showAddAccountExpired();
      } else if (status === "cancelled") {
        clearAddAccountArtifacts();
        $("addAccountStatus").textContent = "登录已取消。";
      } else {
        clearAddAccountArtifacts();
        $("addAccountStatus").textContent = "登录失败：" + (data.error || "请重试");
      }
    }).catch(function (err) {
      if (state.addAccountId !== id) return;
      if (err && err.status === 404) {
        showAddAccountExpired();
        return;
      }
      $("addAccountStatus").textContent = "查询登录进度失败，保留上次结果。";
    });
  }

  function accountRowActions(event) {
    var target = event && event.target;
    if (!target || typeof target.closest !== "function") return;
    var btn = target.closest("[data-account-delete]");
    if (!btn) return;
    var name = btn.getAttribute("data-account-delete");
    if (!name) return;
    if (!window.confirm("确定删除凭据 " + name + " ？\n\n" +
        "会从账号池移除该账号，并删除运行目录下的对应凭据文件。统计与历史日志保留，操作不可撤销。")) return;
    state.policyVersion++;
    api("/admin/api/account-delete", {method: "POST", body: {file: name}}).then(function () {
      showAlert("已删除凭据 " + name + "，账号池已同步移除。", true);
      return refreshActive();
    }).catch(function (err) {
      showAlert("删除失败：" + ((err && err.detail) || "请稍后重试"), false);
    });
  }

  function renderAccounts(list) {
    var onlyUsable = $("onlyUsableAccounts").checked;
    if (onlyUsable) {
      list = list.filter(function (account) {
        return account.state === "active" && account.schedulingEnabled !== false;
      });
    }
    var tb = $("accountsTable").querySelector("tbody");
    var focusTarget = focusedPolicyTarget(tb);
    if (!list.length) {
      tb.innerHTML = '<tr><td colspan="16" class="empty">' +
        (onlyUsable ? "没有符合条件的可用账号，取消勾选可查看全部账号。" : "暂无账号数据（服务是否已写入状态快照？）") + '</td></tr>';
      return;
    }
    tb.innerHTML = list.map(function (a, i) {
      var st = STATE_TEXT[a.state] || a.state || "-";
      var cls = STATE_CLASS[a.state] || "";
      if (a.schedulingEnabled === false) {
        st = a.state === "active" ? "已停用" : "已停用 · " + st;
        cls = "warn";
      }
      var site = a.edition === "intl" ? "国际站" : "国内站";
      return "<tr>" +
        "<td>" + (i + 1) + "</td>" +
        "<td>" + policySwitch("account", a.path, a.schedulingEnabled !== false, a.schedulingReason) + "</td>" +
        "<td class='mono'>" + esc(a.path) + "</td>" +
        "<td>" + esc(a.nickname || "-") + "</td>" +
        "<td>" + esc(site) + "</td>" +
        "<td><span class='badge " + cls + "'>" + esc(st) + "</span></td>" +
        "<td>" + esc(fmtTime(a.tokenExpiresAt)) + "</td>" +
        "<td>" + esc(quotaAmount(a, "total")) + "</td>" +
        "<td>" + esc(quotaAmount(a, "used")) + "</td>" +
        "<td>" + esc(quotaAmount(a, "remaining")) + "</td>" +
        "<td>" + esc((a.planLabel || "-") + (a.planStale ? "（待刷新）" : "")) + "</td>" +
        "<td>" + esc(count(a.freeModels)) + "</td>" +
        "<td>" + esc(count(a.modelCooldowns)) + "</td>" +
        "<td>" + esc(fmtTime(a.lastRefreshTime)) + "</td>" +
        "<td>" + esc(count(a.refreshFailCount)) + "</td>" +
        "<td><button class=\"ghost danger\" type=\"button\" data-account-delete=\"" +
        esc(a.path) + "\">删除</button></td>" +
        "</tr>";
    }).join("");
    restorePolicyFocus(tb, focusTarget);
  }

  function quotaAmount(account, kind) {
    if (!account.quotaKnown) return "-";
    if (account.quotaUnlimited && (kind === "total" || kind === "remaining")) return "不限";
    var value = kind === "total" ? account.quotaTotal : (kind === "used" ? account.quotaUsed : account.quotaRemaining);
    return count(value);
  }

  // ---- 渲染：模型 --------------------------------------------------------
  function focusedPolicyTarget(table) {
    var element = document.activeElement;
    return element && table.contains(element) && element.classList.contains("policy-switch") ?
      element.getAttribute("data-target") : null;
  }

  function restorePolicyFocus(table, target) {
    if (target === null) return;
    table.querySelectorAll(".policy-switch").forEach(function (button) {
      if (button.getAttribute("data-target") === target) button.focus({preventScroll: true});
    });
  }

  function policySwitch(kind, target, enabled, reason) {
    return '<button type="button" role="switch" class="policy-switch" data-kind="' + esc(kind) +
      '" data-target="' + esc(target) + '" aria-checked="' + enabled + '" aria-label="' +
      esc((kind === "account" ? "账号 " : "模型 ") + target + " 调度开关") +
      '" title="' + esc(reason || (enabled ? "允许调度" : "已停用")) + '"' +
      (state.togglePending ? " disabled" : "") + '><span class="switch-track" aria-hidden="true"></span>' +
      (enabled ? "已开启" : "已停用") + '</button>';
  }

  function togglePolicy(event) {
    var button = event.target.closest("button.policy-switch");
    if (!button || state.togglePending || !state.apiKey) return;
    var kind = button.getAttribute("data-kind");
    var target = button.getAttribute("data-target");
    var enabled = button.getAttribute("aria-checked") !== "true";
    var endpoint = "/admin/api/" + kind + "-toggle";
    state.togglePending = true;
    state.policyVersion++;
    document.querySelectorAll(".policy-switch").forEach(function (b) { b.disabled = true; });
    return api(endpoint, {method: "POST", body: {target: target, enabled: enabled}})
      .then(function () {
        // 保存成功才修改控件，不将失败伪装成已生效。
        button.setAttribute("aria-checked", String(enabled));
        button.lastChild.textContent = enabled ? "已开启" : "已停用";
        showAlert("已写入 config.json，" + (kind === "account" ? "账号全局调度" : "模型") +
          (enabled ? "已开启。" : "已停用。") + "已有请求不被中断。", true);
      }).catch(function () {}).then(function () {
        state.togglePending = false;
        state.policyVersion++;
        document.querySelectorAll(".policy-switch").forEach(function (b) { b.disabled = false; });
        refreshActive();
      });
  }

  // ---- 模型 ID 一键复制 ---------------------------------------------------
  function copyModelID(event) {
    var button = event.target.closest("button.copy-model");
    if (!button) return;
    var text = button.getAttribute("data-copy-model") || "";
    if (!text) return;
    var done = function (ok) {
      button.textContent = ok ? "已复制" : "复制失败";
      button.classList.toggle("copied", ok);
      button.classList.toggle("copy-failed", !ok);
      window.setTimeout(function () {
        button.textContent = "复制";
        button.classList.remove("copied", "copy-failed");
      }, 1400);
    };
    if (navigator.clipboard && navigator.clipboard.writeText) {
      try {
        navigator.clipboard.writeText(text).then(function () {
          done(true);
        }, function () {
          fallbackCopyText(text, done);
        });
        return;
      } catch (_) { /* 继续走 execCommand 回退 */ }
    }
    fallbackCopyText(text, done);
  }

  // ---- 解除屏蔽：从 models.blocklist 中移除 -------------------------------
  function unblockModel(event) {
    var button = event.target.closest("button.unblock-model");
    if (!button || state.togglePending || !state.apiKey) return;
    var target = button.getAttribute("data-target");
    if (!target) return;
    state.togglePending = true;
    state.policyVersion++;
    button.disabled = true;
    button.textContent = "处理中…";
    return api("/admin/api/model-toggle", {method: "POST", body: {target: target, enabled: true}})
      .then(function () {
        showAlert("已从屏蔽列表移除：" + target + "，模型已恢复可用。", true);
      }).catch(function () {}).then(function () {
        state.togglePending = false;
        state.policyVersion++;
        refreshActive();
      });
  }

  // 站点覆盖角标：来自后端模型目录的 siteSupport 字段，目录刷新后自动变化。
  function siteBadge(m) {
    var key = m.siteSupport || "";
    var labels = {both: "双站", cn: "国内站", intl: "国际站"};
    var label = labels[key];
    if (!label) return "";
    var detail = key === "both" ? "国内站 + 国际站" : (key === "cn" ? "国内站" : "国际站");
    var tip = "上游模型目录覆盖：" + detail +
      (m.source ? "；目录更新时间 " + m.source : "") +
      "。仅表示目录覆盖，不代表当前一定有该站点的可用账号。";
    return "<span class='badge site-badge " + esc(key) + "' title='" + esc(tip) + "'>" + label + "</span>";
  }

  function renderModels(data) {
    state.lastModels = data;
    var all = data.models || [];
    var onlyUsable = $("onlyUsable").checked;
    var showBlocked = $("showBlocked").checked;
    var blockedCount = all.filter(function (m) { return m.enabled === false; }).length;
    $("blockedCount").textContent = blockedCount;
    $("blockedCount").classList.toggle("hidden", blockedCount === 0);
    var list = all;
    if (onlyUsable) {
      list = list.filter(function (m) { return m.enabled !== false && (m.availableAccounts || 0) > 0; });
    } else if (!showBlocked) {
      list = list.filter(function (m) { return m.enabled !== false; });
    }
    // 已屏蔽的模型排在最后，避免和正常模型混在一起。sort 在现代浏览器中稳定，不影响原有顺序。
    list = list.slice().sort(function (a, b) {
      return (a.enabled === false ? 1 : 0) - (b.enabled === false ? 1 : 0);
    });
    var tb = $("modelsTable").querySelector("tbody");
    var focusTarget = focusedPolicyTarget(tb);
    $("modelPolicyHint").textContent = data.policyMode === "allowlist" ?
      "当前使用模型白名单，开关反映白名单规则。首次修改开关会删除 models.allowlist，将当前模型列表的启停状态转为 models.blocklist；以后新增模型默认开启。" :
      "模型开关写入 models.blocklist 并立即生效；被屏蔽的模型会显示在列表底部并标注「已屏蔽」，点「解除屏蔽」即可恢复。";
    if (!list.length) {
      tb.innerHTML = '<tr><td colspan="10" class="empty">' +
        (onlyUsable ? "没有符合条件的可用模型，取消勾选可查看全部模型。" : "暂无模型数据") + '</td></tr>';
      return;
    }
    tb.innerHTML = list.map(function (m) {
      var blocked = m.enabled === false;
      return "<tr" + (blocked ? " class='model-blocked'" : "") + ">" +
        "<td>" + policySwitch("model", m.id, !blocked, m.policyReason) + "</td>" +
        "<td class='mono model-id-cell'><span class='model-id'>" + esc(m.id) + "</span>" +
        siteBadge(m) +
        "<button type='button' class='copy-model' data-copy-model='" + esc(m.id) +
        "' title='复制模型 ID' aria-label='复制模型 ID " + esc(m.id) + "'>复制</button>" +
        (blocked ? "<span class='badge danger blocked-badge'>已屏蔽</span>" +
          "<button type='button' class='ghost unblock-model' data-target='" + esc(m.id) +
          "' title='从 models.blocklist 移除并恢复该模型'>解除屏蔽</button>" : "") +
        "</td>" +
        rateCell("cn", m.id, m.cnMultiplier) +
        rateCell("intl", m.id, m.intlMultiplier) +
        "<td>" + esc(count(m.availableAccounts)) + "</td>" +
        "<td>" + esc(count(m.requests)) + "</td>" +
        "<td>" + esc(fmtMs(m.avgTtftMs, m.hasTtft && m.ttftMetric === "first_token_v2")) + "</td>" +
        "<td>" + esc(fmtMs(m.avgLatencyMs, m.hasLatency)) + "</td>" +
        "<td>" + esc(fmtTokensM(m.tokens)) + "</td>" +
        "<td>" + esc(m.lastStatus || "-") + "</td>" +
        "</tr>";
    }).join("");
    restorePolicyFocus(tb, focusTarget);
    restoreRateDetails(tb);
    if (data.stale) showAlert("模型统计快照过旧，当前展示的是上次数据。");
  }

  // ---- 倍率来源：全部取自普通日志，没有记录就显示「没有日志」 -------------------
  function rateKey(site, model) {
    return site + "|" + String(model || "").trim().toLowerCase();
  }

  function rateCell(site, model, text) {
    var label = site === "cn" ? "国内" : "国际";
    return "<td class='rate-cell'><div class='rate-line'><span class='rate-value'>" + esc(text || "-") +
      "</span><button type='button' class='rate-info' data-rate-site='" + esc(site) +
      "' data-rate-model='" + esc(model) + "' aria-expanded='false' aria-label='查看" + label +
      "倍率数据来源与更新时间'>!</button></div>" +
      "<div class='rate-detail hidden' role='region' aria-label='" + label + "倍率来源详情'></div></td>";
  }

  function ensureRateDetails() {
    if (state.rateDetails && Date.now() - state.rateDetailsAt < 60000) return Promise.resolve(state.rateDetails);
    if (state.rateDetailsPending) return state.rateDetailsPending;
    state.rateDetailsPending = api("/admin/api/rate-detail").then(function (data) {
      state.rateDetails = data;
      state.rateDetailsAt = Date.now();
      state.rateDetailsPending = null;
      return data;
    }).catch(function (err) {
      state.rateDetailsPending = null;
      throw err;
    });
    return state.rateDetailsPending;
  }

  function rateDetailHTML(payload, site, model) {
    var info = payload && payload.rates ? payload.rates[rateKey(site, model)] : null;
    if (!info || !info.hasLog) {
      return "<p class='rate-none'>没有日志</p><p class='hint'>" +
        esc(payload && payload.noLogHint || "普通日志中没有该模型的倍率记录。") + "</p>";
    }
    // 不再标注「状态：未知」：该结论是实测过的，写「未知」会让人误以为从未检测。
    var meta = "<div class='rate-meta'><span>来源：" + esc(info.source) + "</span>" +
      "<span>更新时间：" + esc(fmtTime(info.updatedAt)) + "</span></div>";
    var html = meta;
    if (info.explain) html += "<p class='rate-explain'>" + esc(info.explain) + "</p>";
    if (info.unknown && info.refreshHint) {
      html += "<p class='rate-refresh'><strong>如何刷新：</strong>" + esc(info.refreshHint) + "</p>";
    }
    var others = info.others || [];
    if (others.length) {
      html += "<div class='rate-others'><div class='rate-logs-title'>其他记录</div><ul>" +
        others.map(function (record) {
          return "<li>" + esc(record.source) + " · " + esc(fmtTime(record.updatedAt)) + " · " +
            esc(record.explain || record.value || "") + "</li>";
        }).join("") + "</ul></div>";
    }
    var raw = (info.records || []).filter(function (record) { return record.logLine; });
    if (raw.length) {
      // 轮询会重建表格，展开状态必须自己记住，否则每次刷新都会被自动折叠。
      var rawOpen = state.openRawLogs[rateKey(site, model)] === true;
      html += "<details class='rate-raw'" + (rawOpen ? " open" : "") +
        "><summary>原始日志（" + raw.length + " 条，默认折叠）</summary><ul>" +
        raw.map(function (record) { return "<li><code>" + esc(record.logLine) + "</code></li>"; }).join("") +
        "</ul></details>";
    }
    return html;
  }

  function fillRateDetail(button, panel) {
    var site = button.getAttribute("data-rate-site");
    var model = button.getAttribute("data-rate-model");
    panel.classList.remove("hidden");
    button.setAttribute("aria-expanded", "true");
    panel.innerHTML = "<p class='hint'>读取日志中…</p>";
    ensureRateDetails().then(function (payload) {
      panel.innerHTML = rateDetailHTML(payload, site, model);
      // details 的 toggle 事件不冒泡，只能在渲染后逐个绑定，用于记住展开状态。
      var details = panel.querySelector("details.rate-raw");
      if (details) {
        details.addEventListener("toggle", function () {
          var key = rateKey(site, model);
          if (details.open) {
            state.openRawLogs[key] = true;
          } else {
            delete state.openRawLogs[key];
          }
        });
      }
    }).catch(function () {
      panel.innerHTML = "<p class='hint'>日志读取失败，请稍后重试；没有数据时不猜测来源。</p>";
    });
  }

  function toggleRateDetail(event) {
    var button = event.target.closest("button.rate-info");
    if (!button) return;
    var cell = button.closest("td");
    var panel = cell ? cell.querySelector(".rate-detail") : null;
    if (!panel) return;
    var key = rateKey(button.getAttribute("data-rate-site"), button.getAttribute("data-rate-model"));
    if (button.getAttribute("aria-expanded") === "true") {
      button.setAttribute("aria-expanded", "false");
      panel.classList.add("hidden");
      panel.innerHTML = "";
      delete state.openRateKeys[key];
      return;
    }
    state.openRateKeys[key] = true;
    fillRateDetail(button, panel);
  }

  // 轮询会重建表格，已展开的详情按 key 恢复，避免每次刷新都折叠。
  function restoreRateDetails(table) {
    Array.prototype.forEach.call(table.querySelectorAll("button.rate-info"), function (button) {
      var key = rateKey(button.getAttribute("data-rate-site"), button.getAttribute("data-rate-model"));
      if (!state.openRateKeys[key]) return;
      var cell = button.closest("td");
      var panel = cell ? cell.querySelector(".rate-detail") : null;
      if (panel) fillRateDetail(button, panel);
    });
  }

  // ---- 渲染：日志 --------------------------------------------------------
  function loadLogs(initial) {
    var file = initial ? "" : $("logFile").value;
    var lines = $("logLines").value;
    var q = "/admin/api/logs?lines=" + encodeURIComponent(lines);
    if (file) q += "&file=" + encodeURIComponent(file);
    return api(q).then(function (data) {
      var sel = $("logFile");
      if (initial && data.files) {
        sel.innerHTML = data.files.map(function (f) {
          return '<option value="' + esc(f.name) + '">' + esc(f.name) + '</option>';
        }).join("");
        if (data.file) sel.value = data.file;
      }
      var box = $("logBox");
      if (data.error) { box.textContent = "\u8bfb\u53d6\u65e5\u5fd7\u5931\u8d25: " + data.error; return; }
      box.textContent = (data.lines && data.lines.length) ? data.lines.join("\n") : "\u8be5\u6587\u4ef6\u6682\u65e0\u65e5\u5fd7\u3002";
      if (data.truncated) showAlert("\u65e5\u5fd7\u8fbe\u5230\u672c\u6b21\u8bfb\u53d6\u9884\u7b97\uff0c\u53ea\u5c55\u793a\u90e8\u5206\u884c\uff1b\u539f\u65e5\u5fd7\u6587\u4ef6\u672a\u4fee\u6539\u3002");
    }).catch(function () {
      if (!initial && file) return loadLogs(true);
    });
  }

  // ---- 渲染：配置 --------------------------------------------------------
  function loadConfig() {
    var version = ++state.configPreviewVersion;
    return api("/admin/api/config").then(function (data) {
      if (version !== state.configPreviewVersion) return;
      var box = $("configBox");
      if (!data.exists) { box.textContent = "未找到 " + (data.path || "config.json"); return; }
      box.textContent = data.content;
    }).catch(function () {
      // 保留上次内容。
    });
  }

  function configDirty() {
    return state.configMode === "edit" && $("configEditor").value !== state.configOriginal;
  }

  function setConfigBusy(busy) {
    state.configPending = busy;
    ["editConfig", "acknowledgeConfigEdit", "saveConfig", "reloadConfigEditor"].forEach(function (id) {
      $(id).disabled = busy;
    });
    $("configEditor").readOnly = busy;
    $("saveConfig").textContent = busy && state.configMode === "edit" ? "处理中…" : "校验并保存";
  }

  function resetConfigEditor() {
    state.configVersion++;
    state.configMode = "preview";
    state.configRevision = "";
    state.configOriginal = "";
    $("configEditor").value = "";
    $("configEditorStatus").textContent = "";
    $("configWarning").classList.add("hidden");
    $("configEditorPanel").classList.add("hidden");
    $("configBox").classList.remove("hidden");
    $("editConfig").classList.remove("hidden");
    $("configTitle").textContent = "config.json（预览 · Key 已隐藏）";
    $("cancelConfigEdit").disabled = false;
    setConfigBusy(false);
  }

  function showConfigWarning() {
    if (state.configPending || state.configMode !== "preview") return;
    hideAlert();
    state.configMode = "warning";
    $("configResult").textContent = "";
    $("editConfig").classList.add("hidden");
    $("configWarning").classList.remove("hidden");
    reportUIEvent("config_edit_warning", "/admin/api/config-edit", 0, traceID());
    $("acknowledgeConfigEdit").focus();
  }

  function exitConfigEdit() {
    if (state.configPending && state.configMode === "edit") return false;
    if (configDirty() && !window.confirm("有未保存的配置修改。退出会丢弃这些修改，是否继续？")) return false;
    reportUIEvent("config_edit_cancelled", "/admin/api/config-edit", 0, traceID());
    resetConfigEditor();
    hideAlert();
    return true;
  }

  function loadConfigEditor() {
    if (state.configPending || state.configMode === "preview") return Promise.resolve();
    if (configDirty() && !window.confirm("重新加载会丢弃未保存的修改，是否继续？")) return Promise.resolve();
    var version = ++state.configVersion;
    var trace = traceID();
    setConfigBusy(true);
    reportUIEvent("config_edit_acknowledged", "/admin/api/config-edit", 0, trace);
    $("configEditorStatus").textContent = "正在加载服务器配置…";
    $("configResult").textContent = "正在加载服务器配置…";
    return api("/admin/api/config-edit?acknowledged=1", {trace: trace}).then(function (data) {
      if (version !== state.configVersion) return;
      state.configRevision = data.revision;
      state.configOriginal = data.content;
      state.configMode = "edit";
      $("configEditor").value = data.content;
      $("configWarning").classList.add("hidden");
      $("configBox").classList.add("hidden");
      $("configEditorPanel").classList.remove("hidden");
      $("editConfig").classList.add("hidden");
      $("configTitle").textContent = "config.json（编辑 · 包含真实 Key）";
      $("configEditorStatus").textContent = "已加载原文。保存前会校验配置和版本；其他页面或服务器修改过配置时将拒绝覆盖。";
      $("configResult").textContent = "";
    }).catch(function (err) {
      if (version !== state.configVersion) return;
      var message = "加载失败：" + (err.detail || err.message) + "。已有输入保留。";
      $("configEditorStatus").textContent = message;
      $("configResult").textContent = message;
    }).then(function () {
      if (version !== state.configVersion) return;
      setConfigBusy(false);
      if (state.configMode === "edit") $("configEditor").focus();
    });
  }

  function saveConfigEditor() {
    if (state.configPending || state.configMode !== "edit") return Promise.resolve();
    var content = $("configEditor").value;
    var trace = traceID();
    var parsed;
    try {
      parsed = JSON.parse(content);
      if (!parsed || typeof parsed !== "object" || Array.isArray(parsed)) throw new Error("必须是单个 JSON 对象");
      if (new TextEncoder().encode(content).length > 1048576) throw new Error("超过 1 MiB");
    } catch (_) {
      $("configEditorStatus").textContent = "配置未保存：请输入不超过 1 MiB 的完整 JSON 对象，请检查引号、逗号和括号。";
      reportUIEvent("config_validation_failed", "/admin/api/config-edit", 400, trace);
      return Promise.resolve();
    }
    var nextAdminKey = parsed.gateway && parsed.gateway.adminKey || "";
    var adminKeyChanged = nextAdminKey !== state.apiKey;
    var removal = !nextAdminKey;
    if (removal && !window.confirm("你正在清空管理 Key（或删除 gateway.adminKey）。保存后管理登录将不可用，需要在服务器本机重新设置。确认清空？")) {
      reportUIEvent("config_edit_cancelled", "/admin/api/config-edit", 0, trace);
      return Promise.resolve();
    }
    if (!window.confirm("确认将整份配置写入服务器 config.json？\n密钥、规则和提示词等立即生效，端口修改需重启。日志保留规则可能触发不可撤销的删除。" +
        (adminKeyChanged ? "\n管理 Key 已修改，保存后需要重新登录，请先妥善保存新 Key。" : ""))) {
      reportUIEvent("config_edit_cancelled", "/admin/api/config-edit", 0, trace);
      return Promise.resolve();
    }
    var version = state.configVersion;
    setConfigBusy(true);
    $("cancelConfigEdit").disabled = true;
    $("configEditorStatus").textContent = "正在校验并保存…";
    return api("/admin/api/config-edit", {method: "POST", trace: trace,
      body: {content: content, revision: state.configRevision, acknowledged: true, confirmAdminKeyRemoval: removal}
    }).then(function (data) {
      if (version !== state.configVersion) return;
      var message = "配置已保存，热配置已生效。" +
        (data.settings && data.settings.restartRequired ? " 监听端口变更需重启服务。" : " 当前监听端口未改变。") +
        (data.settings && data.settings.apiPortOverridden ? " API 端口受 -port 启动参数覆盖。" : "");
      resetConfigEditor();
      $("configResult").textContent = message;
      if (data.adminKeyChanged) {
        state.apiKey = "";
        sessionStorage.removeItem(KEY_STORAGE);
        $("apiKey").value = "";
        lockDashboard();
        showAlert(message + (data.adminKeyCleared ? " 管理 Key 已清空，请在服务器本机重新设置。" : " 管理 Key 已修改，请使用新 Key 重新登录。"), true);
        setConn("需要重新设置管理 Key 或登录", "");
      } else {
        state.settingsLoadVersion++;
        state.logSettingsVersion++;
        state.policyVersion++;
        loadConfig();
        loadSettings();
      }
    }).catch(function (err) {
      if (version !== state.configVersion) return;
      $("configEditorStatus").textContent = err.status === 409 ?
        "版本冲突：服务器配置已变化。修改未写入，当前输入保留。请先核对修改，再重新加载最新配置。" :
        "保存未完成：" + (err.detail || err.message) + "。当前输入保留，请检查后重试。";
    }).then(function () {
      if (version !== state.configVersion) return;
      $("cancelConfigEdit").disabled = false;
      setConfigBusy(false);
    });
  }

  function renderTasks(data) {
    var labels = {success: "成功", failed: "失败", partial: "部分失败", cancelled: "已取消",
      skipped: "跳过", running: "执行中", disabled: "未启用", unknown: "无执行记录"};
    var classes = {success: "ok", failed: "danger", partial: "warn", cancelled: "warn", running: "warn"};
    $("tasksInfo").textContent = "服务器日志时区：" + (data.timezone || "-") + "（表内时间按浏览器时区显示） · 刷新 " + fmtTime(data.generatedAt) +
      (data.truncated ? " · 日志扫描达到预算，缺失记录不代表未执行。" : " · 执行记录以日志为准。");
    $("tasksTable").querySelector("tbody").innerHTML = (data.tasks || []).map(function (task) {
      var next = task.nextRunAt ? fmtTime(task.nextRunAt) + (task.nextEstimated ? "（推算）" : "") :
        (!task.enabled ? "未启用" : task.oneShot && task.lastFinishedAt ? "本次启动已完成" : "等待调度日志");
      return "<tr><td data-label='任务'>" + esc(task.name) + "</td><td data-label='启用'>" + (task.enabled ? "开启" : "关闭") +
        "</td><td data-label='执行周期' class='task-description'>" + esc(task.schedule) + "</td><td data-label='上次执行记录'>" + esc(fmtTime(task.lastRunAt)) +
        "</td><td data-label='上次完成'>" + esc(fmtTime(task.lastFinishedAt)) + "</td><td data-label='执行结果'><span class='badge " +
        (classes[task.status] || "") + "'>" + esc(labels[task.status] || "无执行记录") +
        "</span></td><td data-label='下次预计执行'>" + esc(next) + "</td><td data-label='日志说明' class='task-description'>" + esc(task.detail || "当前服务尚无该任务日志，或日志已清理") +
        (task.logFile ? "<br><span class='mono'>" + esc(task.logFile) + "</span>" : "") + "</td></tr>";
    }).join("");
  }

  function loadLogSettings() {
    var version = ++state.logSettingsVersion;
    $("logRetentionDays").disabled = true;
    $("saveLogSettings").disabled = true;
    $("cleanupLogs").disabled = true;
    return api("/admin/api/log-settings").then(function (data) {
      if (version !== state.logSettingsVersion) return;
      state.logSettings = data;
      $("logRetentionDays").value = data.retentionDays;
    }).catch(function () {}).then(function () {
      if (version !== state.logSettingsVersion) return;
      $("logRetentionDays").disabled = false;
      $("saveLogSettings").disabled = false;
      $("cleanupLogs").disabled = false;
    });
  }

  function saveLogSettings() {
    var days = Number($("logRetentionDays").value);
    if ($("logRetentionDays").value.trim() === "" || !Number.isInteger(days) || days < 0 || days > 36500) {
      showAlert("保留天数必须是 0～36500 的整数，0 表示保留全部。");
      reportUIEvent("log_validation_failed", "/admin/api/log-settings", 0, traceID());
      return;
    }
    if (days > 0 && !window.confirm("保存后会自动删除超出 " + days + " 天的项目日志，删除不可撤销。是否继续？")) return;
    $("saveLogSettings").disabled = true;
    return api("/admin/api/log-settings", {method: "POST", body: {retentionDays: days}})
      .then(function (data) {
        state.logSettings = data;
        $("logCleanupResult").textContent = "已写入 config.json。保留天数：" + days + (days === 0 ? "（不自动清理）。" : " 天，自动清理已启用。");
      }).catch(function () {}).then(function () { $("saveLogSettings").disabled = false; });
  }

  function cleanupExpiredLogs() {
    var days = state.logSettings && state.logSettings.retentionDays;
    if (!days) {
      $("logCleanupResult").textContent = "当前配置保留全部，不会删除任何日志。请先保存正数保留天数。";
      reportUIEvent("log_validation_failed", "/admin/api/log-cleanup", 0, traceID());
      return;
    }
    if (!window.confirm("按已保存的 " + days + " 天保留规则删除过期项目日志，删除不可撤销。是否继续？")) return;
    $("cleanupLogs").disabled = true;
    return api("/admin/api/log-cleanup", {method: "POST", body: {}}).then(function (data) {
      $("logCleanupResult").textContent = "已删除 " + data.deleted + " 个日志文件，回收 " + data.deletedBytes +
        " 字节；跳过正在写入 " + data.skippedActive + " 个，失败 " + data.failed + " 个。";
      loadLogs(true);
    }).catch(function () {}).then(function () { $("cleanupLogs").disabled = false; });
  }

  // ---- 路由与轮询 --------------------------------------------------------
  function refreshActive() {
    if (!state.apiKey || state.refreshPending || state.togglePending) return Promise.resolve();
    if (state.activeTab === "tasks" && Date.now() - state.tasksLastPoll < 15000) return Promise.resolve();
    if (state.activeTab === "tasks") state.tasksLastPoll = Date.now();
    state.refreshPending = true;
    var policyVersion = state.policyVersion;
    var onErr = function () {
      if (state.activeTab === "overview") {
        renderPoolHealth({stale: true});
      }
      if ($("conn").className.indexOf("danger") === -1) {
        setConn("连接失败", "danger");
      }
    };
    var pending = Promise.resolve();
    if (state.activeTab === "overview" || state.activeTab === "accounts") {
      pending = api("/admin/api/status").then(function (data) {
        if (!state.togglePending && policyVersion === state.policyVersion) renderStatus(data);
      }).catch(onErr).then(function () {
        // 统计是附加信息：不并入 refreshActive 的完成链，避免统计变慢时拖住状态轮询。
        if (state.activeTab === "overview") {
          loadStats(false);
          loadAccountAlerts();
        }
        if (state.activeTab === "accounts") pollAddAccount();
      });
    } else if (state.activeTab === "models") {
      pending = api("/admin/api/models").then(function (data) {
        if (!state.togglePending && policyVersion === state.policyVersion) renderModels(data);
      }).catch(onErr);
    } else if (state.activeTab === "logs") {
      pending = loadLogs(false);
    } else if (state.activeTab === "tasks") {
      pending = api("/admin/api/tasks").then(renderTasks).catch(onErr);
    }
    return pending.then(function () {
      state.refreshPending = false;
      if (policyVersion !== state.policyVersion && !state.togglePending) return refreshActive();
    }, function () { state.refreshPending = false; });
  }

  function setTab(name) {
    if (name !== "config" && state.configMode !== "preview" && !exitConfigEdit()) return;
    if (name === "tasks") state.tasksLastPoll = 0;
    state.activeTab = name;
    Array.prototype.forEach.call(document.querySelectorAll(".tab"), function (t) {
      t.classList.toggle("active", t.getAttribute("data-tab") === name);
    });
    Array.prototype.forEach.call(document.querySelectorAll(".panel"), function (p) {
      p.classList.toggle("active", p.id === "panel-" + name);
    });
    if (name === "logs") { loadLogs(true); loadLogSettings(); }
    else if (name === "config") loadConfig();
    else if (name === "settings") loadSettings();
    else refreshActive();
  }

  function renderSettings(data) {
    state.settings = data;
    $("apiKeyEnabled").checked = data.apiKeyEnabled;
    $("modelAPIKey").value = "";
    $("apiKeyState").textContent = data.apiKeyConfigured ? "已配置 API Key，留空将保留原值。" : "尚未配置 API Key。";
    $("apiPortValue").textContent = data.apiPort;
    $("webPortValue").textContent = data.webPort;
    $("configuredApiPort").value = data.configuredApiPort || data.apiPort;
    $("configuredWebPort").value = data.configuredWebPort || data.webPort;
    $("portSettingsStatus").textContent = (data.restartRequired ? "端口配置已保存，待重启生效。 " :
      "配置端口与实际监听一致。 ") + (data.apiPortOverridden ? "API 端口被启动参数 -port 覆盖，需调整该参数才能按配置启动。 " : "") +
      "修改并保存后需重启网关，不会立即中断页面或请求。";
  }

  function loadSettings() {
    var version = ++state.settingsLoadVersion;
    setSettingsBusy(true);
    return api("/admin/api/settings").then(function (data) {
      if (version === state.settingsLoadVersion) renderSettings(data);
    }).catch(function () {}).then(function () {
      if (version === state.settingsLoadVersion) setSettingsBusy(false);
    });
  }

  function setSettingsBusy(busy) {
    ["apiKeyEnabled", "modelAPIKey", "configuredApiPort", "configuredWebPort", "saveAPISettings", "savePortSettings"]
      .forEach(function (id) { $(id).disabled = busy; });
  }

  function saveAPISettings() {
    var enabled = $("apiKeyEnabled").checked;
    var key = $("modelAPIKey").value;
    if (enabled && !key && !(state.settings && state.settings.apiKeyConfigured)) {
      showAlert("开启 API Key 校验前必须输入一个 Key。");
      reportUIEvent("settings_validation_failed", "/admin/api/settings", 0, traceID());
      return;
    }
    setSettingsBusy(true);
    return api("/admin/api/settings", {method: "POST", body: {apiKeyEnabled: enabled, apiKey: key}})
      .then(function (data) {
        renderSettings(data);
        showAlert("已写入 config.json，API 鉴权设置立即生效；管理鉴权未改变。", true);
      }).catch(function () {}).then(function () { setSettingsBusy(false); });
  }

  function savePortSettings() {
    var apiPort = Number($("configuredApiPort").value);
    var webPort = Number($("configuredWebPort").value);
    if (!Number.isInteger(apiPort) || !Number.isInteger(webPort) || apiPort < 1 || apiPort > 65535 ||
      webPort < 1 || webPort > 65535 || apiPort === webPort) {
      showAlert("端口必须是 1～65535 的整数，且 API 与 Web 端口不能相同。");
      reportUIEvent("settings_validation_failed", "/admin/api/settings", 0, traceID());
      return;
    }
    setSettingsBusy(true);
    return api("/admin/api/settings", {method: "POST", body: {apiPort: apiPort, webPort: webPort}})
      .then(function (data) {
        renderSettings(data);
        showAlert("端口已写入 config.json，重启网关后生效；当前监听端口未改变。", true);
      }).catch(function () {}).then(function () { setSettingsBusy(false); });
  }

  // 管理 Key 仅保存在当前标签页会话，验证成功后才保存。
  function saveKey() {
    if (state.configMode !== "preview" && !exitConfigEdit()) return Promise.resolve();
    var key = $("apiKey").value.trim();
    if (!key) { showAlert("请输入管理 Key。"); return Promise.resolve(); }
    state.apiKey = key;
    setConn("校验中…", "");
    return api("/admin/api/settings").then(function (data) {
      sessionStorage.setItem(KEY_STORAGE, key);
      reportRestoredListFilters();
      renderSettings(data);
      $("tabs").classList.remove("hidden");
      $("dashboard").classList.remove("hidden");
      $("setupPanel").classList.add("hidden");
      showAlert("管理鉴权通过。", true);
      refreshActive();
    }).catch(function () {
      // api() 已提示 401 / HTTP 错误；这里补充网络类错误。
      if ($("conn").className.indexOf("danger") === -1) {
        setConn("连接失败", "danger");
        showAlert("无法连接管理端口，请检查访问地址与 Web 端口。");
      }
    });
  }

  function initializeAdmin() {
    $("initializeAdmin").disabled = true;
    return api("/admin/api/setup", {method: "POST", body: {}}).then(function (data) {
      // 原文仅本次弹窗显示，不写 console、日志或 localStorage。
      window.alert("管理 Key 已成功写入 config.json：\n\n" + data.adminKey +
        "\n\n请妥善保存。这是唯一一次自动展示；遗忘时请在服务器本机查看 config.json。");
      $("apiKey").value = data.adminKey;
      return saveKey();
    }).catch(function () {}).then(function () { $("initializeAdmin").disabled = false; });
  }

  function discoverSetup() {
    return api("/admin/api/setup").then(function (data) {
      if (data.setupRequired) {
        state.apiKey = "";
        sessionStorage.removeItem(KEY_STORAGE);
        lockDashboard();
        $("setupPanel").classList.remove("hidden");
        $("initializeAdmin").disabled = !data.canInitialize;
        setConn("等待初始化", "");
        if (!data.canInitialize) {
          showAlert("当前不是本机访问，无法在此页面自动生成管理 Key。可选方式：" +
            "① 在服务器本机打开本页面；② 用 SSH 本地端口转发（ssh -L 8316:127.0.0.1:8316 用户@服务器）后访问；" +
            "③ 直接在服务器上编辑 config.json，把 gateway.adminKey 设为随机字符串，保存后约 1 秒生效。");
        }
      } else if (state.apiKey) {
        saveKey();
      } else {
        lockDashboard();
        setConn("等待管理登录", "");
      }
    }).catch(function () {});
  }

  function initTheme() {
    var saved = localStorage.getItem("wb_theme");
    if (saved) document.documentElement.setAttribute("data-theme", saved);
    $("themeToggle").addEventListener("click", function () {
      var cur = document.documentElement.getAttribute("data-theme");
      var next = cur === "dark" ? "light" : (cur === "light" ? "dark" : (matchMedia("(prefers-color-scheme: dark)").matches ? "light" : "dark"));
      document.documentElement.setAttribute("data-theme", next);
      localStorage.setItem("wb_theme", next);
    });
  }

  function init() {
    // 清理旧版同源页面遗留的模型 API Key，不把它误当成管理 Key。
    localStorage.removeItem("wb_gateway_api_key");
    initListFilters();
    initShowBlockedFilter();
    initStatsFilter();
    $("statsStart").addEventListener("change", function () { state.statsStartTouched = true; });
    $("statsEnd").addEventListener("change", function () {
      state.statsEndTouched = true;
      if (state.statsFollowToday) {
        state.statsFollowToday = false;
        updateStatsEndState();
      }
    });
    $("statsFollowToday").addEventListener("change", function () {
      state.statsFollowToday = !!this.checked;
      if (state.statsFollowToday) {
        state.statsEndTouched = false;
        $("statsEnd").value = statsToday();
      } else {
        state.statsEndTouched = true;
        if (!$("statsEnd").value) $("statsEnd").value = statsToday();
      }
      updateStatsEndState();
      changeStatsFilter(false);
    });
    $("apiKey").value = state.apiKey;
    initTheme();
    $("saveKey").addEventListener("click", saveKey);
    $("statsApply").addEventListener("click", function () { changeStatsFilter(false); });
    $("statsPeriod").addEventListener("change", function () { changeStatsFilter(true); });
    $("statsAccount").addEventListener("change", function () { changeStatsFilter(false); });
    // 图表 viewBox 跟随容器宽度，窗口尺寸变化后重绘一次（用 rAF 合并连续事件）。
    var statsResizePending = false;
    window.addEventListener("resize", function () {
      if (!state.lastStats || statsResizePending) return;
      statsResizePending = true;
      (window.requestAnimationFrame || function (fn) { fn(); })(function () {
        statsResizePending = false;
        if (state.lastStats) renderStats(state.lastStats);
      });
    });
    $("logout").addEventListener("click", function () {
      if (state.configMode !== "preview" && !exitConfigEdit()) return;
      resetConfigEditor();
      sessionStorage.removeItem(KEY_STORAGE);
      state.apiKey = "";
      window.location.reload();
    });
    $("initializeAdmin").addEventListener("click", initializeAdmin);
    $("saveAPISettings").addEventListener("click", saveAPISettings);
    $("savePortSettings").addEventListener("click", savePortSettings);
    $("editConfig").addEventListener("click", showConfigWarning);
    $("acknowledgeConfigEdit").addEventListener("click", loadConfigEditor);
    $("cancelConfigWarning").addEventListener("click", exitConfigEdit);
    $("cancelConfigEdit").addEventListener("click", exitConfigEdit);
    $("reloadConfigEditor").addEventListener("click", loadConfigEditor);
    $("saveConfig").addEventListener("click", saveConfigEditor);
    $("configEditor").addEventListener("input", function () {
      $("configEditorStatus").textContent = configDirty() ? "有未保存的修改。离开配置页会提示确认；不会自动保存。" : "内容未修改。";
    });
    window.addEventListener("beforeunload", function (event) {
      if (!configDirty() && !state.configPending) return;
      event.preventDefault();
      event.returnValue = "";
    });
    $("saveLogSettings").addEventListener("click", saveLogSettings);
    $("cleanupLogs").addEventListener("click", cleanupExpiredLogs);
    $("apiKey").addEventListener("keydown", function (e) { if (e.key === "Enter") saveKey(); });
    Array.prototype.forEach.call(document.querySelectorAll(".tab"), function (t) {
      t.addEventListener("click", function () { setTab(t.getAttribute("data-tab")); });
    });
    $("accountsTable").addEventListener("click", togglePolicy);
    $("accountsTable").addEventListener("click", accountRowActions);
    $("addAccountBtn").addEventListener("click", openAddAccount);
    $("addStart").addEventListener("click", startAddAccount);
    $("addCancel").addEventListener("click", closeAddAccount);
    $("addAccountCopy").addEventListener("click", copyAddAccountLink);
    $("modelsTable").addEventListener("click", togglePolicy);
    $("modelsTable").addEventListener("click", toggleRateDetail);
    $("modelsTable").addEventListener("click", copyModelID);
    $("modelsTable").addEventListener("click", unblockModel);
    $("logFile").addEventListener("change", function () { loadLogs(false); });
    $("logLines").addEventListener("change", function () { loadLogs(false); });

    discoverSetup();
    state.timer = setInterval(refreshActive, POLL_MS);
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
