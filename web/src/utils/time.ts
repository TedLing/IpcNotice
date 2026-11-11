/** 时间展示工具：后端统一返回 RFC3339（UTC，带 Z 后缀），前端转本地时区展示 */

/**
 * 把 RFC3339 时间字符串转成本地时区的 "YYYY-MM-DD HH:mm:ss"（可选毫秒）。
 * 无法解析时退回原字符串（仅把 T 换成空格），保证旧数据不会显示成 "Invalid Date"。
 */
export function formatLocalTime(v: string, withMs = false): string {
  if (!v) return "-";
  const d = new Date(v);
  if (Number.isNaN(d.getTime())) {
    return v.replace("T", " ").slice(0, withMs ? 23 : 19);
  }
  const p = (n: number) => String(n).padStart(2, "0");
  const base = `${d.getFullYear()}-${p(d.getMonth() + 1)}-${p(d.getDate())} ${p(d.getHours())}:${p(d.getMinutes())}:${p(d.getSeconds())}`;
  if (!withMs) return base;
  return `${base}.${String(d.getMilliseconds()).padStart(3, "0")}`;
}
