export type WarnFn = (msg: string) => void;

export async function withRetry<T>(fn: () => Promise<T>, label: string, warn: WarnFn = console.warn, retries = 5): Promise<T> {
  let lastErr: unknown;
  for (let i = 0; i < retries; i++) {
    try {
      return await fn();
    } catch (err) {
      lastErr = err;
      const delay = Math.min(1000 * 2 ** i, 16000);
      warn(`[retry ${i + 1}/${retries}] ${label}: ${String((err as Error)?.message || err)}; waiting ${delay}ms`);
      await new Promise((r) => setTimeout(r, delay));
    }
  }
  throw lastErr;
}
