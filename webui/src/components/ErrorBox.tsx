import { ApiError } from "../api/client";
import { openSignIn } from "../lib/auth";

export function ErrorBox({ error }: { error: unknown }) {
  if (error instanceof ApiError) {
    return (
      <div className="error-box" role="alert">
        <span className="error-status">{error.status}</span>
        <strong>{error.error}</strong>
        {error.detail && <span className="error-detail">{error.detail}</span>}
        {error.status === 401 && (
          <button type="button" className="theme-toggle auth-button" onClick={openSignIn}>
            sign in
          </button>
        )}
      </div>
    );
  }
  const message = error instanceof Error ? error.message : String(error);
  return (
    <div className="error-box" role="alert">
      <strong>Request failed</strong>
      <span className="error-detail">{message}</span>
    </div>
  );
}
